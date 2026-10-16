//! Private append-only journal for opt-in observation records.
//!
//! Socket consumers must use `JournalWorker::try_record`; segment I/O and fsync run on a
//! dedicated blocking thread. Queue saturation is returned to the caller as a hard error.

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::ffi::OsStrExt,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    thread,
};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    schema_version: String,
    sequence: u64,
    previous_hash: String,
    payload_hash: String,
    payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JournalCursor {
    pub segment: u64,
    pub sequence: u64,
    pub hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct JournalEntry {
    pub cursor: JournalCursor,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DurableRecord {
    pub cursor: JournalCursor,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: String,
    segment: u64,
    durable_sequence: u64,
    durable_hash: String,
    durable_bytes: u64,
    segments: Vec<SegmentReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct SegmentReceipt {
    path: String,
    bytes: u64,
    sha256: String,
    records: u64,
    last_record_hash: String,
}

pub const MAX_JOURNAL_ROW_BYTES: usize = 8 * 1024 * 1024;

pub struct ObservationJournal {
    root: PathBuf,
    segment_limit: u64,
    file: File,
    manifest: Manifest,
    sealed: bool,
    minimum_disk_free_bytes: u64,
    active_hasher: Sha256,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapeStreamReceipt {
    pub channel: String,
    pub instrument_id: String,
    pub coverage_start_ms: u64,
    pub coverage_end_ms: u64,
    pub record_count: u64,
    pub gap_count: u64,
    pub subscription_acknowledged: bool,
    pub metadata_revision: String,
    pub receipt_source: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapeSealInput {
    pub environment: String,
    pub region: String,
    pub source_profile: String,
    pub producer_revision: String,
    pub capture_session: String,
    pub availability_semantics: String,
    pub scored_start_ms: u64,
    pub scored_end_ms: u64,
    pub sealed_at_ms: u64,
    pub streams: Vec<TapeStreamReceipt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationTapeManifest {
    pub schema_version: String,
    pub venue: String,
    pub environment: String,
    pub region: String,
    pub source_profile: String,
    pub producer_revision: String,
    pub capture_session: String,
    pub input_schema: String,
    pub replay_mode: String,
    pub coverage_semantics: String,
    pub availability_semantics: String,
    pub scored_window: TapeWindow,
    pub streams: Vec<TapeStreamReceipt>,
    pub segments: Vec<TapeSegment>,
    pub sealed_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapeWindow {
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapeSegment {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
    pub record_count: u64,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub last_record_hash: String,
}

impl ObservationJournal {
    pub fn open(root: impl AsRef<Path>, segment_limit: u64) -> Result<Self> {
        Self::open_with_disk_floor(root, segment_limit, 0)
    }

    pub fn open_with_disk_floor(
        root: impl AsRef<Path>,
        segment_limit: u64,
        minimum_disk_free_bytes: u64,
    ) -> Result<Self> {
        ensure!(
            (4096..=1_073_741_824).contains(&segment_limit),
            "segment limit must be 4 KiB..1 GiB"
        );
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).context("create observation journal directory")?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .context("secure observation journal directory")?;
        let manifest_path = root.join("manifest.json");
        let manifest = if manifest_path.exists() {
            let meta = fs::symlink_metadata(&manifest_path)?;
            ensure!(
                meta.is_file() && meta.permissions().mode() & 0o077 == 0,
                "journal manifest must be private regular file"
            );
            serde_json::from_slice(&fs::read(&manifest_path)?)?
        } else {
            Manifest {
                schema_version: "okx.observations.journal.v1".into(),
                segment: 0,
                durable_sequence: 0,
                durable_hash: zero_hash(),
                durable_bytes: 0,
                segments: Vec::new(),
            }
        };
        ensure!(
            manifest.schema_version == "okx.observations.journal.v1",
            "unsupported journal manifest"
        );
        let sealed = root.join("research.okx_observation_tape.v1.json").exists();
        for (number, receipt) in manifest.segments.iter().enumerate() {
            let current_number = number as u64;
            let path = segment_path(&root, current_number);
            ensure!(
                receipt.path
                    == path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or_default(),
                "journal segment receipt path mismatch"
            );
            if current_number < manifest.segment {
                let (records, last_hash, bytes) = verify_segment(&path, current_number)?;
                ensure!(
                    records == receipt.records
                        && last_hash == receipt.last_record_hash
                        && bytes == receipt.bytes,
                    "historical journal segment does not match manifest receipt"
                );
                ensure!(
                    sha256_file(&path)? == receipt.sha256,
                    "historical journal segment hash mismatch"
                );
            }
        }
        ensure!(
            manifest.segments.len()
                == manifest.segment as usize + usize::from(manifest.durable_sequence > 0),
            "journal manifest has a missing segment receipt"
        );
        let active_path = segment_path(&root, manifest.segment);
        if active_path.exists() {
            recover_active_tail(&active_path, manifest.segment, &manifest)?;
        } else {
            ensure!(
                manifest.durable_sequence == 0 && manifest.durable_bytes == 0,
                "durable journal segment is missing"
            );
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&active_path)?
                .sync_all()?;
        }
        let segment_path = active_path;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .mode(0o600)
            .open(&segment_path)?;
        let mut active_hasher = Sha256::new();
        let mut active = File::open(&segment_path)?;
        std::io::copy(&mut active, &mut Sha256Writer(&mut active_hasher))?;
        let fresh_manifest = !manifest_path.exists();
        let journal = Self {
            root,
            segment_limit,
            file,
            manifest,
            sealed,
            minimum_disk_free_bytes,
            active_hasher,
        };
        if fresh_manifest {
            journal.write_manifest()?;
        }
        Ok(journal)
    }

    /// Append, fsync the segment and atomically publish its durable boundary.
    pub fn append(&mut self, payload: Value) -> Result<(u64, String)> {
        let cursor = self.append_cursor(payload)?;
        Ok((cursor.sequence, cursor.hash))
    }

    pub fn append_cursor(&mut self, payload: Value) -> Result<JournalCursor> {
        ensure!(!self.sealed, "sealed observation tape is immutable");
        self.ensure_disk_floor()?;
        let payload_bytes = serde_json::to_vec(&payload)?;
        ensure!(
            payload_bytes.len() <= MAX_JOURNAL_ROW_BYTES,
            "journal payload exceeds the shared 8 MiB row limit"
        );
        ensure!(
            payload_bytes.len() <= self.segment_limit as usize / 2,
            "journal record exceeds half a segment"
        );
        let record = JournalRecord {
            schema_version: "okx.observations.record.v1".into(),
            sequence: self
                .manifest
                .durable_sequence
                .checked_add(1)
                .context("journal sequence overflow")?,
            previous_hash: self.manifest.durable_hash.clone(),
            payload_hash: sha256(&payload_bytes),
            payload,
        };
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        ensure!(
            line.len() <= MAX_JOURNAL_ROW_BYTES,
            "serialized journal row including newline exceeds the shared 8 MiB wire limit"
        );
        if self.manifest.durable_bytes > 0
            && self.manifest.durable_bytes + line.len() as u64 > self.segment_limit
        {
            self.rotate()?;
            return self.append_cursor(record.payload);
        }
        self.file
            .write_all(&line)
            .context("append observation journal record")?;
        self.active_hasher.update(&line);
        self.file
            .sync_all()
            .context("fsync observation journal segment")?;
        let hash = record_hash(&record)?;
        self.manifest.durable_sequence = record.sequence;
        self.manifest.durable_hash = hash.clone();
        self.manifest.durable_bytes += line.len() as u64;
        self.update_active_receipt()?;
        self.write_manifest()?;
        Ok(JournalCursor {
            segment: self.manifest.segment,
            sequence: record.sequence,
            hash,
        })
    }

    pub fn append_durable(&mut self, payload: Value) -> Result<DurableRecord> {
        let cursor = self.append_cursor(payload.clone())?;
        Ok(DurableRecord { cursor, payload })
    }

    pub fn seal_tape(&mut self, input: TapeSealInput) -> Result<PathBuf> {
        ensure!(!self.sealed, "observation journal is already sealed");
        ensure!(
            input.environment == "public" && matches!(input.region.as_str(), "global" | "eea"),
            "invalid tape environment or region"
        );
        ensure!(
            input.source_profile
                == match input.region.as_str() {
                    "global" => "okx_global_public_v1",
                    "eea" => "okx_eea_public_v1",
                    _ => unreachable!(),
                },
            "source profile must match the allowlisted region"
        );
        ensure!(
            input.scored_start_ms > 0
                && input.scored_end_ms > input.scored_start_ms
                && input.sealed_at_ms >= input.scored_end_ms,
            "invalid scored tape window or seal time"
        );
        ensure!(
            !input.producer_revision.is_empty() && !input.capture_session.is_empty(),
            "producer revision and capture session are required"
        );
        ensure!(
            input.availability_semantics == "source_receipt_only_publication_unavailable",
            "tape availability semantics must disclose missing historical publication receipts"
        );
        ensure!(
            !input.streams.is_empty(),
            "sealed tape needs stream coverage receipts"
        );
        for stream in &input.streams {
            ensure!(
                matches!(
                    stream.channel.as_str(),
                    "trades-all" | "bbo-tbt" | "metadata"
                ),
                "unsupported tape stream channel"
            );
            ensure!(
                stream.coverage_start_ms <= input.scored_start_ms
                    && stream.coverage_end_ms >= input.scored_end_ms,
                "stream coverage does not contain scored window"
            );
            ensure!(
                stream.gap_count == 0 && stream.subscription_acknowledged,
                "stream gaps or missing subscription ACK block tape sealing"
            );
            ensure!(
                !stream.instrument_id.is_empty() && !stream.metadata_revision.is_empty(),
                "stream instrument metadata identity is required"
            );
            ensure!(
                stream.receipt_source == "capture_report",
                "coverage must be identified as a capture report receipt"
            );
        }
        self.file
            .sync_all()
            .context("fsync final observation segment")?;
        self.update_active_receipt()?;
        self.write_manifest()?;
        let segments = self
            .manifest
            .segments
            .iter()
            .map(|segment| {
                let records = segment.records;
                TapeSegment {
                    path: segment.path.clone(),
                    bytes: segment.bytes,
                    sha256: segment.sha256.clone(),
                    record_count: records,
                    first_sequence: if records == 0 { 0 } else { 1 },
                    last_sequence: records,
                    last_record_hash: segment.last_record_hash.clone(),
                }
            })
            .filter(|segment| segment.record_count > 0)
            .collect::<Vec<_>>();
        ensure!(
            !segments.is_empty(),
            "cannot seal an empty observation tape"
        );
        let manifest = ObservationTapeManifest {
            schema_version: "research.okx_observation_tape.v1".into(),
            venue: "okx".into(),
            environment: input.environment,
            region: input.region,
            source_profile: input.source_profile,
            producer_revision: input.producer_revision,
            capture_session: input.capture_session,
            input_schema: "okx.observations.record.v1".into(),
            replay_mode: "observed_availability".into(),
            coverage_semantics: "capture_report_receipts_not_independent_continuity_proof".into(),
            availability_semantics: input.availability_semantics,
            scored_window: TapeWindow {
                start_ms: input.scored_start_ms,
                end_ms: input.scored_end_ms,
            },
            streams: input.streams,
            segments,
            sealed_at_ms: input.sealed_at_ms,
        };
        let destination = self.root.join("research.okx_observation_tape.v1.json");
        let tmp = self.root.join("research.okx_observation_tape.v1.json.tmp");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)?;
        file.write_all(&serde_json::to_vec_pretty(&manifest)?)?;
        file.sync_all()?;
        fs::rename(&tmp, &destination)?;
        File::open(&self.root)?.sync_all()?;
        self.sealed = true;
        Ok(destination)
    }

    fn rotate(&mut self) -> Result<()> {
        self.update_active_receipt()?;
        self.manifest.segment = self
            .manifest
            .segment
            .checked_add(1)
            .context("journal segment sequence overflow")?;
        self.manifest.durable_sequence = 0;
        self.manifest.durable_hash = zero_hash();
        self.manifest.durable_bytes = 0;
        self.file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .read(true)
            .mode(0o600)
            .open(segment_path(&self.root, self.manifest.segment))
            .context("create next journal segment")?;
        self.active_hasher = Sha256::new();
        self.file.sync_all()?;
        self.write_manifest()
    }

    fn update_active_receipt(&mut self) -> Result<()> {
        let path = segment_path(&self.root, self.manifest.segment);
        let receipt = SegmentReceipt {
            path: path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string(),
            bytes: self.manifest.durable_bytes,
            sha256: format!("{:x}", self.active_hasher.clone().finalize()),
            records: self.manifest.durable_sequence,
            last_record_hash: self.manifest.durable_hash.clone(),
        };
        let index = self.manifest.segment as usize;
        if let Some(existing) = self.manifest.segments.get_mut(index) {
            *existing = receipt;
        } else {
            ensure!(
                index == self.manifest.segments.len(),
                "journal segment manifest gap"
            );
            self.manifest.segments.push(receipt);
        }
        Ok(())
    }

    fn write_manifest(&self) -> Result<()> {
        let path = self.root.join("manifest.json");
        let tmp = self.root.join("manifest.json.tmp");
        let bytes = serde_json::to_vec(&self.manifest)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, &path).context("publish journal manifest")?;
        File::open(&self.root)?
            .sync_all()
            .context("fsync journal directory")?;
        Ok(())
    }

    fn ensure_disk_floor(&self) -> Result<()> {
        if self.minimum_disk_free_bytes == 0 {
            return Ok(());
        }
        let path =
            CString::new(self.root.as_os_str().as_bytes()).context("journal path contains NUL")?;
        let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
        ensure!(result == 0, "could not read journal filesystem free space");
        let stats = unsafe { stats.assume_init() };
        let free = (stats.f_bavail as u128).saturating_mul(stats.f_frsize as u128);
        ensure!(
            free >= self.minimum_disk_free_bytes as u128,
            "observation journal disk free space is below configured minimum"
        );
        Ok(())
    }
}

struct Sha256Writer<'a>(&'a mut Sha256);

impl Write for Sha256Writer<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Validate a segment's complete newline records and hash chain. A torn tail is a hard error.
pub fn verify_segment(path: &Path, segment: u64) -> Result<(u64, String, u64)> {
    let file = File::open(path)?;
    let (records, last_hash, bytes, _) = verify_reader(BufReader::new(file), segment)?;
    Ok((records, last_hash, bytes))
}

fn verify_reader<R: BufRead>(mut reader: R, segment: u64) -> Result<(u64, String, u64, String)> {
    let mut line = Vec::new();
    let mut expected_sequence = 1u64;
    let mut previous_hash = zero_hash();
    let mut segment_hasher = Sha256::new();
    let mut bytes = 0u64;
    loop {
        line.clear();
        let read = reader
            .by_ref()
            .take((MAX_JOURNAL_ROW_BYTES as u64) + 1)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        ensure!(
            read <= MAX_JOURNAL_ROW_BYTES,
            "journal row exceeds configured maximum"
        );
        ensure!(
            line.last() == Some(&b'\n'),
            "torn observation journal tail in segment {segment}"
        );
        bytes = bytes
            .checked_add(read as u64)
            .context("journal byte count overflow")?;
        segment_hasher.update(&line);
        line.pop();
        let record: JournalRecord = serde_json::from_slice(&line)
            .with_context(|| format!("invalid journal record in segment {segment}"))?;
        ensure!(
            record.schema_version == "okx.observations.record.v1",
            "unsupported journal record schema"
        );
        ensure!(record.sequence == expected_sequence, "journal sequence gap");
        ensure!(
            record.previous_hash == previous_hash,
            "journal hash chain mismatch"
        );
        ensure!(
            record.payload_hash == sha256(&serde_json::to_vec(&record.payload)?),
            "journal payload hash mismatch"
        );
        previous_hash = record_hash(&record)?;
        expected_sequence = expected_sequence
            .checked_add(1)
            .context("journal sequence overflow")?;
    }
    Ok((
        expected_sequence - 1,
        previous_hash,
        bytes,
        format!("{:x}", segment_hasher.finalize()),
    ))
}

/// Visit a complete hash-verified journal in segment order without retaining its history.
pub fn visit_verified_records(
    root: impl AsRef<Path>,
    mut visit: impl FnMut(JournalEntry) -> Result<()>,
) -> Result<()> {
    let root = root.as_ref();
    let manifest_path = root.join("manifest.json");
    ensure!(
        fs::symlink_metadata(&manifest_path)?.file_type().is_file(),
        "journal manifest is not a regular file"
    );
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(manifest_path)?).context("invalid journal manifest")?;
    ensure!(
        manifest.schema_version == "okx.observations.journal.v1",
        "unsupported journal schema"
    );
    let mut tail_cursor: Option<JournalCursor> = None;
    for (index, receipt) in manifest.segments.iter().enumerate() {
        let segment = index as u64;
        let relative = Path::new(&receipt.path);
        ensure!(
            !relative.as_os_str().is_empty()
                && relative
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_))),
            "journal manifest contains an unsafe segment path"
        );
        let path = root.join(relative);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect journal segment {segment}"))?;
        ensure!(
            metadata.file_type().is_file(),
            "journal segment is not a regular file"
        );
        let file = File::open(&path).with_context(|| format!("open journal segment {segment}"))?;
        let mut reader = BufReader::new(file);
        let mut line = Vec::new();
        let mut sequence = 1u64;
        let mut previous_hash = zero_hash();
        let mut segment_hasher = Sha256::new();
        let mut bytes = 0u64;
        loop {
            line.clear();
            let read = reader
                .by_ref()
                .take((MAX_JOURNAL_ROW_BYTES as u64) + 1)
                .read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            ensure!(
                read <= MAX_JOURNAL_ROW_BYTES,
                "journal row exceeds configured maximum"
            );
            ensure!(
                line.last() == Some(&b'\n'),
                "torn observation journal tail in segment {segment}"
            );
            bytes = bytes
                .checked_add(read as u64)
                .context("journal byte count overflow")?;
            segment_hasher.update(&line);
            line.pop();
            let record: JournalRecord = serde_json::from_slice(&line)
                .with_context(|| format!("invalid journal record in segment {segment}"))?;
            ensure!(
                record.schema_version == "okx.observations.record.v1",
                "unsupported journal record schema"
            );
            ensure!(record.sequence == sequence, "journal sequence gap");
            ensure!(
                record.previous_hash == previous_hash,
                "journal hash chain mismatch"
            );
            ensure!(
                record.payload_hash == sha256(&serde_json::to_vec(&record.payload)?),
                "journal payload hash mismatch"
            );
            let hash = record_hash(&record)?;
            previous_hash = hash.clone();
            tail_cursor = Some(JournalCursor {
                segment,
                sequence,
                hash,
            });
            visit(JournalEntry {
                cursor: tail_cursor.clone().expect("cursor assigned above"),
                payload: record.payload,
            })?;
            sequence = sequence
                .checked_add(1)
                .context("journal sequence overflow")?;
        }
        ensure!(
            sequence - 1 == receipt.records && bytes == receipt.bytes,
            "journal segment receipt mismatch"
        );
        ensure!(
            format!("{:x}", segment_hasher.finalize()) == receipt.sha256,
            "journal segment SHA-256 mismatch"
        );
        ensure!(
            previous_hash == receipt.last_record_hash,
            "journal segment cursor hash mismatch"
        );
    }
    if manifest.segments.is_empty() {
        ensure!(
            manifest.segment == 0 && manifest.durable_sequence == 0 && manifest.durable_bytes == 0,
            "empty journal manifest has nonempty durable state"
        );
    } else {
        ensure!(
            manifest.segment as usize + 1 == manifest.segments.len(),
            "journal manifest segment inventory is incomplete"
        );
        let last = tail_cursor.context("journal manifest has segments without records")?;
        ensure!(
            last.segment == manifest.segment
                && last.sequence == manifest.durable_sequence
                && last.hash == manifest.durable_hash,
            "journal tail cursor does not match manifest"
        );
    }
    Ok(())
}

/// Read a complete hash-verified journal in segment order for deterministic offline replay.
pub fn read_verified_records(root: impl AsRef<Path>) -> Result<Vec<JournalEntry>> {
    let mut entries = Vec::new();
    visit_verified_records(root, |entry| {
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}

/// Bounded convenience reader for callers that need a replay suffix in memory.
pub fn read_verified_records_limited(
    root: impl AsRef<Path>,
    limit: usize,
) -> Result<Vec<JournalEntry>> {
    ensure!(limit > 0, "journal read limit must be positive");
    let mut entries = Vec::new();
    visit_verified_records(root, |entry| {
        ensure!(
            entries.len() < limit,
            "verified journal exceeds configured replay memory limit"
        );
        entries.push(entry);
        Ok(())
    })?;
    Ok(entries)
}

fn recover_active_tail(path: &Path, segment: u64, manifest: &Manifest) -> Result<()> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && meta.permissions().mode() & 0o077 == 0,
        "journal segment must be private regular file"
    );
    let file_len = meta.len();
    ensure!(
        file_len >= manifest.durable_bytes,
        "journal segment is shorter than durable manifest boundary"
    );
    let durable_file = File::open(path)?;
    let (records, last_hash, bytes, durable_sha256) = verify_reader(
        BufReader::new(durable_file).take(manifest.durable_bytes),
        segment,
    )?;
    ensure!(
        records == manifest.durable_sequence
            && last_hash == manifest.durable_hash
            && bytes == manifest.durable_bytes,
        "journal durable prefix does not match manifest"
    );
    if let Some(receipt) = manifest.segments.get(segment as usize) {
        ensure!(
            receipt.records == records
                && receipt.last_record_hash == last_hash
                && receipt.bytes == bytes,
            "active journal receipt does not match durable prefix"
        );
        ensure!(
            receipt.sha256 == durable_sha256,
            "active journal segment hash mismatch"
        );
    }
    let tail_bytes = file_len - manifest.durable_bytes;
    if tail_bytes > 0 {
        let diagnostic = path.with_extension(format!("torn-{}", uuid::Uuid::new_v4().simple()));
        let mut saved = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(diagnostic)
            .context("preserve unacknowledged torn journal tail")?;
        let mut source = File::open(path)?;
        source.seek(SeekFrom::Start(manifest.durable_bytes))?;
        std::io::copy(&mut source.take(tail_bytes), &mut saved)
            .context("copy unacknowledged tail to diagnostic file")?;
        saved.sync_all()?;
        let active = OpenOptions::new().write(true).open(path)?;
        active.set_len(manifest.durable_bytes)?;
        active.sync_all()?;
    }
    Ok(())
}

enum Command {
    Record(Value, oneshot::Sender<Result<DurableRecord, String>>),
    Checkpoint(Value, oneshot::Sender<Result<DurableRecord, String>>),
    Barrier(oneshot::Sender<Result<JournalCursor, String>>),
    Stop(oneshot::Sender<()>),
}

pub struct JournalWorker {
    sender: mpsc::Sender<Command>,
    join: Option<thread::JoinHandle<()>>,
}

pub struct PendingRecord {
    reply: oneshot::Receiver<Result<DurableRecord, String>>,
}

impl PendingRecord {
    pub async fn durable(self) -> Result<DurableRecord> {
        self.reply
            .await
            .context("journal worker stopped before acknowledgement")?
            .map_err(anyhow::Error::msg)
    }
}

impl Clone for JournalWorker {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            join: None,
        }
    }
}

impl JournalWorker {
    pub fn start(
        root: impl AsRef<Path>,
        segment_limit: u64,
        queue_capacity: usize,
    ) -> Result<Self> {
        Self::start_with_disk_floor(root, segment_limit, queue_capacity, 0)
    }

    pub fn start_with_disk_floor(
        root: impl AsRef<Path>,
        segment_limit: u64,
        queue_capacity: usize,
        minimum_disk_free_bytes: u64,
    ) -> Result<Self> {
        ensure!(
            (1..=65_536).contains(&queue_capacity),
            "journal queue must be bounded 1..65536"
        );
        let root = root.as_ref().to_path_buf();
        let (sender, mut receiver) = mpsc::channel(queue_capacity);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("observation-journal".into())
            .spawn(move || {
                let mut journal = match ObservationJournal::open_with_disk_floor(
                    root,
                    segment_limit,
                    minimum_disk_free_bytes,
                ) {
                    Ok(journal) => {
                        let _ = ready_tx.send(Ok(()));
                        journal
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("{error:#}")));
                        return;
                    }
                };
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        Command::Record(value, reply) => {
                            let result = journal
                                .append_durable(value)
                                .map_err(|error| format!("{error:#}"));
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            if failed {
                                break;
                            }
                        }
                        Command::Checkpoint(mut value, reply) => {
                            let cursor = JournalCursor {
                                segment: journal.manifest.segment,
                                sequence: journal.manifest.durable_sequence,
                                hash: journal.manifest.durable_hash.clone(),
                            };
                            let result = serde_json::to_value(cursor)
                                .map_err(|error| format!("{error:#}"))
                                .and_then(|cursor| {
                                    if let Some(checkpoint) = value.get_mut("checkpoint") {
                                        checkpoint["cursor"] = cursor;
                                        journal
                                            .append_durable(value)
                                            .map_err(|error| format!("{error:#}"))
                                    } else {
                                        Err("checkpoint record missing checkpoint object".into())
                                    }
                                });
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            if failed {
                                break;
                            }
                        }
                        Command::Barrier(reply) => {
                            let cursor = JournalCursor {
                                segment: journal.manifest.segment,
                                sequence: journal.manifest.durable_sequence,
                                hash: journal.manifest.durable_hash.clone(),
                            };
                            let _ = reply.send(Ok(cursor));
                        }
                        Command::Stop(reply) => {
                            let _ = reply.send(());
                            break;
                        }
                    }
                }
            })?;
        ready_rx
            .recv()
            .context("journal worker startup ended")?
            .map_err(anyhow::Error::msg)?;
        Ok(Self {
            sender,
            join: Some(join),
        })
    }

    /// Uses `try_send`: full or closed queues are explicit loss conditions, never unbounded waits.
    pub fn submit(&self, payload: Value) -> Result<PendingRecord> {
        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(Command::Record(payload, reply))
            .map_err(|e| anyhow::anyhow!("observation journal queue unavailable: {e}"))?;
        Ok(PendingRecord { reply: result })
    }

    /// Atomically binds the checkpoint to the preceding durable cursor in journal order.
    pub fn submit_checkpoint(&self, payload: Value) -> Result<PendingRecord> {
        ensure!(
            payload.get("record_type").and_then(Value::as_str) == Some("checkpoint"),
            "invalid checkpoint record type"
        );
        ensure!(
            payload.get("checkpoint").is_some_and(Value::is_object),
            "checkpoint record missing state object"
        );
        let (reply, result) = oneshot::channel();
        self.sender
            .try_send(Command::Checkpoint(payload, reply))
            .map_err(|error| anyhow::anyhow!("observation journal queue unavailable: {error}"))?;
        Ok(PendingRecord { reply: result })
    }

    pub async fn try_record(&self, payload: Value) -> Result<(u64, String)> {
        let record = self.submit(payload)?.durable().await?;
        Ok((record.cursor.sequence, record.cursor.hash))
    }

    /// Wait for earlier queued rows, then return the exact durable hash-chain cursor.
    pub async fn barrier(&self) -> Result<JournalCursor> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::Barrier(reply))
            .await
            .context("observation journal worker stopped before barrier submission")?;
        result
            .await
            .context("journal worker stopped before barrier")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn shutdown(mut self) -> Result<()> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(Command::Stop(reply))
            .await
            .context("journal worker stopped before shutdown")?;
        result
            .await
            .context("journal worker shutdown acknowledgement missing")?;
        if let Some(join) = self.join.take() {
            join.join()
                .map_err(|_| anyhow::anyhow!("journal worker panicked"))?;
        }
        Ok(())
    }
}

fn record_hash(record: &JournalRecord) -> Result<String> {
    Ok(sha256(&serde_json::to_vec(record)?))
}
fn zero_hash() -> String {
    "0".repeat(64)
}
fn sha256_file(path: &Path) -> Result<String> {
    let mut reader = File::open(path)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut reader, &mut Sha256Writer(&mut hasher))?;
    Ok(format!("{:x}", hasher.finalize()))
}
fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn segment_path(root: &Path, segment: u64) -> PathBuf {
    root.join(format!("segment-{segment:08}.jsonl"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{collections::BTreeMap, io::Write};

    fn temp_dir(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("okx-observation-{name}-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        path
    }

    #[test]
    fn journal_fsync_boundary_reopens_and_verifies_chain() {
        let root = temp_dir("reopen");
        let mut journal = ObservationJournal::open(&root, 4096).unwrap();
        let (sequence, hash) = journal
            .append(json!({"kind":"trade","trade_id":"7"}))
            .unwrap();
        assert_eq!(sequence, 1);
        drop(journal);
        let replay = ObservationJournal::open(&root, 4096).unwrap();
        assert_eq!(replay.manifest.durable_sequence, 1);
        assert_eq!(replay.manifest.durable_hash, hash);
        assert_eq!(replay.manifest.segments.len(), 1);
        drop(replay);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streaming_verifier_matches_bounded_reader_and_rejects_over_limit() {
        let root = temp_dir("stream-verify");
        let mut journal = ObservationJournal::open(&root, 4096).unwrap();
        for index in 0..4 {
            journal
                .append(json!({"event":"row","index":index}))
                .unwrap();
        }
        drop(journal);
        let expected = read_verified_records(&root).unwrap();
        let mut streamed = Vec::new();
        visit_verified_records(&root, |entry| {
            streamed.push(entry);
            Ok(())
        })
        .unwrap();
        assert_eq!(streamed, expected);
        assert!(read_verified_records_limited(&root, 3).is_err());
        assert_eq!(read_verified_records_limited(&root, 4).unwrap(), expected);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streaming_verifier_rejects_oversized_rows_before_unbounded_allocation() {
        let root = temp_dir("stream-row-bound");
        let mut journal = ObservationJournal::open(&root, 16 * 1024 * 1024).unwrap();
        journal.append(json!({"event":"small"})).unwrap();
        drop(journal);
        let mut segment = OpenOptions::new()
            .append(true)
            .open(segment_path(&root, 0))
            .unwrap();
        segment
            .write_all(&vec![b'x'; MAX_JOURNAL_ROW_BYTES + 1])
            .unwrap();
        segment.write_all(b"\n").unwrap();
        drop(segment);
        let error = visit_verified_records(&root, |_| Ok(())).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("journal row exceeds configured maximum")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn configured_disk_floor_refuses_append_before_mutating_durable_prefix() {
        let root = temp_dir("disk-floor");
        let mut journal = ObservationJournal::open_with_disk_floor(&root, 4096, u64::MAX).unwrap();
        assert!(journal.append(json!({"event":"trade"})).is_err());
        assert_eq!(journal.manifest.durable_sequence, 0);
        assert_eq!(fs::metadata(segment_path(&root, 0)).unwrap().len(), 0);
        drop(journal);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rotation_persists_and_reverifies_every_segment_after_restart() {
        let root = temp_dir("rotation");
        let mut journal = ObservationJournal::open(&root, 4096).unwrap();
        for index in 0..12 {
            journal
                .append(json!({"kind":"raw","index":index,"body":"x".repeat(900)}))
                .unwrap();
        }
        assert!(journal.manifest.segments.len() > 1);
        let segments = journal.manifest.segments.clone();
        drop(journal);
        let reopened = ObservationJournal::open(&root, 4096).unwrap();
        assert_eq!(reopened.manifest.segments, segments);
        assert!(segments.iter().enumerate().all(|(index, segment)| {
            let path = root.join(&segment.path);
            sha256_file(&path).unwrap() == segment.sha256
                && verify_segment(&path, index as u64).unwrap().0 == segment.records
        }));
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unacknowledged_tail_is_preserved_then_excluded_from_replay() {
        let root = temp_dir("tail");
        let mut journal = ObservationJournal::open(&root, 4096).unwrap();
        journal
            .append(json!({"kind":"trade","trade_id":"8"}))
            .unwrap();
        drop(journal);
        let path = segment_path(&root, 0);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{torn").unwrap();
        file.sync_all().unwrap();
        let recovered = ObservationJournal::open(&root, 4096).unwrap();
        assert_eq!(recovered.manifest.durable_sequence, 1);
        assert_eq!(verify_segment(&path, 0).unwrap().0, 1);
        assert!(
            fs::read_dir(&root)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().contains("torn-"))
        );
        drop(recovered);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupted_durable_record_fails_closed() {
        let root = temp_dir("corrupt");
        let mut journal = ObservationJournal::open(&root, 4096).unwrap();
        journal
            .append(json!({"kind":"trade","trade_id":"9"}))
            .unwrap();
        drop(journal);
        let path = segment_path(&root, 0);
        let mut bytes = fs::read(&path).unwrap();
        let index = bytes.iter().position(|byte| *byte == b'9').unwrap();
        bytes[index] = b'0';
        fs::write(&path, bytes).unwrap();
        assert!(ObservationJournal::open(&root, 4096).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tape_seal_requires_full_stream_coverage_and_freezes_segment_inventory() {
        let root = temp_dir("seal");
        let mut journal = ObservationJournal::open(&root, 4096).unwrap();
        journal
            .append(json!({"record_type":"trade","schema_version":"okx.observations.record.v1"}))
            .unwrap();
        let receipt = |channel: &str| TapeStreamReceipt {
            channel: channel.into(),
            instrument_id: "okx:spot:BTC-USDC".into(),
            coverage_start_ms: 100,
            coverage_end_ms: 200,
            record_count: 1,
            gap_count: 0,
            subscription_acknowledged: channel == "metadata"
                || channel == "trades-all"
                || channel == "bbo-tbt",
            metadata_revision: "fixture-meta-v1".into(),
            receipt_source: "capture_report".into(),
        };
        let input = TapeSealInput {
            environment: "public".into(),
            region: "global".into(),
            source_profile: "okx_global_public_v1".into(),
            producer_revision: "fixture-commit".into(),
            capture_session: "fixture-session".into(),
            availability_semantics: "source_receipt_only_publication_unavailable".into(),
            scored_start_ms: 100,
            scored_end_ms: 200,
            sealed_at_ms: 201,
            streams: vec![
                receipt("trades-all"),
                receipt("bbo-tbt"),
                receipt("metadata"),
            ],
        };
        let path = journal.seal_tape(input.clone()).unwrap();
        let manifest: ObservationTapeManifest =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(manifest.schema_version, "research.okx_observation_tape.v1");
        assert_eq!(manifest.replay_mode, "observed_availability");
        assert_eq!(
            manifest.availability_semantics,
            "source_receipt_only_publication_unavailable"
        );
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(
            manifest.segments[0].sha256,
            sha256_file(&segment_path(&root, 0)).unwrap()
        );
        assert!(journal.append(json!({"kind":"must not mutate"})).is_err());
        drop(journal);
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn checkpoint_cursor_is_bound_atomically_to_previous_durable_record() {
        let root = temp_dir("checkpoint");
        let worker = JournalWorker::start(&root, 16 * 1024, 8).unwrap();
        let first = worker
            .submit(json!({"event":"timer","available_at_ms":60_000,"qualified_source":true,"timer_kind":"fixed_time_bar"}))
            .unwrap()
            .durable()
            .await
            .unwrap();
        let dedup = crate::observations::TradeDeduplicator::new(8).unwrap();
        let bars = crate::observations::FixedTimeBars::new_at(60_000, 0).unwrap();
        let checkpoint = crate::observations::ReplayCheckpoint {
            schema_version: "okx.observations.replay-checkpoint.v1".into(),
            channel: "trades-all".into(),
            cursor: JournalCursor {
                segment: 0,
                sequence: 0,
                hash: zero_hash(),
            },
            bar_duration_ms: 60_000,
            configuration_identity: "fixture-config".into(),
            dedup: dedup.checkpoint(),
            time_bars: BTreeMap::from([("okx:spot:BTC-USDC".into(), bars)]),
            notional_bars: BTreeMap::new(),
        };
        let persisted = worker
            .submit_checkpoint(
                json!({"record_type":"checkpoint","channel":"trades-all","checkpoint":checkpoint}),
            )
            .unwrap()
            .durable()
            .await
            .unwrap();
        let saved: crate::observations::ReplayCheckpoint =
            serde_json::from_value(persisted.payload["checkpoint"].clone()).unwrap();
        assert_eq!(saved.cursor, first.cursor);
        worker.shutdown().await.unwrap();
        let loaded = crate::observations::ObservationReplay::latest_checkpoint(&root, "trades-all")
            .unwrap()
            .unwrap();
        assert_eq!(loaded.cursor, first.cursor);
        assert_eq!(loaded.cursor, saved.cursor);
        std::fs::remove_dir_all(root).unwrap();
    }
}
