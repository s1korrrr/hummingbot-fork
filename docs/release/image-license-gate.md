# Final-image license evidence gate

The Hummingbot release-image workflow is fail-closed on
`licenses/image-license-policy.json`. The committed policy remains
`HOLD_FOR_EVIDENCE`; it must not be changed to `APPROVED` until a named human
reviewer has accepted the exact, platform-specific evidence.

## What the image records

Every final image generates
`/usr/share/licenses/hummingbot/third-party-license-inventory.json`. The
inventory records the exact source commit for provenance and binds the
platform, Miniforge base digest, and final installed package set into the
canonical dependency-evidence hash:

- Conda name, version, build, source URL, artifact SHA-256, and declared
  license for both the base and `hummingbot` prefixes;
- direct pip source URL, archive SHA-256, version, and declared license;
- installed PyPI distribution version, license metadata, and installed
  `RECORD` hash, with unresolved artifact sources stated explicitly;
- final dpkg name, version, architecture, source package, and a hash of its
  Debian copyright evidence.

The image also preserves the Hummingbot, Miniforge, and `miniforge-images`
license texts under `/usr/share/licenses/hummingbot/`.

## Approval boundary

This gate proves mechanical completeness against recorded evidence. It does
not make a legal determination. Package-specific redistribution duties,
copyleft or source-offer requirements, hosted-service terms, and intended use
must be reviewed by the operator or counsel.

The approval hash is intentionally scoped to dependency and notice evidence.
It does not include `source_commit`, does not approve the application source
tree, and must not be cited as source-review, binary-reproducibility, security,
or release-readiness evidence. The inventory's `source_commit` field remains a
separate provenance field because binding it into a policy stored in the same
source tree would create a circular commit dependency. Source review and exact
image identity require their own independent gates.

An approval record must:

1. bind the exact Miniforge base digest and both target platforms;
2. bind each platform's reviewed canonical evidence-set SHA-256, covering the
   exact package records, unresolved-source state, dpkg status, base digest,
   platform, and preserved notice hashes;
3. name the human reviewer;
4. clear `hold_reason` and record a nonempty `approval_reason` describing the
   reviewed disposition;
5. carry an approval time and a future expiry time; and
6. set both `decision` to `APPROVED` and `publication_allowed` to `true`.

The workflow validates that record before release confirmation, registry
login, or publication. A missing, expired, malformed, or held record stops the
release. This approval record is necessary but not sufficient while dependency
inputs remain mutable or an inventory contains unresolved artifact sources.

## Locked builder-environment evidence

The release Docker build validates all committed dependency locks offline
before it installs anything. `TARGETPLATFORM` then selects exactly one reviewed
pair: `linux/amd64` uses `linux-64.explicit.txt` and
`pip-injective-linux-64.lock.txt`; `linux/arm64` uses
`linux-aarch64.explicit.txt` and
`pip-injective-linux-aarch64.lock.txt`. Empty and unsupported target values fail
closed. There is no `environment.yml` solve or broad-spec fallback.

The selected `@EXPLICIT` Conda lock creates `/opt/conda/envs/hummingbot`.
The target Injective closure and direct EIP-712 lock are installed separately
with hashes required, dependency and build isolation disabled, and distinct
pip reports. The builder preserves canonical source recipes, selected locks,
provenance, target mapping, final Conda lists, explicit package URLs, Python
version, and installed-requirement evidence under `/opt/conda/image-evidence`
and `/opt/conda/conda-meta`.

After the direct EIP-712 install, the image executes a deterministic EIP-712
signable-bytes operation and checks the canonical empty-input Keccak-256 digest.
That runtime reversal gate proves the reviewed `safe-pysha3` substitution works
for the named operation. Generic `pip check` remains intentionally non-green
because `eip712-structs` metadata still requests `pysha3`; it is diagnostic,
not a claimed green gate.

This is local target-image dependency/runtime evidence only. It is not proof of
the compiled application, final-image license disposition, legal clearance,
registry publication, deployment, service health, or trading readiness.

## Current remaining evidence

- exact final-image dependency inventories for `linux/amd64` and `linux/arm64`,
  with their separately recorded source SHA;
- resolution or explicit reviewed disposition for every unresolved artifact
  source;
- package-specific redistribution review for the complete per-platform sets;
- exact final-image confirmation that the reviewed locks and builder evidence
  remain unchanged through source compilation and runtime assembly;
- explicit operator or counsel approval.

Do not treat a green unit test, a local single-platform build, metadata alone,
this dependency approval, or an image digest alone as source approval, legal
clearance, release readiness, or publication authority.
