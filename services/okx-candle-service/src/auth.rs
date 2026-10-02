//! Consumer-scoped bearer authentication. Each consumer (an engine, a shadow reader, a
//! dashboard) holds its own token with a pair scope, a history ceiling and a stream cap, so a
//! misbehaving or compromised reader cannot widen what it may subscribe to or starve others.
use axum::http::HeaderMap;
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::Semaphore;

/// One authenticated reader. `pairs == None` means the whole catalog (legacy single token).
#[derive(Debug)]
pub struct Consumer {
    pub id: String,
    token: String,
    pub pairs: Option<BTreeSet<String>>,
    pub max_history: usize,
    pub streams: Arc<Semaphore>,
}

impl Consumer {
    pub fn new(
        id: impl Into<String>,
        token: impl Into<String>,
        pairs: Option<BTreeSet<String>>,
        max_history: usize,
        max_streams: usize,
    ) -> Self {
        Self {
            id: id.into(),
            token: token.into(),
            pairs,
            max_history,
            streams: Arc::new(Semaphore::new(max_streams)),
        }
    }
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn allows_pair(&self, pair: &str) -> bool {
        self.pairs.as_ref().is_none_or(|scope| scope.contains(pair))
    }
}

#[derive(Debug)]
pub struct Auth {
    consumers: Vec<Arc<Consumer>>,
}

impl Auth {
    pub fn new(consumers: Vec<Consumer>) -> Self {
        Self {
            consumers: consumers.into_iter().map(Arc::new).collect(),
        }
    }
    /// The historical single-token deployment: full catalog, full history, shared stream cap.
    pub fn single(token: String, max_history: usize, max_streams: usize) -> Self {
        Self::new(vec![Consumer::new(
            "default",
            token,
            None,
            max_history,
            max_streams,
        )])
    }
    pub fn consumers(&self) -> &[Arc<Consumer>] {
        &self.consumers
    }
    /// Constant-time over every configured token: no early exit on a match or mismatch.
    pub fn authenticate(&self, headers: &HeaderMap) -> Option<Arc<Consumer>> {
        let offered = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))?;
        let mut matched = None;
        for consumer in &self.consumers {
            if token_eq(offered, &consumer.token) && matched.is_none() {
                matched = Some(consumer.clone());
            }
        }
        matched
    }
}

fn token_eq(value: &str, token: &str) -> bool {
    if value.len() != token.len() {
        return false;
    }
    value
        .bytes()
        .zip(token.bytes())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}
