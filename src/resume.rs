use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::shared_types::ChunkRange;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DownloadState {
    pub(crate) url: String,
    pub(crate) file_size: u64,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
    pub(crate) completed_chunks: Vec<ChunkRange>,
}

impl DownloadState {
    pub(crate) fn new(
        url: &Url,
        file_size: u64,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Self {
        Self { url: url.to_string(), file_size, etag, last_modified, completed_chunks: vec![] }
    }

    /// Returns `true` if this state is compatible with the given URL and server
    /// metadata — i.e. it is safe to resume the partial download.
    pub(crate) fn is_valid_for(
        &self,
        url: &Url,
        file_size: Option<u64>,
        etag: &Option<String>,
        last_modified: &Option<String>,
    ) -> bool {
        if self.url != url.to_string() {
            warn!(
                "Resume state URL mismatch: stored={}, current={}",
                self.url, url
            );
            return false;
        }
        if let Some(size) = file_size {
            if self.file_size != size {
                warn!(
                    "Resume state file size mismatch: stored={}, current={}",
                    self.file_size, size
                );
                return false;
            }
        }
        // ETag is the strongest cache validator — check it first.
        if let (Some(stored), Some(current)) = (&self.etag, etag) {
            if stored != current {
                warn!("ETag changed: stored={}, current={}", stored, current);
                return false;
            }
        }
        // Fall back to Last-Modified only when neither side has an ETag.
        if self.etag.is_none() && etag.is_none() {
            if let (Some(stored), Some(current)) = (&self.last_modified, last_modified) {
                if stored != current {
                    warn!(
                        "Last-Modified changed: stored={}, current={}",
                        stored, current
                    );
                    return false;
                }
            }
        }
        true
    }
}

pub(crate) fn part_path(output: &str) -> String {
    format!("{}.part", output)
}

pub(crate) fn state_path(output: &str) -> String {
    format!("{}.dlrs", output)
}

pub(crate) async fn load_state(path: &str) -> Option<DownloadState> {
    let content = tokio::fs::read_to_string(path).await.ok()?;
    serde_json::from_str(&content).ok()
}

async fn discard_partial(part: &str, state: &str) {
    tokio::fs::remove_file(part).await.ok();
    tokio::fs::remove_file(state).await.ok();
}

pub(crate) async fn save_state(path: &str, state: &DownloadState) -> anyhow::Result<()> {
    let content = serde_json::to_string(state).expect("state serialization failed");
    tokio::fs::write(path, content).await.with_context(|| format!("failed to write resume state to {path}"))
}

/// Inspects the sidecar files for `output` and returns the existing
/// `DownloadState` if the download can be resumed, or `None` if it should
/// start fresh.  Returns an error when inconsistent or mismatched state is
/// found and `force` is not set.
pub(crate) async fn resolve(
    output: &str,
    url: &Url,
    file_size: Option<u64>,
    etag: &Option<String>,
    last_modified: &Option<String>,
    force: bool,
) -> anyhow::Result<Option<DownloadState>> {
    let part = part_path(output);
    let state = state_path(output);
    let part_exists = tokio::fs::metadata(&part).await.is_ok();
    let state_exists = tokio::fs::metadata(&state).await.is_ok();

    match (part_exists, state_exists) {
        (true, true) => match load_state(&state).await {
            Some(s) if s.is_valid_for(url, file_size, etag, last_modified) => Ok(Some(s)),
            Some(_) if force => {
                warn!("Resume state invalid; restarting from scratch (--force)");
                discard_partial(&part, &state).await;
                Ok(None)
            }
            Some(_) => bail!(
                "Resume state validation failed (URL, size, or ETag mismatch). \
                 Use --force to discard the partial download and restart."
            ),
            None if force => {
                warn!("Corrupt resume state; restarting from scratch (--force)");
                discard_partial(&part, &state).await;
                Ok(None)
            }
            None => bail!(
                "Found a partial download but the resume state is corrupt or \
                 unreadable. Use --force to discard and restart."
            ),
        },
        (true, false) => {
            if force {
                warn!("Found .part file without resume state; restarting (--force)");
                tokio::fs::remove_file(&part).await.ok();
                Ok(None)
            } else {
                bail!(
                    "Found partial file '{part}' without a resume state file. \
                     Use --force to discard it and restart."
                )
            }
        }
        (false, true) => {
            // Stale state file with no matching .part — silently remove it.
            tokio::fs::remove_file(&state).await.ok();
            Ok(None)
        }
        (false, false) => Ok(None),
    }
}
