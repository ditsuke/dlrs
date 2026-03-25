use std::cmp;
use std::error::Error;
use std::io::SeekFrom;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use indicatif::MultiProgress;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::{sync::mpsc, task::JoinHandle};
use url::Url;

use crate::progress_reporter::spawn_progress_reporter;
use crate::resource::{ResourceError, ResourceHandle};
use crate::resume::{self, DownloadState};
use crate::shared_types::{ByteCount, ChunkRange};

const MB_TO_BYTES: u64 = 1024 * 1024;
const CHUNK_SIZE: u64 = 2 * MB_TO_BYTES;
const MAX_RETRIES: u32 = 3;
const STALL_TIMEOUT: Duration = Duration::from_secs(30);

type ChunkBoundaries = Option<ChunkRange>;
type FileChunk = (Bytes, ChunkBoundaries);
type ChunkSpec = (Arc<ResourceHandle>, ChunkBoundaries);

enum DownloadUpdate {
    Chunk(FileChunk),
    FatalError(ResourceError),
}

pub(crate) struct DownloadPreferences {
    pub(crate) url: Url,
    pub(crate) preferred_splits: u8,
    pub(crate) output: Option<String>,
    pub(crate) force: bool,
}

/// Tracks the paths and in-progress state needed to persist resume checkpoints
/// and perform the final rename from `.part` to the real output on success.
struct ResumeContext {
    part_path: String,
    final_path: String,
    state_path: String,
    state: DownloadState,
}

pub(crate) async fn start_download(
    prefs: DownloadPreferences,
    multi: Option<MultiProgress>,
) -> Result<(), Box<dyn Error>> {
    let handle = ResourceHandle::try_from(&prefs.url)?;
    let specs = handle.get_specs().await?;
    debug!("resource specs: {:?}", specs);

    if specs.size.is_none() {
        warn!("Server did not send Content-Length; progress and split downloads unavailable");
    }

    let output = prefs
        .output
        .or(specs.inferred_filename)
        .unwrap_or_else(|| {
            warn!("Could not infer filename from URL or headers; saving as \"download\"");
            "download".into()
        });

    // Only range-capable downloads with a known size can be chunked and resumed.
    let is_chunked = specs.supports_splits && specs.size.is_some();
    let (part_path, state_path) = (resume::part_path(&output), resume::state_path(&output));

    let resume_state = if is_chunked {
        let state =
            resume::resolve(&output, &prefs.url, specs.size, &specs.etag, &specs.last_modified, prefs.force)
                .await?;
        if let Some(ref s) = state {
            let total = chunk_count(specs.size.unwrap());
            debug!("resuming: {}/{total} chunks already done", s.completed_chunks.len());
        }
        state
    } else {
        None
    };

    let is_resuming = resume_state.is_some();

    if !is_resuming && tokio::fs::metadata(&output).await.is_ok() && !prefs.force {
        return Err(format!("output file '{output}' already exists; use --force to overwrite").into());
    }

    let pending = pending_chunks(specs.size, specs.supports_splits, resume_state.as_ref());

    let resume_ctx = if is_chunked {
        let state = if is_resuming {
            resume_state.unwrap()
        } else {
            let s = DownloadState::new(&prefs.url, specs.size.unwrap(), specs.etag, specs.last_modified);
            resume::save_state(&state_path, &s).await?;
            s
        };
        Some(ResumeContext { part_path: part_path.clone(), final_path: output.clone(), state_path, state })
    } else {
        None
    };

    let worker_count = effective_worker_count(specs.supports_splits, prefs.preferred_splits);
    debug!("downloading {} — {worker_count} worker(s), {} chunk(s)", prefs.url, pending.len());

    let (tx_update, rx_update) = mpsc::channel(worker_count as usize);
    let (tx_spec, rx_spec) = async_channel::bounded(worker_count as usize * 4);
    let (tx_progress, rx_progress) = mpsc::channel(worker_count as usize);

    let write_path = if is_chunked { &part_path } else { &output };
    let file = open_output_file(write_path, (!is_resuming).then_some(specs.size.unwrap_or(0))).await?;

    let worker = DownloadWorker {
        rx_chunk_spec: rx_spec.clone(),
        tx_update: tx_update.clone(),
        tx_progress: tx_progress.clone(),
    };
    let handles: Vec<JoinHandle<()>> = std::iter::once(Writer { output_file: file, r_chunks: rx_update, resume_ctx }.spawn())
        .chain((0..worker_count).map(|_| worker.clone().spawn()))
        .collect();
    drop((tx_update, worker));

    match multi {
        Some(m) => { spawn_progress_reporter(specs.size, rx_progress, m); }
        None => drop(rx_progress), // workers use try_send; dropping the receiver is safe
    }

    let handle = Arc::new(handle);
    tokio::spawn(async move {
        for bound in pending {
            if tx_spec.send((handle.clone(), bound)).await.is_err() {
                break; // all workers gone (fatal error already aborted them)
            }
        }
    });

    futures::future::join_all(handles).await;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns an iterator over all [`ChunkRange`]s for a file of the given size.
fn chunk_ranges(file_size: u64) -> impl Iterator<Item = ChunkRange> {
    (0..chunk_count(file_size)).map(move |i| {
        let start = i * CHUNK_SIZE;
        let end = cmp::min(start + CHUNK_SIZE, file_size) - 1;
        ChunkRange { start, end }
    })
}

fn chunk_count(file_size: u64) -> u64 {
    (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE
}

/// Returns the list of chunk boundaries that still need to be downloaded,
/// filtering out any ranges already recorded in a prior partial run.
fn pending_chunks(
    file_size: Option<u64>,
    supports_splits: bool,
    resume_state: Option<&DownloadState>,
) -> Vec<ChunkBoundaries> {
    let completed = resume_state.map(|s| s.completed_chunks.as_slice()).unwrap_or(&[]);
    match (file_size, supports_splits) {
        (Some(size), true) => chunk_ranges(size)
            .filter(|r| !completed.contains(r))
            .map(Some)
            .collect(),
        _ => vec![None],
    }
}

fn effective_worker_count(supports_splits: bool, preferred: u8) -> u8 {
    if !supports_splits {
        if preferred > 1 {
            warn!(
                "Server does not support range requests; ignoring --splits={preferred}, \
                 downloading with 1 worker"
            );
        }
        return 1;
    }
    preferred
}

async fn open_output_file(path: &str, preallocate: Option<u64>) -> tokio::io::Result<File> {
    let file = OpenOptions::new().create(true).read(true).write(true).open(path).await?;
    if let Some(size) = preallocate {
        file.set_len(size).await?;
    }
    Ok(file)
}

// ── Worker ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct DownloadWorker {
    rx_chunk_spec: async_channel::Receiver<ChunkSpec>,
    tx_update: mpsc::Sender<DownloadUpdate>,
    tx_progress: mpsc::Sender<ByteCount>,
}

impl DownloadWorker {
    fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            'outer: while let Ok((handle, bounds)) = self.rx_chunk_spec.recv().await {
                let mut attempts = 0u32;
                'retry: loop {
                    let stream = handle.stream_range(bounds, self.tx_progress.clone()).await;
                    tokio::pin!(stream);

                    let stream_err: Option<ResourceError>;
                    loop {
                        match tokio::time::timeout(STALL_TIMEOUT, stream.try_next()).await {
                            Err(_elapsed) => {
                                stream_err = Some(ResourceError::Stall(STALL_TIMEOUT));
                                break;
                            }
                            Ok(Ok(Some(chunk))) => {
                                debug!("sending chunk for bounds {bounds:?} to writer");
                                if self
                                    .tx_update
                                    .send(DownloadUpdate::Chunk((chunk, bounds)))
                                    .await
                                    .is_err()
                                {
                                    break 'outer; // writer gone
                                }
                            }
                            Ok(Ok(None)) => {
                                stream_err = None;
                                break;
                            }
                            Ok(Err(e)) => {
                                stream_err = Some(e);
                                break;
                            }
                        }
                    }

                    let Some(e) = stream_err else { break 'retry }; // None = chunk succeeded
                    if attempts >= MAX_RETRIES {
                        error!(
                            "Chunk {:?} failed after {} attempts: {}. Aborting download.",
                            bounds, MAX_RETRIES, e
                        );
                        self.tx_update.send(DownloadUpdate::FatalError(e)).await.ok();
                        break 'outer;
                    }
                    let delay = Duration::from_millis(500 * (1u64 << attempts.min(4)));
                    warn!(
                        "Chunk {:?} failed (attempt {}/{}): {}. Retrying in {:?}...",
                        bounds,
                        attempts + 1,
                        MAX_RETRIES,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    attempts += 1;
                }
            }
            // Dropping tx_update here signals the writer that one fewer worker is active.
        })
    }
}

// ── Writer ────────────────────────────────────────────────────────────────────

struct Writer {
    output_file: File,
    r_chunks: mpsc::Receiver<DownloadUpdate>,
    resume_ctx: Option<ResumeContext>,
}

impl Writer {
    fn spawn(self) -> JoinHandle<()> {
        let mut r_chunks = self.r_chunks;
        let mut output_file = self.output_file;
        let mut resume_ctx = self.resume_ctx;

        tokio::spawn(async move {
            // Loop exits either when all worker senders drop (channel closed = success)
            // or on a FatalError from a worker.
            let mut succeeded = true;
            while let Some(update) = r_chunks.recv().await {
                match update {
                    DownloadUpdate::Chunk(chunk) => {
                        write_file_chunk(&mut output_file, &chunk).await;
                        // Record the completed chunk *after* it is on disk so a
                        // crash between writes causes at most one re-download.
                        if let (Some(ctx), Some(range)) = (&mut resume_ctx, chunk.1) {
                            ctx.state.completed_chunks.push(range);
                            resume::save_state(&ctx.state_path, &ctx.state)
                                .await
                                .expect("failed to save resume state");
                        }
                    }
                    DownloadUpdate::FatalError(e) => {
                        error!("Download failed: {}. Aborting.", e);
                        succeeded = false;
                        break;
                    }
                }
            }
            output_file.shutdown().await.expect("file shutdown failed");

            // On success rename .part → final output and clean up the state file.
            // On failure the sidecar files are left intact for the next resume attempt.
            if succeeded {
                if let Some(ctx) = resume_ctx {
                    tokio::fs::rename(&ctx.part_path, &ctx.final_path)
                        .await
                        .expect("failed to rename .part to final output");
                    tokio::fs::remove_file(&ctx.state_path).await.ok();
                    debug!("renamed {} → {}", ctx.part_path, ctx.final_path);
                }
            }
        })
    }
}

async fn write_file_chunk(file: &mut File, chunk: &FileChunk) {
    if let Some(range) = &chunk.1 {
        file.seek(SeekFrom::Start(range.start)).await.expect("seek failed");
    }
    file.write_all(chunk.0.as_ref()).await.expect("write failed");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    /// Responds to GET requests with full body or a 206 partial response when a
    /// `Range: bytes=start-end` header is present. Used to simulate a server that
    /// supports split/parallel downloads.
    struct RangeAwareBody {
        data: Bytes,
    }

    impl Respond for RangeAwareBody {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let total = self.data.len();
            let range_value: Option<String> = request
                .headers
                .iter()
                .find(|(name, _)| name.as_str().eq_ignore_ascii_case("range"))
                .and_then(|(_, vals)| vals.iter().next().map(|v| v.as_str().to_owned()));

            if let Some(range_str) = range_value {
                let range_str = range_str.strip_prefix("bytes=").unwrap_or(&range_str).to_owned();
                let (start_str, end_str) = range_str.split_once('-').unwrap();
                let start: usize = start_str.trim().parse().unwrap();
                let end: usize = end_str.trim().parse().unwrap();
                let slice = self.data.slice(start..=end);
                let content_range = format!("bytes {start}-{end}/{total}");
                let content_length = slice.len().to_string();
                ResponseTemplate::new(206)
                    .insert_header("Content-Range", content_range.as_str())
                    .insert_header("Content-Length", content_length.as_str())
                    .set_body_bytes(slice)
            } else {
                let content_length = total.to_string();
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", content_length.as_str())
                    .set_body_bytes(self.data.clone())
            }
        }
    }

    /// Mounts HEAD and GET mocks for `/file` on a fresh MockServer.
    /// The HEAD response advertises range support and content length.
    /// The GET responder handles both full and ranged requests.
    async fn setup_server(data: Bytes) -> (MockServer, Url) {
        let server = MockServer::start().await;
        let total = data.len();
        let content_length = total.to_string();

        Mock::given(method("HEAD"))
            .and(path("/file"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Length", content_length.as_str())
                    .insert_header("Accept-Ranges", "bytes"),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/file"))
            .respond_with(RangeAwareBody { data })
            .mount(&server)
            .await;

        let url = format!("{}/file", server.uri()).parse().unwrap();
        (server, url)
    }

    #[tokio::test]
    async fn test_single_chunk_download() {
        let data = Bytes::from(vec![0xABu8; 1024]); // 1 KB — fits in one chunk
        let (_server, url) = setup_server(data.clone()).await;

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out").to_str().unwrap().to_string();

        start_download(
            DownloadPreferences { url, preferred_splits: 1, output: Some(output.clone()), force: false },
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), data.as_ref());
    }

    #[tokio::test]
    async fn test_multi_chunk_download() {
        // 5 MB — splits into three 2 MB chunks (2 MB, 2 MB, 1 MB)
        let data = Bytes::from((0u8..=255).cycle().take(5 * 1024 * 1024).collect::<Vec<_>>());
        let (_server, url) = setup_server(data.clone()).await;

        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("out").to_str().unwrap().to_string();

        start_download(
            DownloadPreferences { url, preferred_splits: 4, output: Some(output.clone()), force: false },
            None,
        )
        .await
        .unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), data.as_ref());
    }
}
