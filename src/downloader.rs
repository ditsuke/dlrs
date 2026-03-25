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
use crate::resource::ResourceError;
use crate::resource::ResourceHandle;
use crate::shared_types::{ByteCount, ChunkRange};

const MB_TO_BYTES: u64 = 1024 * 1024;
const CHUNK_SIZE: u64 = 2 * MB_TO_BYTES;
const MAX_RETRIES: u32 = 3;

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
}

pub(crate) async fn start_download(
    specs: DownloadPreferences,
    multi: MultiProgress,
) -> Result<(), Box<dyn Error>> {
    let resource_handle = ResourceHandle::try_from(&specs.url)?;

    let resource_specs = resource_handle.get_specs().await?;
    debug!("File to download has specs: {:?}", resource_specs);

    let output_filename = match specs.output {
        Some(filename) => filename,
        None => {
            if let Some(filename) = resource_specs.inferred_filename {
                filename
            } else {
                warn!("Could not infer filename from URL or headers; saving as \"download\"");
                "download".into() // TODO: come up with a better default way
            }
        }
    };
    debug!("Output filename: {}", output_filename);

    let file_size = resource_specs.size;
    if file_size.is_none() {
        warn!("Server did not send Content-Length; progress and split downloads unavailable");
    }
    let chunk_count = match (file_size, resource_specs.supports_splits) {
        (Some(size), true) => Some(((size + CHUNK_SIZE - 1) / CHUNK_SIZE) as u32),
        _ => None,
    };
    debug!("File size: {:?}, chunk count: {chunk_count:?}", file_size);
    let chunk_bounds = match chunk_count {
        Some(count) => (0..count)
            .map(|i| {
                let start = i as u64 * CHUNK_SIZE;
                let end = cmp::min(start + CHUNK_SIZE, file_size.unwrap_or(0));
                Some(ChunkRange { start, end })
            })
            .collect::<Vec<_>>(),
        None => vec![None],
    };

    let download_worker_count = if resource_specs.supports_splits {
        specs.preferred_splits
    } else {
        if specs.preferred_splits > 1 {
            warn!(
                "Server does not support range requests; ignoring --splits={}, downloading with 1 worker",
                specs.preferred_splits
            );
        }
        1
    };
    debug!(
        "Downloading {} with {} worker(s), {} chunk(s)",
        specs.url,
        download_worker_count,
        chunk_count.unwrap_or(1)
    );

    let (tx_update, rx_update) = mpsc::channel::<DownloadUpdate>(download_worker_count as usize);
    let (tx_chunk_spec, rx_chunk_spec) =
        async_channel::bounded::<ChunkSpec>(download_worker_count as usize * 4);
    let (tx_progress, rx_progress) = mpsc::channel::<u64>(download_worker_count as usize);

    let mut worker_handles = vec![];

    // FIXME: define truncate behavior
    let output_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(output_filename)
        .await?;

    // TODO: we'd have to leave this untouched if we're resuming a download
    output_file.set_len(file_size.unwrap_or(0)).await?;

    worker_handles.push(
        Writer {
            output_file,
            r_chunks: rx_update,
        }
        .spawn(),
    );

    let download_worker = DownloadWorker {
        rx_chunk_spec: rx_chunk_spec.clone(),
        tx_update: tx_update.clone(),
        tx_progress: tx_progress.clone(),
    };
    for _ in 0..download_worker_count {
        worker_handles.push(download_worker.clone().spawn());
    }

    // Drop unused clones
    drop(tx_update);
    drop(download_worker);

    spawn_progress_reporter(file_size, rx_progress, multi);

    let resource_handle = Arc::new(resource_handle);
    tokio::spawn(async move {
        for bound in chunk_bounds {
            if tx_chunk_spec
                .send((resource_handle.clone(), bound))
                .await
                .is_err()
            {
                break; // all workers gone (e.g. fatal error already aborted them)
            }
        }
    });

    futures::future::join_all(worker_handles).await;
    debug!("exiting download function");
    Ok(())
}

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
                        match stream.try_next().await {
                            Ok(Some(chunk)) => {
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
                            Ok(None) => {
                                stream_err = None;
                                break;
                            }
                            Err(e) => {
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
                        self.tx_update
                            .send(DownloadUpdate::FatalError(e))
                            .await
                            .ok();
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
            // Dropping s_chunks here signals the writer that one fewer worker is active.
        })
    }
}

struct Writer {
    output_file: File,
    r_chunks: mpsc::Receiver<DownloadUpdate>,
}

impl Writer {
    fn spawn(self) -> JoinHandle<()> {
        let mut r_chunks = self.r_chunks;
        let mut output_file = self.output_file;

        tokio::spawn(async move {
            // Loop exits either when all worker senders drop (channel closed = success)
            // or on a FatalError from a worker.
            while let Some(update) = r_chunks.recv().await {
                match update {
                    DownloadUpdate::Chunk(chunk) => {
                        write_file_chunk(&mut output_file, &chunk).await;
                    }
                    DownloadUpdate::FatalError(e) => {
                        error!("Download failed: {}. Aborting.", e);
                        break;
                    }
                }
            }
            output_file.shutdown().await.expect("file shutdown failed");
        })
    }
}

async fn write_file_chunk(file: &mut File, chunk: &FileChunk) {
    if let Some(range) = &chunk.1 {
        file.seek(SeekFrom::Start(range.start))
            .await
            .expect("seek failed");
    }
    file.write_all(chunk.0.as_ref())
        .await
        .expect("write failed");
}
