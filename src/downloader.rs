use std::cmp;
use std::error::Error;
use std::io::SeekFrom;

use bytes::Bytes;

use futures::{future, TryStreamExt};

use indicatif::MultiProgress;

use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use tokio::{sync::mpsc, task::JoinHandle};
use url::Url;

use crate::http_utils::GetError;
use crate::progress_reporter::spawn_progress_reporter;
use crate::resource::{self};

use crate::resource::DownloadUrl;
use crate::shared_types::{ByteCount, ChunkRange};

const MB_TO_BYTES: u32 = 1024 * 1024;
const CHUNK_SIZE: u32 = 2 * MB_TO_BYTES;

type ChunkBoundaries = Option<ChunkRange>;
type FileChunk = (Bytes, ChunkBoundaries);
type ChunkSpec = (DownloadUrl, ChunkBoundaries);
enum DownloadUpdate {
    Chunk(Result<FileChunk, (GetError, ChunkSpec)>),
    Complete,
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
    let url = resource::url_to_resource_handle(&specs.url)?;

    let resource_specs = url.get_specs().await?;
    debug!("File to download has specs: {:?}", resource_specs);

    let output_filename = match specs.output {
        Some(filename) => filename,
        None => {
            if let Some(filename) = resource_specs.inferred_filename {
                filename
            } else {
                "download".into() // TODO: come up with a better default way
            }
        }
    };
    debug!("Output filename: {}", output_filename);

    let file_size = resource_specs.size;
    let chunk_count = match (file_size, resource_specs.supports_splits) {
        (Some(size), true) => Some((size as f32 / CHUNK_SIZE as f32).ceil() as u32),
        _ => None,
    };
    debug!("File size: {:?}, chunk count: {chunk_count:?}", file_size);
    let chunk_bounds = match chunk_count {
        Some(count) => (0..count)
            .map(|i| {
                let start = i * CHUNK_SIZE;
                let end = cmp::min(start + CHUNK_SIZE, file_size.unwrap_or(0));
                Some(ChunkRange { start, end })
            })
            .collect::<Vec<_>>(),
        None => vec![None],
    };

    let download_worker_count = if resource_specs.supports_splits {
        specs.preferred_splits
    } else {
        1
    };
    debug!("downloading with {download_worker_count} workers");

    let (s_chunks, r_update) = mpsc::channel::<DownloadUpdate>(download_worker_count as usize);
    let (s_processing_q, r_processing_q) = async_channel::unbounded::<ChunkSpec>();
    let (s_progress, r_progress) = mpsc::channel::<u64>(download_worker_count as usize);

    let mut handles = vec![];

    // writer/processor
    let output_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(output_filename)
        .await?;

    // TODO: we'd have to leave this untouched if we're resuming a download
    output_file.set_len(file_size.unwrap_or(0) as u64).await?;
    let writer = Writer {
        output_file,
        chunk_count,
        r_chunks: r_update,
        s_processing_q: s_processing_q.clone(),
    };
    handles.push(writer.spawn());

    // download workers
    let download_worker = DownloadWorker {
        r_processing_q: r_processing_q.clone(),
        s_chunks: s_chunks.clone(),
        s_progress: s_progress.clone(),
        single_chunk_dl: matches!(chunk_count, Some(1) | None),
    };
    for _ in 0..download_worker_count {
        handles.push(download_worker.spawn());
    }

    spawn_progress_reporter(file_size, r_progress, multi);

    // Send splits to download queue
    future::join_all(
        chunk_bounds
            .iter()
            .map(|bound| s_processing_q.send((url.clone(), *bound)))
            .collect::<Vec<_>>(),
    )
    .await;

    futures::future::join_all(handles).await;
    debug!("exiting download function");
    Ok(())
}

#[derive(Clone, Debug)]
struct DownloadWorker {
    r_processing_q: async_channel::Receiver<ChunkSpec>,
    s_chunks: mpsc::Sender<DownloadUpdate>,
    s_progress: mpsc::Sender<ByteCount>,
    single_chunk_dl: bool,
}

impl DownloadWorker {
    fn spawn(&self) -> JoinHandle<()> {
        let t = self.clone();
        tokio::spawn(async move {
            while let Ok((handle, bounds)) = t.r_processing_q.recv().await {
                let r = handle
                    .stream_range(bounds, t.s_progress.clone())
                    .await
                    .try_for_each(|chunk| async {
                        debug!("sending chunk for bounds {bounds:?} to writer");
                        t.s_chunks
                            .send(DownloadUpdate::Chunk(Ok((chunk, bounds))))
                            .await
                            .expect("failed to send file chunk to writer");
                        Ok(())
                    })
                    .await;
                if let Err(e) = r {
                    t.s_chunks
                        .send(DownloadUpdate::Chunk(Err((e, (handle, bounds)))))
                        .await
                        .expect("failed to send error to writer");
                }

                if t.single_chunk_dl {
                    t.s_chunks.send(DownloadUpdate::Complete).await.ok();
                }
            }
        })
    }
}

macro_rules! windup_writer {
    ($file:ident, $s_processing:ident, $r_chunks:ident) => {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        $file.shutdown().await.expect("shutdown failed");
        $s_processing.close();
        $r_chunks.close();
        break;
    };
}

#[derive(Debug, Clone, Copy)]
enum ChunkState {
    Done,
    Pending,
}

struct Writer {
    output_file: File,
    chunk_count: Option<u32>,
    r_chunks: mpsc::Receiver<DownloadUpdate>,
    s_processing_q: async_channel::Sender<ChunkSpec>,
}

impl Writer {
    fn spawn(self) -> JoinHandle<()> {
        let chunk_count = self.chunk_count;
        let s_processing_q = self.s_processing_q;
        let mut r_chunks = self.r_chunks;
        let mut output_file = self.output_file;
        let mut chunk_states = chunk_count.map(|count| vec![ChunkState::Pending; count as usize]);

        tokio::spawn(async move {
            let mut chunks_received = 0;
            while let Some(chunk) = r_chunks.recv().await {
                match chunk {
                    DownloadUpdate::Chunk(Ok(chunk)) => {
                        write_file_chunk(&mut output_file, &chunk)
                            .await
                            .expect("write failed");
                        let (_, chunk_range) = chunk;
                        let chunk_index = if let Some(range) = chunk_range {
                            range.start / CHUNK_SIZE
                        } else {
                            0
                        } as usize;
                        if let Some(ref mut states) = chunk_states {
                            states[chunk_index] = ChunkState::Done;
                        }

                        chunks_received += 1;
                        let we_are_done = match chunk_count {
                            Some(count) => chunks_received == count,
                            None => false,
                        };
                        if we_are_done {
                            debug!("download complete (all chunks received)");
                            windup_writer!(output_file, s_processing_q, r_chunks);
                        }
                    }

                    DownloadUpdate::Chunk(Err((e, chunk_spec))) => {
                        error!("Error downloading chunk: {}. Retrying...", e);
                        s_processing_q
                            .clone()
                            .send(chunk_spec)
                            .await
                            .expect("send failed");
                    }

                    DownloadUpdate::Complete => {
                        debug!("download complete (completion signal received)");
                        windup_writer!(output_file, s_processing_q, r_chunks);
                    }
                }
            }
        })
    }
}

#[derive(Error, Debug)]
enum WriteChunkError {}
async fn write_file_chunk(file: &mut File, chunk: &FileChunk) -> Result<(), WriteChunkError> {
    if let Some(range) = &chunk.1 {
        file.seek(SeekFrom::Start(range.start as u64))
            .await
            .expect("seek failed");
    }
    file.write_all(chunk.0.as_ref())
        .await
        .expect("write failed");
    file.flush().await.expect("flush failed");
    Ok(())
}
