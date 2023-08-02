use std::cmp;
use std::error::Error;
use std::io::SeekFrom;

use bytes::Bytes;
use futures::{future, TryStreamExt};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::{sync::mpsc, task::JoinHandle};
use url::Url;

use crate::resource::{self, ResourceGetError};

use crate::resource::DownloadUrl;
use crate::shared_types::ChunkRange;

const MB_TO_BYTES: u32 = 1024 * 1024;
const CHUNK_SIZE: u32 = 10 * MB_TO_BYTES;

type ChunkBoundaries = Option<ChunkRange>;
type FileChunk = (Bytes, ChunkBoundaries);
type ChunkSpec = (DownloadUrl, ChunkBoundaries);
enum DownloadUpdate {
    Chunk(Result<FileChunk, (ResourceGetError, ChunkSpec)>),
    Complete,
}

pub(crate) struct DownloadPreferences {
    pub(crate) url: Url,
    pub(crate) preferred_splits: u8,
    pub(crate) output: Option<String>,
}

pub(crate) struct DownloadProgress {
    done: u64,
    total: Option<u64>,
}

pub(crate) async fn start_download(
    specs: DownloadPreferences,
    // TODO: implement progress reporting back to the master
    _s_progress: mpsc::Sender<DownloadProgress>,
) -> Result<(), Box<dyn Error>> {
    let url = resource::url_to_resource_handle(&specs.url)?;

    let resource_specs = url.get_specs().await?;
    debug!("File to download has specs: {:?}", resource_specs);

    let file_size = resource_specs.size;
    let chunk_count = match file_size {
        Some(size) => Some(size / CHUNK_SIZE + 1),
        None => None,
    };
    debug!("Chunk count: {:?}", chunk_count);

    let output_filename = match specs.output {
        Some(filename) => filename,
        None => {
            if let Ok(filename) = url.infer_filename().await {
                filename
            } else {
                "download".into() // TODO: come up with a better default way
            }
        }
    };

    let chunk_bounds = match chunk_count {
        Some(count) => (0..count)
            .map(|i| {
                let start = i * CHUNK_SIZE;
                let end = cmp::min(start + CHUNK_SIZE, file_size.unwrap_or(0));
                ChunkRange { start, end }
            })
            .collect::<Vec<_>>(),
        None => vec![ChunkRange { start: 0, end: 0 }],
    };

    let download_worker_count = if resource_specs.supports_splits {
        specs.preferred_splits
    } else {
        1
    };

    let (s_update, r_update) = mpsc::channel::<DownloadUpdate>(download_worker_count as usize);
    let (s_processing_q, r_processing_q) = async_channel::unbounded::<ChunkSpec>();
    let (s_progress, r_progress) = mpsc::channel::<u32>(download_worker_count as usize);

    let mut handles = vec![];

    // writer/processor
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(output_filename)
        .await?;
    // TODO: we'd have to leave this untouched if we're resuming a download
    file.set_len(file_size.unwrap_or(0) as u64).await?;
    handles.push(spawn_writer(
        file,
        chunk_count,
        r_update,
        s_processing_q.clone(),
    ));

    // download workers
    for _ in 0..download_worker_count {
        let rx_processing_q = r_processing_q.clone();
        let tx_chunks = s_update.clone();
        handles.push(spawn_download_worker(
            rx_processing_q,
            tx_chunks,
            s_progress.clone(),
            download_worker_count == 1,
        ));
    }

    spawn_progress_reporter(file_size, r_progress);

    // Send splits to download queue
    future::join_all(
        chunk_bounds
            .iter()
            .map(|bound| s_processing_q.send((url.clone(), Some(*bound))))
            .collect::<Vec<_>>(),
    )
    .await;

    futures::future::join_all(handles).await;
    Ok(())
}

fn spawn_download_worker(
    r_processing_q: async_channel::Receiver<ChunkSpec>,
    s_chunks: mpsc::Sender<DownloadUpdate>,
    s_progress: mpsc::Sender<u32>,
    solo_worker: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((handle, bounds)) = r_processing_q.recv().await {
            handle
                .stream_range(bounds, s_progress.clone())
                .await
                .try_for_each(|chunk| async {
                    s_chunks
                        .send(DownloadUpdate::Chunk(Ok((chunk, bounds))))
                        .await
                        .expect("send failed");
                    Ok(())
                })
                .await
                .ok();
        }
        debug!("stopping download worker");
        if solo_worker {
            debug!("sending completion signal");
            s_chunks
                .send(DownloadUpdate::Complete)
                .await
                .expect("send failed");
        }
    })
}

#[derive(Debug, Clone, Copy)]
enum ChunkState {
    Done,
    Pending,
}

fn spawn_writer(
    mut output_file: File,
    chunk_count: Option<u32>,
    mut r_chunks: mpsc::Receiver<DownloadUpdate>,
    s_processing_q: async_channel::Sender<ChunkSpec>,
) -> JoinHandle<()> {
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
                    debug!("chunk received");
                    if let Some(count) = chunk_count {
                        if count == chunks_received {
                            output_file.shutdown().await.expect("shutdown failed");
                            s_processing_q.close();
                            r_chunks.close();
                            break;
                        }
                    }
                }

                DownloadUpdate::Chunk(Err((e, chunk_spec))) => {
                    error!("Error downloading chunk: {}", e);
                    s_processing_q
                        .clone()
                        .send(chunk_spec)
                        .await
                        .expect("send failed");
                }
                DownloadUpdate::Complete => {
                    debug!("download complete (completion signal received)");
                    break;
                }
            }
        }
    })
}

fn spawn_progress_reporter(
    total_size: Option<u32>,
    mut rx_progres: mpsc::Receiver<u32>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut progress = 0;
        while let Some(chunk_size) = rx_progres.recv().await {
            progress += chunk_size;
            info!(
                "Progress: {}/{}",
                progress,
                match total_size {
                    Some(size) => format!("{size}"),
                    None => "unknown".into(),
                }
            );
        }
    })
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
    Ok(())
}
