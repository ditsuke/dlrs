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
use crate::shared_types::{ChunkRange};

const MB_TO_BYTES: u32 = 1024 * 1024;
const CHUNK_SIZE: u32 = 10 * MB_TO_BYTES;

type ChunkBoundaries = Option<ChunkRange>;
type FileChunk = (Bytes, ChunkBoundaries);
type ChunkSpec = (DownloadUrl, ChunkBoundaries);
type ChunkResult = Result<FileChunk, (ResourceGetError, ChunkSpec)>;

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
        Some(size) => {
            if resource_specs.supports_splits {
                size / CHUNK_SIZE + 1
            } else {
                1
            }
        }
        None => 1,
    };
    debug!("Chunk count: {}", chunk_count);

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

    let chunk_bounds = (0..chunk_count)
        .map(|i| {
            let start = i * CHUNK_SIZE;
            let end = cmp::min(start + CHUNK_SIZE, file_size.unwrap_or(0));
            ChunkRange { start, end }
        })
        .collect::<Vec<_>>();

    let splits = if resource_specs.supports_splits {
        specs.preferred_splits
    } else {
        1
    };

    let (tx_chunks, rx_chunks) = mpsc::channel::<ChunkResult>(splits as usize);
    let (tx_processing_q, rx_processing_q) = async_channel::unbounded::<ChunkSpec>();
    let (s_progress, r_progress) = mpsc::channel::<u32>(splits as usize);

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
        rx_chunks,
        tx_processing_q.clone(),
    ));

    // download workers
    for _ in 0..splits {
        let rx_processing_q = rx_processing_q.clone();
        let tx_chunks = tx_chunks.clone();
        handles.push(spawn_download_worker(
            rx_processing_q,
            tx_chunks,
            s_progress.clone(),
        ));
    }

    spawn_progress_reporter(file_size, r_progress);

    // Send splits to download queue
    future::join_all(
        chunk_bounds
            .iter()
            .map(|bound| tx_processing_q.send((url.clone(), Some(*bound))))
            .collect::<Vec<_>>(),
    )
    .await;

    futures::future::join_all(handles).await;
    Ok(())
}

fn spawn_download_worker(
    r_processing_q: async_channel::Receiver<ChunkSpec>,
    s_chunks: mpsc::Sender<ChunkResult>,
    s_progress: mpsc::Sender<u32>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((handle, bounds)) = r_processing_q.recv().await {
            handle
                .stream_range(bounds, s_progress.clone())
                .await
                .try_for_each(|chunk| async {
                    s_chunks
                        .send(Ok((chunk, bounds)))
                        .await
                        .expect("send failed");
                    Ok(())
                })
                .await
                .ok();
        }
        debug!("stopping download worker");
    })
}

#[derive(Debug, Clone, Copy)]
enum ChunkState {
    Done,
    Pending,
}

fn spawn_writer(
    mut output_file: File,
    chunk_count: u32,
    mut r_chunks: mpsc::Receiver<ChunkResult>,
    s_processing_q: async_channel::Sender<ChunkSpec>,
) -> JoinHandle<()> {
    let mut chunk_states = vec![ChunkState::Pending; chunk_count as usize];

    tokio::spawn(async move {
        let mut chunks_received = 0;
        while let Some(chunk) = r_chunks.recv().await {
            match chunk {
                Ok(chunk) => {
                    write_file_chunk(&mut output_file, &chunk)
                        .await
                        .expect("write failed");
                    let (_, chunk_range) = chunk;
                    let chunk_index = if let Some(range) = chunk_range {
                        range.start / CHUNK_SIZE
                    } else {
                        0
                    } as usize;
                    chunk_states[chunk_index] = ChunkState::Done;
                    chunks_received += 1;
                    debug!("chunk received");
                    if chunks_received == chunk_count {
                        output_file.shutdown().await.expect("shutdown failed");
                        s_processing_q.close();
                        r_chunks.close();
                        break;
                    }
                }
                Err((e, chunk_spec)) => {
                    error!("Error downloading chunk: {}", e);
                    s_processing_q
                        .clone()
                        .send(chunk_spec)
                        .await
                        .expect("send failed");
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