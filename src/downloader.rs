use std::cmp;
use std::error::Error;
use std::io::SeekFrom;

use bytes::Bytes;
use futures::{future, TryStreamExt};

use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};

use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::{sync::mpsc, task::JoinHandle};
use url::Url;

use crate::resource::{self, ResourceGetError};

use crate::resource::DownloadUrl;
use crate::shared_types::ChunkRange;

const MB_TO_BYTES: u32 = 1024 * 1024;
const CHUNK_SIZE: u32 = 2 * MB_TO_BYTES;

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
            if let Ok(filename) = url.infer_filename().await {
                filename
            } else {
                "download".into() // TODO: come up with a better default way
            }
        }
    };

    let file_size = resource_specs.size;
    let chunk_count = match (file_size, resource_specs.supports_splits) {
        (Some(size), true) => Some((size as f32 / CHUNK_SIZE as f32).ceil() as u32),
        _ => None,
    };
    debug!("File size: {:?}", file_size);
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

fn spawn_download_worker(
    r_processing_q: async_channel::Receiver<ChunkSpec>,
    s_chunks: mpsc::Sender<DownloadUpdate>,
    s_progress: mpsc::Sender<u32>,
    single_chunk: bool,
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
                .unwrap();
            if single_chunk {
                // debug!("sending completion signal");

                s_chunks.send(DownloadUpdate::Complete).await.ok();
            }
        }
        // debug!("stopping download worker");
    })
}

#[derive(Debug, Clone, Copy)]
enum ChunkState {
    Done,
    Pending,
}

macro_rules! windup {
    ($file:ident, $s_processing:ident, $r_chunks:ident) => {
        // debug!("winding up: because we think we're done with things!");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        $file.shutdown().await.expect("shutdown failed");
        $s_processing.close();
        $r_chunks.close();
        break;
    };
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
                    let we_are_done = match chunk_count {
                        Some(count) => chunks_received == count,
                        None => false,
                    };
                    if we_are_done {
                        debug!("download complete (all chunks received)");
                        windup!(output_file, s_processing_q, r_chunks);
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
                    windup!(output_file, s_processing_q, r_chunks);
                }
            }
        }
    })
}

fn spawn_progress_reporter(
    total_size: Option<u32>,
    mut rx_progress: mpsc::Receiver<u32>,
    multi: MultiProgress,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut progress = 0;

        let pb = total_size.map_or_else(ProgressBar::new_spinner, |size| {
            ProgressBar::new(size as u64)
        });
        let pb = multi.add(pb);
        pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));

        while let Some(chunk_size) = rx_progress.recv().await {
            progress += chunk_size;
            pb.set_position(progress as u64);
        }
        pb.finish_with_message("DLD");
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
    file.flush().await.expect("flush failed");
    Ok(())
}
