mod http_utils;
mod resource;

use std::{cmp, error::Error, io::SeekFrom};

use clap::Parser;
use futures::future;
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
    task::JoinHandle,
};
use url::Url;

use crate::resource::{ResourceHandle, ResourceReadError};

type FileChunk = (Vec<u8>, ChunkBoundaries);
type ChunkBoundaries = Option<ChunkRange>;
type ChunkSpec = (ResourceHandle, ChunkBoundaries);
type ChunkResult = Result<FileChunk, (ResourceReadError, ChunkSpec)>;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    #[arg(short, long, default_value = "1")]
    splits: u32,

    #[arg(short, long)]
    url: String,

    #[arg(short, long, default_value = None)]
    output: Option<String>,
}

#[derive(Clone, Copy)]
struct ChunkRange {
    start: u32,
    end: u32,
}

mod ftp_utils {}

const MB_TO_BYTES: u32 = 1024 * 1024;
const CHUNK_SIZE: u32 = 10 * MB_TO_BYTES;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simplelog::TermLogger::init(
        simplelog::LevelFilter::Info,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    let args = CliArgs::parse();

    let url = Url::parse(args.url.as_ref())?;
    let resource = match url.scheme() {
        "http" | "https" => ResourceHandle::Http(url),
        "ftp" | "ftps" => ResourceHandle::Ftp(url),
        _ => {
            return Err("Unsupported protocol".into());
        }
    };

    let resource_specs = resource.get_specs().await?;
    println!("File to download has specs: {:?}", resource_specs);

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

    let chunk_bounds = (0..chunk_count)
        .map(|i| {
            let start = i * CHUNK_SIZE;
            let end = cmp::min(start + CHUNK_SIZE, file_size.unwrap_or(0));
            ChunkRange { start, end }
        })
        .collect::<Vec<_>>();

    let (tx_chunks, rx_chunks) = mpsc::channel::<ChunkResult>(args.splits as usize);
    let (tx_processing_q, rx_processing_q) = async_channel::unbounded::<ChunkSpec>();
    future::join_all(
        chunk_bounds
            .iter()
            .map(|bound| tx_processing_q.send((resource.clone(), Some(*bound))))
            .collect::<Vec<_>>(),
    )
    .await;

    let output_filename = match args.output {
        Some(filename) => filename,
        None => {
            if let Ok(filename) = resource.infer_filename().await {
                filename
            } else {
                "download".into() // TODO: come up with a better default way
            }
        }
    };

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
    for _ in 0..args.splits {
        let rx_processing_q = rx_processing_q.clone();
        let tx_chunks = tx_chunks.clone();
        handles.push(spawn_download_worker(rx_processing_q, tx_chunks));
    }

    futures::future::join_all(handles).await;
    Ok(())
}

fn spawn_download_worker(
    r_processing_q: async_channel::Receiver<ChunkSpec>,
    s_chunks: mpsc::Sender<ChunkResult>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Ok((handle, bounds)) = r_processing_q.recv().await {
            match handle.read_range(bounds).await {
                Ok(chunk) => {
                    s_chunks
                        .send(Ok((chunk, bounds)))
                        .await
                        .expect("send failed");
                }
                Err(e) => {
                    s_chunks
                        .send(Err((e, (handle, bounds))))
                        .await
                        .expect("send failed");
                }
            }
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
                    println!("chunk received");
                    if chunks_received == chunk_count {
                        break;
                    }
                }
                Err((e, chunk_spec)) => {
                    // TODO: use a logger
                    eprintln!("Error downloading chunk: {}", e);
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
