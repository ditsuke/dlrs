use std::error::Error;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};

use futures::{Stream, StreamExt};

use thiserror::Error;

use tokio::sync::mpsc;
use url::Url;

use crate::http_utils::{self, GetError};
use crate::shared_types::{ByteCount, ChunkRange};

pub(crate) fn url_to_resource_handle(url: &Url) -> Result<DownloadUrl, Box<dyn Error>> {
    match url.scheme() {
        "http" | "https" => Ok(DownloadUrl::Http(url.to_owned())),
        "ftp" => Ok(DownloadUrl::Ftp(url.to_owned())),
        _ => Err(format!("Unsupported scheme: {}", url.scheme()).into()),
    }
}

#[derive(Error, Debug)]
pub(crate) enum ResourceReadError {
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),
    #[error("FTP error: {0}")]
    _Ftp(String),
}

#[derive(Error, Debug)]
pub(crate) enum ResourceGetError {
    #[error("Resource read error: {0}")]
    ReadError(ResourceReadError),
    #[error("Handle does not support partial content")]
    NoPartialContentSupport,
}

#[derive(Debug, Clone)]
pub(crate) enum DownloadUrl {
    Ftp(Url),
    Http(Url),
}

#[derive(Debug)]
pub(crate) struct ResourceSpec {
    pub(crate) url: Url,
    pub(crate) size: Option<u32>,
    pub(crate) supports_splits: bool,
}

impl DownloadUrl {
    pub(crate) async fn get_specs(&self) -> Result<ResourceSpec, Box<dyn Error>> {
        match self {
            DownloadUrl::Ftp(_url) => {
                todo!()
            }
            DownloadUrl::Http(url) => {
                let headers = http_utils::get_headers_follow_redirects(url).await?;
                debug!("headers: {:?}", headers);

                let supports_splits = headers
                    .get("Accept-Ranges")
                    .map(|v| {
                        v.to_str()
                            .map_err(|e| {
                                format!("failed to map Accept-Ranges from {url} to a string: {e}")
                            })
                            .unwrap()
                            == "bytes"
                    })
                    .unwrap_or(false);

                let mut size = headers
                    .get("Content-Length")
                    .map(|v| v.to_str().unwrap().parse::<u32>().unwrap());
                if size == Some(0) {
                    size = None;
                }

                Ok(ResourceSpec {
                    url: url.clone(),
                    size,
                    supports_splits,
                })
            }
        }
    }

    pub(crate) async fn infer_filename(&self) -> Result<String, Box<dyn Error>> {
        match self {
            DownloadUrl::Ftp(_url) => {
                todo!()
            }
            DownloadUrl::Http(url) => {
                let headers = http_utils::get_headers_follow_redirects(url).await?;
                Ok(http_utils::get_file_name_from_headers(&headers)
                    .unwrap_or_else(|| url.path_segments().unwrap().last().unwrap().to_owned()))
            }
        }
    }

    pub(crate) async fn stream_range(
        &self,
        range: Option<ChunkRange>,
        s_progress: mpsc::Sender<ByteCount>,
    ) -> impl Stream<Item = Result<Bytes, GetError>> {
        let u = self.clone();
        match u {
            DownloadUrl::Http(url) => {
                try_stream! {
                    let mut stream = http_utils::get_stream(&url, range).await?;

                    // HACK: this feels so wrong on so many levels.
                    // While I can't exactly figure out a "good" way to do this,
                    // The solution should be verification of chunks and whether
                    // they're done or not on the end of the writer.
                    // OKAY, idea: the writer maintains an array of of chunks,
                    // keeping track Chunk::ToBeWritten, along with the start
                    // and end of the chunk. When a chunk is written, it's
                    // marked as Chunk::Written only if the entire chunk was
                    // written. If the chunk was only partially written, it's
                    // Chunk::ToBeWritten bytecount is updated to reflect the
                    // remaining bytes to be written. Once the chunk is
                    // completely written we can update the chunks_written
                    // counter and move on to the next chunk, thus allowing
                    // it to figure out the termination condition on its own too.
                    let buffer_capacity = if let Some(range) = range {
                        (range.end - range.start + 1) as usize
                    } else {
                        1000 // TODO: WTF``
                    };
                    let mut buffer = BytesMut::with_capacity(buffer_capacity);
                    let mut cumulative = 0;

                    while let Some(sub_chunk) = stream.next().await {
                        let sub_chunk = sub_chunk?;
                        cumulative += sub_chunk.len();
                        s_progress.try_send(sub_chunk.len() as ByteCount).ok();
                        buffer.extend_from_slice(&sub_chunk);
                        if cumulative >= buffer_capacity {
                            yield buffer.freeze();
                            cumulative = 0;
                            buffer = BytesMut::with_capacity(buffer_capacity);
                        }
                    }

                    if !buffer.is_empty() {
                        yield buffer.freeze();
                    }
                }
            }
            _ => todo!(),
        }
    }
}
