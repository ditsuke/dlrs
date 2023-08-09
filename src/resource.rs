use std::error::Error;
use std::time::Duration;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};

use futures::{Stream, StreamExt};

use thiserror::Error;

use tokio::sync::mpsc;
use url::Url;

use crate::http_utils;
use crate::shared_types::{ByteCount, ChunkRange};

#[derive(Error, Debug)]
pub(crate) enum ReadError {
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),
    #[error("FTP error: {0}")]
    _Ftp(String),
}

#[derive(Error, Debug)]
pub(crate) enum ResourceError {
    #[error("Resource read error: {0}")]
    ReadError(ReadError),
    #[error("Handle does not support partial content")]
    NoPartialContentSupport,
}

impl From<http_utils::GetError> for ResourceError {
    fn from(e: http_utils::GetError) -> Self {
        match e {
            http_utils::GetError::Http(e) => ResourceError::ReadError(ReadError::Http(e)),
            http_utils::GetError::NoPartialContentSupportWhenceRequested => {
                ResourceError::NoPartialContentSupport
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ResourceHandle {
    Ftp(Url),
    Http { url: Url, client: reqwest::Client },
}

#[derive(Error, Debug)]
pub(crate) enum HandleCreationError {
    #[error("Unsupported protocol: {protocol}")]
    UnsupportedProtocol { protocol: String },
    #[error("Http client error: {0}")]
    Http(#[from] reqwest::Error),
}

impl TryFrom<&Url> for ResourceHandle {
    type Error = HandleCreationError;
    fn try_from(url: &Url) -> Result<Self, Self::Error> {
        match url.scheme() {
            "http" | "https" => Ok(ResourceHandle::Http {
                url: url.to_owned(),
                client: reqwest::Client::builder()
                    .tcp_keepalive(Some(Duration::from_secs(2)))
                    .build()?,
            }),
            "ftp" => Ok(ResourceHandle::Ftp(url.to_owned())),
            scheme => Err(HandleCreationError::UnsupportedProtocol {
                protocol: scheme.into(),
            }),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ResourceSpec {
    pub(crate) url: Url,
    pub(crate) size: Option<u32>,
    pub(crate) supports_splits: bool,
    pub(crate) inferred_filename: Option<String>,
}

impl ResourceHandle {
    pub(crate) async fn get_specs(&self) -> Result<ResourceSpec, Box<dyn Error>> {
        match self {
            ResourceHandle::Ftp(_url) => {
                todo!()
            }
            ResourceHandle::Http { client, url } => {
                let (headers, url) = http_utils::get_headers_follow_redirects(client, url).await?;
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

                let inferred_filename = http_utils::get_file_name_from_headers(&headers)
                    .unwrap_or_else(|| url.path_segments().unwrap().last().unwrap().to_owned());
                let inferred_filename = if inferred_filename.is_empty() {
                    None
                } else {
                    Some(inferred_filename)
                };

                Ok(ResourceSpec {
                    url: url.clone(),
                    size,
                    supports_splits,
                    inferred_filename,
                })
            }
        }
    }

    pub(crate) async fn stream_range(
        &self,
        range: Option<ChunkRange>,
        s_progress: mpsc::Sender<ByteCount>,
    ) -> impl Stream<Item = Result<Bytes, ResourceError>> {
        match self {
            ResourceHandle::Http { client, url } => {
                let (client, url) = (client.clone(), url.clone());
                try_stream! {
                    let mut stream = http_utils::get_stream(&client, &url, range).await?;

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
