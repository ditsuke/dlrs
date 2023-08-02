use std::error::Error;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use thiserror::Error;

use tokio::sync::mpsc;
use url::Url;

use crate::http_utils::{self, GetError};
use crate::shared_types::ChunkRange;

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

                let size = headers
                    .get("Content-Length")
                    .map(|v| v.to_str().unwrap().parse::<u32>().unwrap());

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
        s_progress: mpsc::Sender<u32>,
    ) -> impl Stream<Item = Result<Bytes, GetError>> {
        let u = self.clone();
        match u {
            DownloadUrl::Http(url) => {
                try_stream! {
                    // let mut stream = self.read_range(range, s_progress).await;
                    let mut stream = http_utils::get_stream(&url, range).await?;
                    let buffer_cap = if let Some(range) = range {
                        (range.end - range.start) as usize
                    } else {
                        1000
                    };
                    let mut buffer = BytesMut::with_capacity(buffer_cap);
                    let mut cum = 0;
                    while let Some(v) = stream.next().await {
                        let v = v?;
                        cum += v.len();
                        s_progress.try_send(v.len() as u32).ok();
                        buffer.extend_from_slice(&v);
                        if cum >= buffer_cap {
                            yield buffer.freeze();
                            cum = 0;
                            buffer = BytesMut::with_capacity(buffer_cap);
                        }
                    }
                }
            }
            _ => todo!(),
        }
    }
}
