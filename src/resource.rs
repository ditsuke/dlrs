use anyhow::Context;

use async_stream::try_stream;
use bytes::{Bytes, BytesMut};

use futures::{Stream, StreamExt};

use thiserror::Error;

use tokio::sync::mpsc;
use url::Url;

use crate::shared_types::{ByteCount, ChunkRange};
use crate::http;

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
    #[error("Stalled: no data received for {0:?}")]
    Stall(std::time::Duration),
}

impl From<http::GetError> for ResourceError {
    fn from(e: http::GetError) -> Self {
        match e {
            http::GetError::Http(e) => ResourceError::ReadError(ReadError::Http(e)),
            http::GetError::NoPartialContentSupportWhenceRequested => {
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
                client: http::build_http_client()?,
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
    pub(crate) size: Option<u64>,
    pub(crate) supports_splits: bool,
    pub(crate) inferred_filename: Option<String>,
    pub(crate) etag: Option<String>,
    pub(crate) last_modified: Option<String>,
}

impl ResourceHandle {
    pub(crate) async fn get_specs(&self) -> anyhow::Result<ResourceSpec> {
        match self {
            ResourceHandle::Ftp(_url) => {
                todo!()
            }
            ResourceHandle::Http { client, url } => {
                let probe = http::probe::probe_resource(client, url)
                    .await
                    .with_context(|| format!("failed to probe {url}"))?;
                debug!("probe headers: {:?}", probe.headers);

                let supports_splits = probe.supports_splits;
                if !supports_splits {
                    debug!("server at {} does not support range requests", probe.url);
                }

                let size = probe.size;
                if size.is_none() {
                    warn!("could not determine file size from probe response");
                }

                let etag = probe
                    .headers
                    .get("ETag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_owned());
                let last_modified = probe
                    .headers
                    .get("Last-Modified")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_owned());

                let inferred_filename = http::get_file_name_from_headers(&probe.headers)
                    .or_else(|| probe.url.path_segments()?.next_back().map(str::to_owned))
                    .filter(|s| !s.is_empty());

                Ok(ResourceSpec {
                    url: probe.url,
                    size,
                    supports_splits,
                    inferred_filename,
                    etag,
                    last_modified,
                })
            }
        }
    }
}

/// Streams a single byte range from `url`, buffering network sub-chunks into
/// full chunk-sized [`Bytes`] before yielding them to the caller.
pub(crate) async fn stream_range(
    client: &reqwest::Client,
    url: &Url,
    range: Option<ChunkRange>,
    s_progress: mpsc::Sender<ByteCount>,
) -> impl Stream<Item = Result<Bytes, ResourceError>> {
    let (client, url) = (client.clone(), url.clone());
    try_stream! {
        let mut stream = http::get_stream(&client, &url, range).await?;

        let buffer_capacity = range.map_or(1024, |r| (r.end - r.start + 1) as usize);
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
