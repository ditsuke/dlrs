use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use reqwest::{header::HeaderMap, Client};
use thiserror::Error;
use url::Url;

use crate::shared_types::ChunkRange;

/// Builds a configured HTTP client. Each download worker gets its own instance
/// so connections are never shared or multiplexed across workers.
pub(crate) fn build_http_client() -> Result<Client, reqwest::Error> {
    Client::builder()
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .build()
}

#[derive(Error, Debug)]
pub(crate) enum GetError {
    #[error("Failed to GET resource: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Resource handle does not support partial content (RANGE)")]
    NoPartialContentSupportWhenceRequested,
}

pub(crate) async fn get_stream(
    client: &Client,
    url: &Url,
    range: Option<ChunkRange>,
) -> Result<impl Stream<Item = Result<Bytes, GetError>>, GetError> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(range) = range {
        headers.insert(
            reqwest::header::RANGE,
            format!("bytes={}-{}", range.start, range.end)
                .parse()
                .unwrap(),
        );
    }

    let response = client
        .get(url.as_str())
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;

    if range.is_some() && response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(GetError::NoPartialContentSupportWhenceRequested);
    }

    Ok(response.bytes_stream().map_err(GetError::from))
}

pub(crate) fn get_file_name_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("Content-Disposition")?
        .to_str()
        .ok()?
        .split(';')
        .map(|s| s.trim())
        .find(|s| s.starts_with("filename="))
        .and_then(|s| s.strip_prefix("filename=").map(str::to_owned))
}
