use std::error::Error;

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use reqwest::{header::HeaderMap, Client};
use thiserror::Error;
use url::Url;

use crate::shared_types::ChunkRange;

#[async_recursion::async_recursion]
pub(crate) async fn get_headers_follow_redirects(
    client: &Client,
    url: &Url,
) -> Result<(HeaderMap, Url), Box<dyn Error>> {
    let headers = client
        .head(url.as_str())
        .send()
        .await?
        .error_for_status()?
        .headers()
        .to_owned();
    if headers.get("Location").is_some() {
        debug!("Redirecting to {:?}", headers.get("Location").unwrap());
        let new_url = headers.get("Location").unwrap().to_str().unwrap();
        let new_url = Url::parse(new_url)?;
        get_headers_follow_redirects(client, &new_url).await?;
    }
    Ok((headers, url.clone()))
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
        .get(url.to_owned())
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
    debug!("headers are {headers:?}");
    headers
        .get("Content-Disposition")?
        .to_str()
        .ok()?
        .split(';')
        .map(|s| s.trim())
        .find(|s| s.starts_with("filename="))
        .map(|s| s.replace("filename=", ""))
}
