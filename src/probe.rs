use anyhow::Context;
use reqwest::{header::HeaderMap, Client, StatusCode};
use url::Url;

pub(crate) struct ResourceProbe {
    pub(crate) url: Url,
    pub(crate) headers: HeaderMap,
    pub(crate) supports_splits: bool,
    pub(crate) size: Option<u64>,
}

/// Probes a URL to discover file size, range support, and response headers.
///
/// Tries HEAD first (no body at all). Falls back to `GET Range: bytes=0-0` if
/// HEAD fails (e.g. 405, connection error) — some servers do not handle HEAD correctly.
/// reqwest follows redirects automatically; the final URL is returned via
/// `ResourceProbe::url`.
pub(crate) async fn probe_resource(client: &Client, url: &Url) -> anyhow::Result<ResourceProbe> {
    // Try HEAD first.
    if let Ok(response) = client.head(url.as_str()).send().await {
        if let Ok(response) = response.error_for_status() {
            let url = response.url().clone();
            let headers = response.headers().to_owned();
            let supports_splits = headers
                .get("Accept-Ranges")
                .and_then(|v| v.to_str().ok())
                .map(|v| v.eq_ignore_ascii_case("bytes"))
                .unwrap_or(false);
            let size = content_length(&headers);
            return Ok(ResourceProbe {
                url,
                headers,
                supports_splits,
                size,
            });
        }
    }

    // Fall back to GET with a Range header.
    debug!("HEAD failed or rejected; falling back to GET Range: bytes=0-0");
    let response = client
        .get(url.as_str())
        .header(reqwest::header::RANGE, "bytes=0-0")
        .send()
        .await?
        .error_for_status()
        .context("Error probing resource")?;

    let url = response.url().clone();
    let supports_splits = response.status() == StatusCode::PARTIAL_CONTENT;
    let headers = response.headers().to_owned();
    // Drop without consuming — probe client is not shared with workers.
    let size = if supports_splits {
        // Content-Range: bytes 0-0/<total>
        headers
            .get("Content-Range")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split('/').next_back())
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&s| s > 0)
    } else {
        content_length(&headers)
    };

    Ok(ResourceProbe {
        url,
        headers,
        supports_splits,
        size,
    })
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("Content-Length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&s| s > 0)
}
