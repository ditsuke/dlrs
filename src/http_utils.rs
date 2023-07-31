use std::error::Error;

use reqwest::header::HeaderMap;
use url::Url;

use crate::shared_types::ChunkRange;

#[async_recursion::async_recursion]
pub(crate) async fn get_headers_follow_redirects(url: &Url) -> Result<HeaderMap, Box<dyn Error>> {
    let http_client = reqwest::Client::new();
    let headers = http_client
        .head(url.as_str())
        .send()
        .await?
        .error_for_status()?
        .headers()
        .to_owned();
    if headers.get("Location").is_some() {
        let new_url = headers.get("Location").unwrap().to_str().unwrap();
        let new_url = Url::parse(new_url)?;
        get_headers_follow_redirects(&new_url).await?;
    }
    Ok(headers)
}

pub(crate) async fn get(url: &Url, range: Option<ChunkRange>) -> Result<Vec<u8>, reqwest::Error> {
    let http_client = reqwest::Client::new();
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(range) = range {
        headers.insert(
            reqwest::header::RANGE,
            format!("bytes={}-{}", range.start, range.end)
                .parse()
                .unwrap(),
        );
    }

    let response = http_client
        .get(url.to_owned())
        .headers(headers)
        .send()
        .await?
        .error_for_status()?;

    Ok(response.bytes().await?.to_vec())
}

pub(crate) fn get_file_name_from_headers(headers: &HeaderMap) -> Option<String> {
    println!("headers are {headers:?}");
    headers.get("Content-Disposition").map(|v| {
        println!("v is {v:?}");
        v.to_str()
            .unwrap()
            .split(';')
            .map(|s| s.trim())
            .find(|s| s.starts_with("filename="))
            .map(|s| s.replace("filename=", ""))
            .unwrap()
    })
    // .or_else(|| {
    //     headers
    //         .get("Content-Type")
    //         .map(|v| {
    //             v.to_str()
    //                 .unwrap()
    //                 .split(';')
    //                 .map(|s| s.trim())
    //                 .find(|s| s.starts_with("filename="))
    //                 .map(|s| s.replace("filename=", ""))
    //         })
    //         .or_else(|| {
    //             headers
    //                 .get("Location")
    //                 .map(|v| v.to_str().unwrap().to_owned())
    //         })
    // })
}
