use std::error::Error;

use clap::Parser;
use futures::future;
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
};
use url::Url;

use crate::{http_utils, ChunkRange};

#[derive(Error, Debug)]
pub(crate) enum ResourceReadError {
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),
    #[error("Handle does not support partial content")]
    NoPartialContentSupport,
    #[error("FTP error: {0}")]
    Ftp(String),
}

#[derive(Debug, Clone)]
pub(crate) enum ResourceHandle {
    Ftp(Url),
    Http(Url),
}

#[derive(Debug)]
pub(crate) struct ResourceSpec {
    pub(crate) url: Url,
    pub(crate) size: Option<u32>,
    pub(crate) supports_splits: bool,
}

impl ResourceHandle {
    pub(crate) async fn get_specs(&self) -> Result<ResourceSpec, Box<dyn Error>> {
        match self {
            ResourceHandle::Ftp(_url) => {
                todo!()
            }
            ResourceHandle::Http(url) => {
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
            ResourceHandle::Ftp(_url) => {
                todo!()
            }
            ResourceHandle::Http(url) => {
                let headers = http_utils::get_headers_follow_redirects(url).await?;
                Ok(http_utils::get_file_name_from_headers(&headers)
                    .unwrap_or_else(|| url.path_segments().unwrap().last().unwrap().to_owned()))
            }
        }
    }

    pub(crate) async fn read_range(
        &self,
        range: Option<ChunkRange>,
    ) -> Result<Vec<u8>, ResourceReadError> {
        match self {
            ResourceHandle::Ftp(_url) => {
                todo!()
            }
            ResourceHandle::Http(url) => Ok(http_utils::get(url, range)
                .await
                .map_err(ResourceReadError::Http))?,
        }
    }
}
