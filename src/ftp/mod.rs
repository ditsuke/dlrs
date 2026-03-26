use std::pin::Pin;

use bytes::Bytes;
use futures::io::AsyncReadExt;
use futures::Stream;
use suppaftp::{AsyncFtpStream, FtpError as SuppaFtpError};
use thiserror::Error;
use url::Url;

use crate::shared_types::ChunkRange;

#[derive(Error, Debug)]
pub(crate) enum FtpError {
    #[error("FTP error: {0}")]
    Ftp(#[from] SuppaFtpError),
    #[error("FTP: missing host in URL")]
    MissingHost,
    #[error("FTP I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub(crate) struct FtpProbe {
    pub(crate) url: Url,
    pub(crate) size: Option<u64>,
    pub(crate) supports_rest: bool,
    pub(crate) mdtm: Option<String>,
}

/// Extract the file path from an FTP URL (the URL path without the leading slash).
fn ftp_path(url: &Url) -> &str {
    let p = url.path();
    p.strip_prefix('/').unwrap_or(p)
}

/// Connect and login to the FTP server described by `url`.
async fn connect(url: &Url) -> Result<AsyncFtpStream, FtpError> {
    let host = url.host_str().ok_or(FtpError::MissingHost)?;
    let port = url.port().unwrap_or(21);

    let mut ftp = AsyncFtpStream::connect(format!("{host}:{port}")).await?;

    let user = if url.username().is_empty() {
        "anonymous"
    } else {
        url.username()
    };
    let pass = url.password().unwrap_or("anonymous@");

    ftp.login(user, pass).await?;
    ftp.transfer_type(suppaftp::types::FileType::Binary).await?;

    Ok(ftp)
}

/// Probe an FTP resource for size, REST support, and modification time.
pub(crate) async fn probe_resource(url: &Url) -> Result<FtpProbe, FtpError> {
    let mut ftp = connect(url).await?;
    let path = ftp_path(url);

    let size = ftp.size(path).await.ok().map(|s| s as u64);

    // Test REST support by issuing REST 0.
    let supports_rest = if size.is_some() {
        ftp.resume_transfer(0).await.is_ok()
    } else {
        false
    };

    let mdtm = ftp.mdtm(path).await.ok().map(|dt| dt.to_string());

    ftp.quit().await.ok();

    Ok(FtpProbe {
        url: url.clone(),
        size,
        supports_rest,
        mdtm,
    })
}

/// Open a byte stream for `url`, optionally starting from an offset.
///
/// Each call opens its own FTP session so parallel workers are independent.
/// FTP has no end-offset in REST/RETR, so we byte-count to `end - start + 1`
/// and then drop the connection.
pub(crate) async fn get_stream(
    url: &Url,
    range: Option<ChunkRange>,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, FtpError>> + Send>>, FtpError> {
    let mut ftp = connect(url).await?;
    let path = ftp_path(url).to_owned();

    if let Some(ref r) = range {
        ftp.resume_transfer(r.start as usize).await?;
    }

    let data_stream = ftp.retr_as_stream(path).await?;
    let max_bytes: Option<u64> = range.map(|r| r.end - r.start + 1);

    Ok(Box::pin(async_stream::try_stream! {
        let mut reader = data_stream;
        let mut remaining = max_bytes.unwrap_or(u64::MAX);
        let mut buf = vec![0u8; 64 * 1024];

        while remaining > 0 {
            let to_read = (remaining as usize).min(buf.len());
            let n = reader.read(&mut buf[..to_read]).await.map_err(FtpError::Io)?;
            if n == 0 {
                break;
            }
            remaining -= n as u64;
            yield Bytes::copy_from_slice(&buf[..n]);
        }

        // Finalize the RETR transfer on the control connection.
        ftp.finalize_retr_stream(reader).await?;
        ftp.quit().await.ok();
    }))
}
