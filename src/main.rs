#[macro_use]
extern crate log;
extern crate simplelog;

mod downloader;
mod http_utils;
mod resource;
mod shared_types;

use std::error::Error;

use clap::Parser;
use downloader::{start_download, DownloadPreferences, DownloadProgress};

use tokio::sync::mpsc;
use url::Url;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    #[arg(short, long, default_value = "1")]
    splits: u8,

    #[arg(index = 1)]
    url: String,

    #[arg(short, long, default_value = None)]
    output: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    simplelog::TermLogger::init(
        simplelog::LevelFilter::Debug,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    debug!("Starting up");

    let args = CliArgs::parse();

    let url = Url::parse(args.url.as_ref())?;
    let preferred_splits = args.splits;

    let (tx_progress, _rx_progress) = mpsc::channel::<DownloadProgress>(preferred_splits as usize);

    let dl_specs = DownloadPreferences {
        url,
        preferred_splits,
        output: args.output,
    };
    start_download(dl_specs, tx_progress).await?;

    Ok(())
}
