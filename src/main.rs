#[macro_use]
extern crate log;
extern crate simplelog;

mod downloader;
mod http_utils;
mod progress_reporter;
mod resource;
mod resume;
mod shared_types;

use std::error::Error;

use clap::Parser;
use downloader::{start_download, DownloadPreferences};

use indicatif::MultiProgress;
use indicatif_log_bridge::LogWrapper;
use simplelog::TermLogger;
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

    /// Overwrite an existing output file or discard a mismatched partial download
    /// and restart from scratch.
    #[arg(short, long, default_value_t = false)]
    force: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let multi_progress = MultiProgress::new();
    let logger = TermLogger::new(
        simplelog::LevelFilter::Info,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    );

    LogWrapper::new(multi_progress.clone(), logger)
        .try_init()
        .expect("failed to init global logger");

    let args = CliArgs::parse();

    let url = Url::parse(args.url.as_ref())?;
    let preferred_splits = args.splits;

    let preferences = DownloadPreferences {
        url,
        preferred_splits,
        output: args.output,
        force: args.force,
    };

    start_download(preferences, Some(multi_progress)).await?;

    debug!("Download complete");
    log::logger().flush();
    Ok(())
}
