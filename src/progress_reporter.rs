use std::sync::Arc;
use std::time::Duration;

use circular_buffer::CircularBuffer;

use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};

use tokio::sync::RwLock;
use tokio::time::Instant;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::shared_types::ByteCount;

pub(crate) struct ProgressReporter {
    rx_progress: mpsc::Receiver<ByteCount>,
    total_size: Option<u32>,
    multi_progress: MultiProgress,
}

impl ProgressReporter {
    pub(crate) fn new(
        rx_progress: mpsc::Receiver<ByteCount>,
        total_size: Option<u32>,
        multi_progress: MultiProgress,
    ) -> Self {
        Self {
            rx_progress,
            total_size,
            multi_progress,
        }
    }

    pub(crate) fn spawn(self) -> JoinHandle<()> {
        spawn_progress_reporter(self.total_size, self.rx_progress, self.multi_progress)
    }
}

pub(crate) fn spawn_progress_reporter(
    total_size: Option<u32>,
    mut rx_progress: mpsc::Receiver<ByteCount>,
    multi: MultiProgress,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut progress = 0;
        type ProgressPoint = (ByteCount, Instant);
        let progress_q = Arc::new(RwLock::new(CircularBuffer::<50, ProgressPoint>::new()));
        let pb = total_size.map_or_else(ProgressBar::new_spinner, |size| {
            ProgressBar::new(size as u64)
        });
        let pb = multi.add(pb);
        pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta}) ({msg})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));

        // Spawn a task to update the speed every 500ms
        {
            let progress_q = progress_q.clone();
            let pb = pb.clone();
            const UPDATE_INTERVAL: Duration = Duration::from_millis(500);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(UPDATE_INTERVAL).await;
                    let q = progress_q.read().await;
                    let back = q.back();
                    let front = q.front();
                    if let (
                        Some((latest_byte, latest_instant)),
                        Some((oldest_byte, oldest_instant)),
                    ) = (back, front)
                    {
                        if latest_byte == oldest_byte {
                            continue;
                        }
                        let speed = (latest_byte - oldest_byte) as f64
                            / latest_instant.duration_since(*oldest_instant).as_secs_f64();
                        let (unit, speed) = if speed > 1024.0 {
                            ("MB/s", speed / (1024.0 * 1024.0))
                        } else {
                            ("kB/s", speed / 1024.0)
                        };
                        pb.set_message(format!("{:.1} {}", speed, unit));
                    }
                }
            });
        }

        while let Some(chunk_size) = rx_progress.recv().await {
            progress += chunk_size;
            pb.set_position(progress);
            let mut q = progress_q.write().await;
            q.push_back((progress, Instant::now()));
        }
        let speed = progress as f64 / pb.elapsed().as_secs_f64();
        let (unit, speed) = if speed > 1024.0 {
            ("MB/s", speed / (1024.0 * 1024.0))
        } else {
            ("kB/s", speed / 1024.0)
        };
        pb.finish_with_message(format!("({:.1} {})", speed, unit));
        println!("");
    })
}
