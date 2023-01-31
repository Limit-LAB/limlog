mod index_writer;
mod log_writer;

use anyhow::{anyhow, ensure, Result};
use crossbeam::queue::ArrayQueue;
use once_cell::sync::OnceCell;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{formats::log::Log, log_groups};

use self::log_writer::LogWriter;

#[derive(Debug)]
pub struct Builder {
    work_dir: PathBuf,
    queue_size: usize,
    flush_percent: f32,
    file_size_threshold: u64,
}

impl Builder {
    pub fn new(path: impl AsRef<Path>) -> Builder {
        Self {
            work_dir: path.as_ref().to_path_buf(),
            queue_size: 128,
            flush_percent: 0.2,                   // 20%
            file_size_threshold: 5 * 1024 * 1024, // 500 MiB
        }
    }

    pub fn queue_size(mut self, queue_size: usize) -> Builder {
        self.queue_size = queue_size;
        self
    }

    pub fn file_size_threshold(mut self, file_size_threshold: u64) -> Builder {
        self.file_size_threshold = file_size_threshold;
        self
    }

    pub fn flush_percent(mut self, flush_percent: f32) -> Builder {
        self.flush_percent = flush_percent;
        self
    }

    pub fn build(self) -> Result<LogAppender> {
        ensure!(self.work_dir.is_dir(), "Path must be a directory");
        ensure!(self.queue_size > 0, "Queue size must be greater than zero");
        ensure!(
            self.file_size_threshold > 128,
            "File max size must be greater than 128 bytes"
        );

        let writer = find_latest_log_group(&self.work_dir)
            .and_then(|(id, ts)| {
                Some(OnceCell::with_value(
                    LogWriter::new(&self.work_dir, format!("{id}_{ts}")).ok()?,
                ))
            })
            .unwrap_or_else(|| OnceCell::new());

        Ok(LogAppender {
            writer: Arc::new(writer),
            inner: Arc::new(LogAppenderInner {
                work_dir: self.work_dir,
                queue: ArrayQueue::new(self.queue_size),
                flush_len: (self.queue_size as f32 * self.flush_percent) as _,
                file_size_threshold: self.file_size_threshold,
            }),
        })
    }
}

#[derive(Clone, Debug)]
pub struct LogAppender {
    inner: Arc<LogAppenderInner>,
    writer: Arc<OnceCell<LogWriter>>,
}

impl LogAppender {
    pub fn builder(path: impl AsRef<Path>) -> Builder {
        Builder::new(path)
    }

    #[inline]
    pub fn insert(&self, log: Log) -> Result<()> {
        self.insert_batch(vec![log])
    }

    pub fn insert_batch(&self, batch: impl IntoIterator<Item = Log>) -> Result<()> {
        for log in batch {
            if let Err(log) = self.inner.queue.push(log) {
                self.flush()?;
                self.inner
                    .queue
                    .push(log)
                    .map_err(|log| anyhow!("Insert {:?} failed", log))?;
            }
        }

        if self.inner.queue.len() > self.inner.flush_len {
            self.flush()?;
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut logs = Vec::with_capacity(self.inner.queue.len());

        while let Some(log) = self.inner.queue.pop() {
            logs.push(log);
        }

        let Some(first) = logs.first() else { return Ok(()); };

        if let Some(writer) = self.writer.get() {
            if writer.file_size() >= self.inner.file_size_threshold {
                _ = self.writer.set(LogWriter::new(
                    &self.inner.work_dir,
                    format!("{}_{}", first.id, first.ts),
                )?);
            }
        }

        let writer = self.writer.get_or_try_init(|| {
            LogWriter::new(&self.inner.work_dir, format!("{}_{}", first.id, first.ts))
        })?;

        writer.append_logs(logs)?;

        Ok(())
    }
}

#[derive(Debug)]
struct LogAppenderInner {
    work_dir: PathBuf,
    queue: ArrayQueue<Log>,
    flush_len: usize,
    file_size_threshold: u64,
}

fn find_latest_log_group(log_dir: impl AsRef<Path>) -> Option<(u64, u64)> {
    let log_groups = log_groups(log_dir);
    let mut latest = *log_groups.first()?;

    // FIXME: last modified conflicts with ts in the filename
    for entry in log_groups {
        if entry.ts < latest.ts {
            latest = entry;
        }
    }

    Some((latest.id, latest.ts))
}
