mod index_writer;
pub(crate) mod log_writer;

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use anyhow::{anyhow, ensure, Result};
use crossbeam_queue::ArrayQueue;
use uuid7::Uuid;

use self::log_writer::LogWriter;
use crate::{formats::log::Log, util::log_groups};

#[derive(Debug)]
pub struct Builder {
    work_dir: PathBuf,
    queue_size: usize,
    flush_percent: f32,
    file_size_threshold: u64,
}

impl Builder {
    /// Creates a new builder for the [LogAppender] by given log directory.
    pub fn new(path: impl AsRef<Path>) -> Builder {
        Self {
            work_dir: path.as_ref().to_path_buf(),
            queue_size: 128,
            flush_percent: 0.2,                     // 20%
            file_size_threshold: 500 * 1024 * 1024, // 500 MiB
        }
    }

    /// Set the buffer queue size, default is 128.
    pub fn queue_size(mut self, queue_size: usize) -> Builder {
        self.queue_size = queue_size;
        self
    }

    /// Set the log file size threshold.
    ///
    /// A new log file will be created when the log file size exceeds the
    /// threshold. default is 500 MiB.
    pub fn file_size_threshold(mut self, file_size_threshold: u64) -> Builder {
        self.file_size_threshold = file_size_threshold;
        self
    }

    /// Set the flush percentage.
    ///
    /// [LogAppender] will automatically flush
    /// when queue len exceeds the queue_size * flush_percentage.
    pub fn flush_percentage(mut self, flush_percent: f32) -> Builder {
        self.flush_percent = flush_percent;
        self
    }

    /// Build a [LogAppender].
    pub fn build(self) -> Result<LogAppender> {
        ensure!(self.work_dir.is_dir(), "Path must be a directory");
        ensure!(self.queue_size > 0, "Queue size must be greater than zero");
        ensure!(
            self.file_size_threshold > 128,
            "File max size must be greater than 128 bytes"
        );

        // open the latest log group if present
        let writer = find_latest_log_group(&self.work_dir)
            .and_then(|uuid| {
                let writer = OnceLock::new();
                let Ok(log_writer) = LogAppender::open_log_group(&self.work_dir, &uuid) else {
                    // archive broken log group
                    LogAppender::archive_log_group(&self.work_dir, &uuid).ok()?;
                    return None;
                };
                writer.set(log_writer).ok()?;
                Some(writer)
            })
            .unwrap_or_else(OnceLock::new);

        Ok(LogAppender {
            inner: Arc::new(LogAppenderInner {
                writer,
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
}

#[derive(Debug)]
struct LogAppenderInner {
    work_dir: PathBuf,
    queue: ArrayQueue<Log>,
    flush_len: usize,
    file_size_threshold: u64,

    writer: OnceLock<LogWriter>,
}

macro_rules! log_file_path {
    ($dir:expr, $file_name:expr, $ext:literal) => {
        $dir.join(format!(concat!("{}.", $ext), $file_name))
    };
}

impl LogAppender {
    pub fn builder(path: impl AsRef<Path>) -> Builder {
        Builder::new(path)
    }

    /// Insert a log asynchronously.
    #[inline]
    pub fn insert(&self, log: Log) -> Result<()> {
        self.insert_batch(vec![log])
    }

    /// Insert a log batch asynchronously.
    pub fn insert_batch(&self, batch: impl IntoIterator<Item = Log>) -> Result<()> {
        for log in batch {
            if let Err(log) = self.inner.queue.push(log) {
                self.flush()?;
                self.inner
                    .queue
                    .push(log)
                    .expect("Failed to flush log");
            }
        }

        if self.inner.queue.len() > self.inner.flush_len {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush logs in the buffer queue to disk
    pub fn flush(&self) -> Result<()> {
        let mut logs = Vec::with_capacity(self.inner.queue.len());
        while let Some(log) = self.inner.queue.pop() {
            logs.push(log);
        }

        let Some(first) = logs.first() else { return Ok(()); };
        if let Some(writer) = self.inner.writer.get() {
            if writer.file_size() >= self.inner.file_size_threshold {
                // 神父换碟
                _ = self.inner.writer.set(self.create_log_group(&first.uuid)?);
            }
        }

        self.inner
            .writer
            .get_or_try_init(|| self.create_log_group(&first.uuid))?
            .append_logs(logs)?;

        Ok(())
    }

    // open a exist log group
    fn open_log_group(path: impl AsRef<Path>, uuid: &Uuid) -> Result<LogWriter> {
        let mut binding = File::options();
        let filename = uuid.to_string();
        let options = binding.append(true).read(true);

        LogWriter::new(
            options.open(log_file_path!(path.as_ref(), filename, "limlog"))?,
            options.open(log_file_path!(path.as_ref(), filename, "idx"))?,
        )
    }

    fn archive_log_group(path: impl AsRef<Path>, uuid: &Uuid) -> Result<()> {
        let filename = uuid.to_string();

        fs::rename(
            log_file_path!(path.as_ref(), filename, "limlog"),
            log_file_path!(path.as_ref(), filename, "limlog.old"),
        )?;
        fs::rename(
            log_file_path!(path.as_ref(), filename, "idx"),
            log_file_path!(path.as_ref(), filename, "idx.old"),
        )?;

        Ok(())
    }

    // create a brand new log group
    fn create_log_group(&self, uuid: &Uuid) -> Result<LogWriter> {
        let mut binding = File::options();
        let filename = uuid.to_string();
        let options = binding.append(true).create_new(true).read(true);

        LogWriter::new(
            options.open(log_file_path!(self.inner.work_dir, filename, "limlog"))?,
            options.open(log_file_path!(self.inner.work_dir, filename, "idx"))?,
        )
    }
}

fn find_latest_log_group(log_dir: impl AsRef<Path>) -> Option<Uuid> {
    let log_groups = log_groups(log_dir);
    let mut latest = *log_groups.first()?;

    // FIXME: last modified conflicts with ts in the filename
    for uuid in log_groups {
        if uuid < latest {
            latest = uuid;
        }
    }

    Some(latest)
}
