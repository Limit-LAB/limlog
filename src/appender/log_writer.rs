use anyhow::{anyhow, ensure, Result};
use crossbeam::channel::{self, Receiver, Sender};
use std::{
    fs::File,
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use crate::{
    appender::index_writer::IndexWriter,
    formats::log::{Index, Log, LogFileHeader, Timestamp},
};

type Counter = Arc<AtomicU64>;

#[derive(Debug)]
pub struct LogWriter {
    handle: JoinHandle<Result<()>>,
    sender: Sender<Vec<Log>>,
    file_size_view: Counter,
}

impl LogWriter {
    pub fn new(path: impl AsRef<Path>, file_name: String) -> Result<LogWriter> {
        let (sender, receiver) = channel::unbounded();
        let file = File::options()
            .append(true)
            .open(path.as_ref().join(format!("{file_name}.limlog")))?;

        let file_size = file.metadata()?.len();
        ensure!(file_size == 0 || file_size > 24, "Invalid log file");

        let idx_writer = IndexWriter::new(path.as_ref(), &file_name, "idx")?;
        let ts_idx_writer = IndexWriter::new(path.as_ref(), &file_name, "ts.idx")?;

        let file_size = Arc::new(AtomicU64::new(file_size));
        let file_size_view = file_size.clone();
        let handle =
            thread::spawn(move || Self::exec(file, file_size, receiver, idx_writer, ts_idx_writer));

        Ok(Self {
            handle,
            sender,
            file_size_view,
        })
    }

    pub fn append_logs(&self, logs: Vec<Log>) -> Result<()> {
        if self.handle.is_finished() {
            Err(anyhow!("Worker thread already exited"))?;
        }

        self.sender.send(logs)?;
        Ok(())
    }

    pub fn file_size(&self) -> u64 {
        self.file_size_view.load(Ordering::Acquire)
    }

    fn exec(
        mut log_file: File,
        file_size: Counter,
        receiver: Receiver<Vec<Log>>,
        idx_writer: IndexWriter<Index>,
        ts_idx_writer: IndexWriter<Timestamp>,
    ) -> Result<()> {
        if file_size.load(Ordering::Acquire) == 0 {
            Self::init_header(&mut log_file)?;
            file_size.store(24, Ordering::Release);
        }

        while let Ok(logs) = receiver.recv() {
            let mut buf = Vec::with_capacity(1024);
            let mut idx = Vec::with_capacity(128);
            let mut ts_idx = Vec::with_capacity(128);

            for log in logs {
                let size = file_size.load(Ordering::Acquire);
                let bytes = bincode::serialize(&log)?;

                idx.push(Index(log.id, size));
                ts_idx.push(Timestamp(log.ts, size));

                buf.write_all(&bytes).unwrap();
                file_size.fetch_add(bytes.len() as u64, Ordering::AcqRel);
            }

            idx_writer.append_log_indexes(idx)?;
            ts_idx_writer.append_log_indexes(ts_idx)?;

            log_file.write_all(&buf)?;
            log_file.sync_data()?;
        }

        Ok(())
    }

    #[inline]
    fn init_header(log_file: &mut File) -> Result<()> {
        // TODO
        let header = LogFileHeader {
            magic_number: 0,
            attributes: 0,
            entry_count: 0,
        };

        log_file.write_all(&bincode::serialize(&header)?)?;
        log_file.sync_data()?;

        Ok(())
    }
}
