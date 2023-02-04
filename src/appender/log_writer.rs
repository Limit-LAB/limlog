use anyhow::{ensure, Result};
use kanal::{bounded, Receiver, Sender};
use positioned_io::{ReadAt, WriteAt};
use std::{
    fs::File,
    io::Write,
    mem::size_of,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use crate::{
    appender::index_writer::IndexWriter,
    formats::log::{Index, Log, LogFileHeader, Timestamp, INDEX_HEADER, TS_INDEX_HEADER},
};

type Counter = Arc<AtomicU64>;

#[derive(Debug)]
pub(crate) struct LogWriter {
    sender: Sender<Vec<Log>>,
    file_size_view: Counter,
}

impl LogWriter {
    pub(crate) fn new(path: impl AsRef<Path>, file_name: String) -> Result<LogWriter> {
        let (file, file_size) = Self::open_file(&path, &file_name)?;
        let (sender, receiver) = bounded(8);
        let inner = LogWriterInner {
            file,
            file_size: Arc::new(AtomicU64::new(file_size)),
            receiver,
            idx_writer: IndexWriter::new(
                path.as_ref().join(format!("{file_name}.idx")),
                INDEX_HEADER,
            )?,
            ts_idx_writer: IndexWriter::new(
                path.as_ref().join(format!("{file_name}.ts.idx")),
                TS_INDEX_HEADER,
            )?,
        };

        let file_size_view = inner.file_size.clone();
        thread::spawn(move || inner.exec());

        Ok(Self {
            sender,
            file_size_view,
        })
    }

    pub(crate) fn append_logs(&self, logs: Vec<Log>) -> Result<()> {
        self.sender.send(logs)?;
        Ok(())
    }

    pub(crate) fn file_size(&self) -> u64 {
        self.file_size_view.load(Ordering::Acquire)
    }

    fn open_file(path: impl AsRef<Path>, file_name: &String) -> Result<(File, u64)> {
        let file = File::options()
            .append(true)
            .create(true)
            .read(true)
            .open(path.as_ref().join(format!("{file_name}.limlog")))?;

        let file_size = file.metadata()?.len();
        ensure!(
            file_size == 0 || file_size > size_of::<LogFileHeader>() as u64,
            "Invalid log file: broken header"
        );

        Ok((file, file_size))
    }
}

struct LogWriterInner {
    file: File,
    file_size: Counter,
    receiver: Receiver<Vec<Log>>,
    idx_writer: IndexWriter<Index>,
    ts_idx_writer: IndexWriter<Timestamp>,
}

impl LogWriterInner {
    fn exec(mut self) -> Result<()> {
        let mut header = self.get_or_init_header()?;

        while let Ok(logs) = self.receiver.recv() {
            let mut buf = Vec::with_capacity(1024);
            let mut idx = Vec::with_capacity(logs.len());
            let mut ts_idx = Vec::with_capacity(logs.len());

            for log in logs {
                let size = self.file_size.load(Ordering::Acquire);
                let bytes = bincode::serialize(&log)?;

                idx.push(Index(log.id, size));
                ts_idx.push(Timestamp(log.ts, size));

                buf.write_all(&bytes)?;
                header.entry_count += 1;
                self.file_size
                    .fetch_add(bytes.len() as u64, Ordering::AcqRel);
            }

            self.idx_writer.append_log_indexes(idx)?;
            self.ts_idx_writer.append_log_indexes(ts_idx)?;

            self.file.write_all(&buf)?;
            self.file.write_at(0, &bincode::serialize(&header)?)?;
            self.file.sync_data()?;
        }

        Ok(())
    }

    fn get_or_init_header(&mut self) -> Result<LogFileHeader> {
        let mut header = LogFileHeader::default();

        if self.file_size.load(Ordering::Acquire) == 0 {
            self.file.write_all(&bincode::serialize(&header)?)?;
            self.file.sync_data()?;

            self.file_size
                .store(size_of::<LogFileHeader>() as _, Ordering::Release);
        } else {
            let mut buf = Box::new([0u8; size_of::<LogFileHeader>()]);
            self.file.read_at(0, buf.as_mut_slice())?;

            header = LogFileHeader::try_from(buf.as_slice())?;
        }

        Ok(header)
    }
}
