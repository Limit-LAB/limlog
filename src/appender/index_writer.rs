use anyhow::{anyhow, ensure, Result};
use kanal::{bounded, Receiver, Sender};
use positioned_io::ReadAt;
use serde::Serialize;
use std::{fs::File, io::Write, mem::size_of, path::Path, thread};

use crate::formats::log::IndexFileHeader;

#[derive(Debug)]
pub(crate) struct IndexWriter<T: Serialize + Send + Sync + 'static> {
    sender: Sender<Vec<T>>,
}

impl<T: Serialize + Send + Sync + 'static> IndexWriter<T> {
    pub(crate) fn new(path: impl AsRef<Path>, expected_header: IndexFileHeader) -> Result<Self> {
        let (file, file_size) = Self::open_file(path)?;
        let (sender, receiver) = bounded(8);
        let inner = IndexWriterInner {
            file,
            file_size,
            receiver,
            expected_header,
        };
        thread::spawn(move || inner.exec());

        Ok(Self { sender })
    }

    pub(crate) fn append_log_indexes(&self, indexes: Vec<T>) -> Result<()> {
        Ok(self.sender.send(indexes)?)
    }

    #[inline]
    fn open_file(path: impl AsRef<Path>) -> Result<(File, u64)> {
        let file = File::options()
            .append(true)
            .create(true)
            .open(path.as_ref())?;

        let file_size = file.metadata()?.len();
        ensure!(
            file_size == 0
                || (file_size - size_of::<IndexFileHeader>() as u64) % size_of::<T>() as u64 == 0,
            "Invalid log index file"
        );

        Ok((file, file_size))
    }
}

struct IndexWriterInner<T: Serialize + Send + Sync + 'static> {
    file: File,
    file_size: u64,
    receiver: Receiver<Vec<T>>,
    expected_header: IndexFileHeader,
}

impl<T: Serialize + Send + Sync + 'static> IndexWriterInner<T> {
    fn exec(mut self) -> Result<()> {
        self.check_or_init_header()?;

        while let Ok(indexes) = self.receiver.recv() {
            let mut buf = Vec::with_capacity(256);

            for index in indexes {
                let bytes = bincode::serialize(&index).unwrap();
                buf.write_all(&bytes).unwrap();
            }

            self.file.write_all(&buf)?;
            self.file.sync_data()?;
        }

        Ok(())
    }

    fn check_or_init_header(&mut self) -> Result<()> {
        if self.file_size == 0 {
            self.file
                .write_all(&bincode::serialize(&self.expected_header)?)?;
            self.file.sync_data()?;

            Ok(())
        } else {
            let mut buf = Box::new([0u8; size_of::<IndexFileHeader>()]);
            self.file.read_at(0, buf.as_mut_slice())?;

            (IndexFileHeader::try_from(buf.as_slice())? == self.expected_header)
                .then_some(())
                .ok_or(anyhow!("Invalid file header"))
        }
    }
}
