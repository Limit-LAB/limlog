use anyhow::{anyhow, ensure, Result};
use kanal::{bounded, Receiver, Sender};
use std::{mem::size_of, slice::from_raw_parts, thread};

use crate::{
    formats::log::IndexFileHeader,
    util::{BlockIODevice, LogItem},
};

#[derive(Debug)]
pub(crate) struct IndexWriter<T> {
    sender: Sender<Vec<T>>,
}

impl<T: LogItem> IndexWriter<T> {
    pub(crate) fn new(file: impl BlockIODevice, expected_header: IndexFileHeader) -> Result<Self> {
        let file_size = file.len()?;
        let header_len = size_of::<IndexFileHeader>() as u64;
        if file_size > 0 {
            ensure!(
                file_size > header_len && (file_size - header_len) % size_of::<T>() as u64 == 0,
                "Invalid log index file"
            );
        }

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
}

struct IndexWriterInner<F, T> {
    file: F,
    file_size: u64,
    receiver: Receiver<Vec<T>>,
    expected_header: IndexFileHeader,
}

impl<F: BlockIODevice, T: LogItem> IndexWriterInner<F, T> {
    fn exec(mut self) -> Result<()> {
        self.check_or_init_header()?;

        while let Ok(indexes) = self.receiver.recv() {
            // let mut buf = Vec::with_capacity(256);

            // for index in indexes {
            //     let bytes = bincode::serialize(&index).unwrap();
            //     buf.write_all(&bytes).unwrap();
            // }

            let buf = unsafe {
                from_raw_parts(
                    indexes.as_ptr() as *const u8,
                    indexes.len() * size_of::<T>(),
                )
            };

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
            let mut buf = [0u8; size_of::<IndexFileHeader>()];
            self.file.read_at(0, buf.as_mut_slice())?;

            (IndexFileHeader::try_from(buf.as_slice())? == self.expected_header)
                .then_some(())
                .ok_or(anyhow!("Invalid file header"))
        }
    }
}
