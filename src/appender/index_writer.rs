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
    pub(crate) fn new(path: impl AsRef<Path>, header: IndexFileHeader) -> Result<Self> {
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

        let (sender, receiver) = bounded(8);
        thread::spawn(move || Self::exec(file, file_size, receiver, header));

        Ok(Self { sender })
    }

    pub(crate) fn append_log_indexes(&self, indexes: Vec<T>) -> Result<()> {
        self.sender.send(indexes)?;
        Ok(())
    }

    fn exec(
        mut index_file: File,
        file_size: u64,
        receiver: Receiver<Vec<T>>,
        header: IndexFileHeader,
    ) -> Result<()> {
        Self::check_or_init_header(&mut index_file, file_size, header)?;

        while let Ok(indexes) = receiver.recv() {
            let mut buf = Vec::with_capacity(256);

            for index in indexes {
                let bytes = bincode::serialize(&index).unwrap();
                buf.write_all(&bytes).unwrap();
            }

            index_file.write_all(&buf)?;
            index_file.sync_data()?;
        }

        Ok(())
    }

    fn check_or_init_header(
        index_file: &mut File,
        file_size: u64,
        header: IndexFileHeader,
    ) -> Result<()> {
        if file_size == 0 {
            index_file.write_all(&bincode::serialize(&header)?)?;
            index_file.sync_data()?;

            Ok(())
        } else {
            let mut buf = Box::new([0u8; size_of::<IndexFileHeader>()]);
            index_file.read_at(0, buf.as_mut_slice())?;

            (IndexFileHeader::try_from(buf.as_slice())? == header)
                .then_some(())
                .ok_or(anyhow!("Invalid file header"))
        }
    }
}
