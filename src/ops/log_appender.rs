use anyhow::Result;
use std::{fs::File, io::Write};

use crate::formats::log::{Index, Log, Timestamp};

use super::Append;

const BUF_SIZE: usize = 4 * 1024;

pub struct LogAppender {
    log_file: File,
    index_file: File,
    timestamps_file: File,

    log_size: u64,
    buf: Vec<u8>,
}

impl LogAppender {
    pub fn new(log: File, index: File, timestamps: File) -> Result<Self> {
        Ok(Self {
            log_size: log.metadata()?.len(),
            log_file: log,
            index_file: index,
            timestamps_file: timestamps,
            buf: Vec::with_capacity(BUF_SIZE),
        })
    }
}

impl LogAppender {
    #[inline]
    fn insert_logs(
        &mut self,
        batch: Vec<(u64, u64, Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<(u64, u64, u64)>> {
        self.buf.clear();

        let res = batch
            .into_iter()
            .map(|(id, ts, key, value)| {
                let log_data = Log { ts, id, key, value }.into_bytes().unwrap();

                self.log_size += log_data.len() as u64;
                self.buf.write_all(&log_data).unwrap();

                (id, ts, self.log_size - 1)
            })
            .collect::<Vec<_>>();

        self.log_file.write_all(&self.buf)?;
        // self.log_file.sync_data()?;

        Ok(res)
    }

    #[inline]
    fn insert_indexes(&mut self, batch_metadata: &Vec<(u64, u64, u64)>) -> Result<()> {
        self.buf.clear();

        batch_metadata.iter().for_each(|(id, _, offset)| {
            self.buf
                .write_all(&Index(*id, *offset).into_bytes().unwrap())
                .unwrap();
        });

        self.index_file.write_all(&self.buf)?;
        // self.index_file.sync_data()?;

        Ok(())
    }

    #[inline]
    fn insert_timestamps(&mut self, batch_metadata: &Vec<(u64, u64, u64)>) -> Result<()> {
        self.buf.clear();

        batch_metadata.iter().for_each(|(_, ts, offset)| {
            self.buf
                .write_all(&Timestamp(*ts, *offset).into_bytes().unwrap())
                .unwrap();
        });

        self.timestamps_file.write_all(&self.buf)?;
        // self.timestamps_file.sync_data()?;

        Ok(())
    }
}

impl Append for LogAppender {
    type Key = Vec<u8>;
    type Value = Vec<u8>;

    fn append_batch(&mut self, batch: Vec<(u64, u64, Self::Key, Self::Value)>) -> Result<()> {
        let metadata = self.insert_logs(batch)?;
        self.insert_indexes(&metadata)?;
        self.insert_timestamps(&metadata)?;

        Ok(())
    }
}
