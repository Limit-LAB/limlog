use std::{io::SeekFrom, mem::size_of};

use anyhow::{ensure, Result};

use crate::{formats::log::LogFileHeader, util::BlockIODevice, Log};

#[derive(Debug)]
pub(crate) struct LogReader<F> {
    file: F,
}

impl<F: BlockIODevice> LogReader<F> {
    pub(crate) fn new(file: F) -> Result<LogReader<F>> {
        let file_size = file.len()?;
        ensure!(
            file_size == 0 || file_size > size_of::<LogFileHeader>() as u64,
            "Invalid log file: broken header"
        );

        Ok(Self { file })
    }

    pub(crate) fn select_logs(&mut self, start: u64, count: u64) -> Result<Vec<Log>> {
        let mut logs = Vec::with_capacity(count as usize);
        self.file.seek(SeekFrom::Start(start))?;

        for _ in 0..count {
            logs.push(bincode::deserialize_from(&mut self.file)?);
        }

        Ok(logs)
    }
}
