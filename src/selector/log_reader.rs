use std::io::SeekFrom;

use anyhow::Result;

use crate::{checker::LogChecker, util::BlockIODevice, Log};

#[derive(Debug)]
pub(crate) struct LogReader<F> {
    file: F,
}

impl<F: BlockIODevice> LogReader<F> {
    pub(crate) fn new(mut file: F) -> Result<LogReader<F>> {
        let mut file_size = file.len()?;
        LogChecker::check(&mut file, &mut file_size).header()?;

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
