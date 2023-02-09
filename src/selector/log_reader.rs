use std::io::SeekFrom;

use anyhow::Result;

use crate::{checker::LogChecker, util::BlockIODevice, Log};

#[derive(Debug)]
pub(crate) struct LogReader<F> {
    file: F,
}

impl<F: BlockIODevice> LogReader<F> {
    // New and check log file
    pub(crate) fn new(mut file: F) -> Result<LogReader<F>> {
        let mut file_size = file.len()?;
        LogChecker::check(&mut file, &mut file_size).header()?;

        Ok(Self { file })
    }

    pub(crate) fn select_logs(
        &mut self,
        start: u64, // starting offset of the log file to be selected
        count: u64, // number of logs to be selected
    ) -> Result<Vec<Log>> {
        let mut logs = Vec::with_capacity(count as usize);
        self.file.seek(SeekFrom::Start(start))?;

        for _ in 0..count {
            // deserialize a log per times
            logs.push(bincode::deserialize_from(&mut self.file)?);
        }

        Ok(logs)
    }
}
