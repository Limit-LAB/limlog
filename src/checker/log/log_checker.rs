use std::mem::size_of;

use crate::formats::log::LogFileHeader;
use crate::util::BlockIODevice;
use anyhow::{ensure, Result};

pub(crate) struct LogChecker<'a, F> {
    file: &'a mut F,
    file_size: &'a mut u64,
}

impl<'a, F: BlockIODevice> LogChecker<'a, F> {
    pub fn check(file: &'a mut F, file_size: &'a mut u64) -> Self {
        Self { file, file_size }
    }

    pub fn header(self) -> Result<LogFileHeader> {
        ensure!(*self.file_size > 0, "Empty log file");
        ensure!(
            *self.file_size >= size_of::<LogFileHeader>() as u64,
            "Invalid log file: broken header"
        );

        let mut buf = [0u8; size_of::<LogFileHeader>()];
        self.file.read_at(0, &mut buf)?;
        Ok(bincode::deserialize(&buf)?)
    }

    pub fn or_init(self) -> Result<LogFileHeader> {
        if *self.file_size == 0 {
            let header = LogFileHeader::default();
            self.file.write_all(&bincode::serialize(&header)?)?;
            *self.file_size = size_of::<LogFileHeader>() as u64;

            return Ok(header);
        }

        self.header()
    }
}
