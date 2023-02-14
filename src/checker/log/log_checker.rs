use anyhow::{ensure, Result};

use crate::{formats::log::LogFileHeader, util::BlockIODevice};

pub(crate) struct LogChecker<'a, F> {
    file: &'a mut F,
    file_size: &'a mut u64,
    header_size: u64,
}

impl<'a, F: BlockIODevice> LogChecker<'a, F> {
    // create a checker
    pub fn check(file: &'a mut F, file_size: &'a mut u64) -> Self {
        Self {
            file,
            file_size,
            header_size: bincode::serialized_size(&LogFileHeader::default())
                .expect("Serialization failed"),
        }
    }

    // check and get header only
    pub fn header(self) -> Result<LogFileHeader> {
        ensure!(*self.file_size > 0, "Empty log file");
        ensure!(
            *self.file_size >= self.header_size,
            "Invalid log file: broken header"
        );

        let mut buf = vec![0u8; self.header_size as _];
        self.file.read_at(0, &mut buf)?;
        Ok(bincode::deserialize(&buf)?)
    }

    // init header if file is empty
    pub fn or_init(self) -> Result<LogFileHeader> {
        if *self.file_size == 0 {
            let header = LogFileHeader::default();
            self.file.write_all(&bincode::serialize(&header)?)?;
            *self.file_size = self.header_size;

            return Ok(header);
        }

        self.header()
    }
}
