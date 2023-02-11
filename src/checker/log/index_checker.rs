use std::mem::size_of;

use crate::formats::log::IndexFileHeader;
use crate::util::BlockIODevice;
use anyhow::{anyhow, ensure, Result};

pub(crate) struct IndexChecker<'a, F> {
    file: &'a mut F,
    file_size: &'a mut u64,
    index_size: u64,
    expected_header: IndexFileHeader,
}

impl<'a, F: BlockIODevice> IndexChecker<'a, F> {
    // create a checker
    pub fn check<I>(
        file: &'a mut F,
        file_size: &'a mut u64,
        expected_header: IndexFileHeader,
    ) -> Self {
        Self {
            file,
            file_size,
            index_size: size_of::<I>() as _,
            expected_header,
        }
    }

    // check and get header only
    pub fn header(self) -> Result<()> {
        ensure!(*self.file_size > 0, "Empty log index file");
        ensure!(
            *self.file_size >= size_of::<IndexFileHeader>() as u64,
            "Invalid log file: broken header"
        );
        ensure!(
            (*self.file_size - size_of::<IndexFileHeader>() as u64) % self.index_size == 0,
            "Broken index file"
        );

        let mut buf = [0u8; size_of::<IndexFileHeader>()];
        self.file.read_at(0, &mut buf)?;
        (bincode::deserialize::<IndexFileHeader>(&buf)? == self.expected_header)
            .then_some(())
            .ok_or(anyhow!("Invalid index file header"))
    }

    // init header if file is empty
    pub fn or_init(self) -> Result<()> {
        if *self.file_size == 0 {
            let header = self.expected_header;
            self.file.write_all(&bincode::serialize(&header)?)?;
            *self.file_size = size_of::<IndexFileHeader>() as u64;

            return Ok(());
        }

        self.header()
    }
}
