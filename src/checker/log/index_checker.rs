use anyhow::{anyhow, ensure, Result};

use crate::{
    formats::log::{IndexFileHeader, UuidIndex, INDEX_HEADER},
    util::BlockIODevice,
};

// Checker of IndexChecker
pub(crate) struct IndexChecker<'a, F> {
    file: &'a mut F,
    file_size: &'a mut u64,
    index_size: u64,
    header_size: u64,
}

impl<'a, F: BlockIODevice> IndexChecker<'a, F> {
    // create a checker
    pub fn check(file: &'a mut F, file_size: &'a mut u64) -> Self {
        Self {
            file,
            file_size,
            index_size: bincode::serialized_size(&UuidIndex::default())
                .expect("Serialization failed"),
            header_size: bincode::serialized_size(&INDEX_HEADER).expect("Serialization failed"),
        }
    }

    // check and get header only
    pub fn header(self) -> Result<IndexFileHeader> {
        ensure!(*self.file_size > 0, "Empty log index file");
        ensure!(
            *self.file_size >= self.header_size,
            "Invalid log file: broken header"
        );
        ensure!(
            (*self.file_size - self.header_size) % self.index_size == 0,
            "Broken index file"
        );

        let mut buf = vec![0u8; self.header_size as _];
        self.file.read_at(0, &mut buf)?;
        (bincode::deserialize::<IndexFileHeader>(&buf)? == INDEX_HEADER)
            .then_some(INDEX_HEADER)
            .ok_or(anyhow!("Invalid index file header"))
    }

    // init header if file is empty
    pub fn or_init(self) -> Result<IndexFileHeader> {
        if *self.file_size == 0 {
            self.file.write_all(&bincode::serialize(&INDEX_HEADER)?)?;
            *self.file_size = self.header_size;

            return Ok(INDEX_HEADER);
        }

        self.header()
    }
}
