use std::{mem::size_of, ptr::read};

use anyhow::{anyhow, ensure, Result};

use crate::{
    formats::log::IndexFileHeader,
    util::{BlockIODevice, LogItem},
};

#[derive(Debug)]
pub(crate) struct IndexReader<F> {
    file: F,
    start: u64,
    len: u64,
}

impl<F: BlockIODevice> IndexReader<F> {
    pub(crate) fn new<Q>(file: F, expected_header: IndexFileHeader) -> Result<IndexReader<F>>
    where
        Q: LogItem + PartialOrd,
        [u8; size_of::<Q>()]: Sized,
    {
        let file_size = file.len()?;
        let start = size_of::<IndexFileHeader>() as u64;
        ensure!(file_size >= start, "Invalid header");

        let len = file_size - start;
        ensure!(len % size_of::<Q>() as u64 == 0, "Invalid log index file");

        Self::check_header(&file, expected_header)?;

        Ok(Self { file, start, len })
    }

    pub(crate) fn select_range<Q>(&self, start: &Q, end: &Q) -> Result<Option<(Q, u64)>>
    where
        Q: LogItem + PartialOrd,
        [u8; size_of::<Q>()]: Sized,
    {
        // TODO: cache
        let (left, _) = self.binary_search(start, PartialOrd::lt)?;
        let (_, right) = self.binary_search(end, PartialOrd::le)?;

        Ok((right > left).then_some((self.index_item(left)?, right - left)))
    }

    fn binary_search<Q>(&self, target: &Q, cmp: impl Fn(&Q, &Q) -> bool) -> Result<(u64, u64)>
    where
        Q: LogItem + PartialOrd,
        [u8; size_of::<Q>()]: Sized,
    {
        let mut left = 0;
        let mut right = self.len - 1;
        let mut mid;

        while left < right {
            mid = left + (right - left) / 2;
            let v: Q = self.index_item(mid)?;

            if cmp(&v, target) {
                left = mid + 1;
            } else if mid > 0 {
                right = mid - 1;
            } else {
                break;
            }
        }

        Ok((left, right))
    }

    #[inline]
    fn index_to_offset<Q>(&self, index: u64) -> u64
    where
        Q: LogItem + PartialOrd,
        [u8; size_of::<Q>()]: Sized,
    {
        assert!(
            index >= self.len / size_of::<Q>() as u64,
            "Index out of bounds"
        );
        index * size_of::<Q>() as u64 + self.start
    }

    #[inline]
    fn index_item<Q>(&self, index: u64) -> Result<Q>
    where
        Q: LogItem + PartialOrd,
        [u8; size_of::<Q>()]: Sized,
    {
        let mut buf = [0u8; size_of::<Q>()];
        self.file
            .read_at(self.index_to_offset::<Q>(index), buf.as_mut_slice())?;
        // Ok(bincode::deserialize(&buf)?)
        Ok(unsafe { read(buf.as_ptr() as *const _) })
    }

    fn check_header(file: &F, expected_header: IndexFileHeader) -> Result<()> {
        let mut buf = [0u8; size_of::<IndexFileHeader>()];
        file.read_at(0, buf.as_mut_slice())?;

        (IndexFileHeader::try_from(buf.as_slice())? == expected_header)
            .then_some(())
            .ok_or(anyhow!("Invalid file header"))
    }
}
