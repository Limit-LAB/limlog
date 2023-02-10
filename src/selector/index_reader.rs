use std::{marker::PhantomData, mem::size_of, ptr::read};

use anyhow::Result;

use crate::{
    checker::IndexChecker,
    formats::log::IndexFileHeader,
    util::{BlockIODevice, LogItem},
};

#[derive(Debug)]
pub(crate) struct IndexReader<F, I> {
    file: F,
    start: u64,
    len: u64,
    phantom: PhantomData<I>,
}

impl<F, I> IndexReader<F, I>
where
    F: BlockIODevice,
    I: LogItem + PartialOrd,
    [u8; size_of::<I>()]: Sized,
{
    pub(crate) fn new(mut file: F, expected_header: IndexFileHeader) -> Result<IndexReader<F, I>> {
        let mut file_size = file.len()?;
        IndexChecker::check::<I>(&mut file, &mut file_size, expected_header).header()?;

        let start = size_of::<IndexFileHeader>() as u64;
        let len = file_size - start;

        Ok(Self {
            file,
            start,
            len,
            phantom: PhantomData,
        })
    }

    // select by given index(eg. Timestamp, Id) on the file
    pub(crate) fn select_range(&self, start: &I, end: &I) -> Result<Option<(I, u64)>> {
        // TODO: cache
        let (left, _) = self.binary_search(start, PartialOrd::lt)?;
        let (_, right) = self.binary_search(end, PartialOrd::le)?;

        Ok((right > left).then_some((self.index_item(left)?, right - left)))
    }

    fn binary_search(&self, target: &I, cmp: impl Fn(&I, &I) -> bool) -> Result<(u64, u64)> {
        let mut left = 0;
        let mut right = self.len / size_of::<I>() as u64 - 1;
        let mut mid;

        while left < right {
            mid = left + (right - left) / 2;
            let v: I = self.index_item(mid)?;

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

    // convert "index" of index item to file offset
    #[inline]
    fn index_to_offset(&self, index: u64) -> u64 {
        debug_assert!(
            index < self.len / size_of::<I>() as u64,
            "Index out of bounds: {index}"
        );
        index * size_of::<I>() as u64 + self.start
    }

    // get index item by given "index"
    #[inline]
    fn index_item(&self, index: u64) -> Result<I> {
        let mut buf = [0u8; size_of::<I>()];
        self.file
            .read_at(self.index_to_offset(index), buf.as_mut_slice())?;
        // Ok(bincode::deserialize(&buf)?)
        Ok(unsafe { read(buf.as_ptr() as *const _) })
    }
}
