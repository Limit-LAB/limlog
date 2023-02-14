use anyhow::Result;
use positioned_io::ReadAt;
use uuid7::Uuid;

use crate::{checker::IndexChecker, formats::log::UuidIndex, util::BlockIODevice};

// Reader for UuidIndex
#[derive(Debug)]
pub(crate) struct IndexReader<F> {
    file: F,
    start: u64,
    len: u64,
    item_size: u64, // UuidIndex serialized size
}

impl<F: BlockIODevice> IndexReader<F> {
    pub(crate) fn new(mut file: F) -> Result<IndexReader<F>> {
        let mut file_size = file.len()?;
        let header = IndexChecker::check(&mut file, &mut file_size).header()?;

        let start = bincode::serialized_size(&header).expect("Serialization failed") as u64;
        let len = file_size - start;

        Ok(Self {
            file,
            start,
            len,
            item_size: bincode::serialized_size(&UuidIndex::default())
                .expect("Serialization failed"),
        })
    }

    // select by given UUID then returns
    pub(crate) fn select_range(
        &self,
        start: &Uuid,
        end: &Uuid,
    ) -> Result<Option<(UuidIndex, u64)>> {
        // TODO: cache
        let (left, _) = self.binary_search(start, PartialOrd::lt)?;
        let (_, right) = self.binary_search(end, PartialOrd::le)?;

        Ok((right > left).then_some((self.index_item(left)?, right - left)))
    }

    fn binary_search(
        &self,
        target: &Uuid,
        cmp: impl Fn(&Uuid, &Uuid) -> bool,
    ) -> Result<(u64, u64)> {
        let mut left = 0;
        let mut right = self.len / self.item_size;
        let mut mid;

        while left < right {
            mid = left + (right - left) / 2;
            let v = self.index_item(mid)?;

            if cmp(&v.uuid, target) {
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
            index < self.len / self.item_size,
            "Index out of bounds: {index}"
        );
        index * self.item_size + self.start
    }

    // get index item by given "index"
    #[inline]
    fn index_item(&self, index: u64) -> Result<UuidIndex> {
        let cur = ReadAtCursor {
            file: &self.file,
            pos: self.index_to_offset(index),
        };

        Ok(bincode::deserialize_from(cur)?)
    }
}

/// A wrapper of `ReadAt` to implement `std::io::Read` so that even `&self` can
/// be read and write. It is possible to add a `for<'a> &'a F: Read` but I'm too
/// lazy to add impl for `TestFile`.
pub struct ReadAtCursor<'a, F> {
    file: &'a F,
    pos: u64,
}

impl<'a, F: ReadAt> std::io::Read for ReadAtCursor<'a, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.file.read_at(self.pos, buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}
