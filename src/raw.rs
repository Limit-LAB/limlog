use std::{fs::File, mem::ManuallyDrop, path::Path};

use fs2::FileExt;
use memmap2::{MmapOptions, MmapRaw};
use tap::{Pipe, Tap};
use tracing::trace;

use crate::{consts::HEADER_SIZE, error::Result, formats::Header};

/// A wrapper for [`MmapRaw`], with a 16-byte header.
#[derive(Debug)]
pub struct RawMap {
    raw: ManuallyDrop<MmapRaw>,
    file: File,
}

impl RawMap {
    pub(crate) fn new(path: &Path, size: u64, header: Header) -> Result<Self> {
        trace!(?path, size, "Opening mmap");

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.try_lock_exclusive()?;
        file.set_len(size + HEADER_SIZE as u64)?;
        let raw = MmapOptions::new().map_raw(&file)?.pipe(ManuallyDrop::new);
        let this = Self { raw, file };
        this.write_header(header);
        Ok(this)
    }

    pub fn advice_write(&self, _offset: usize, _len: usize) -> Result<()> {
        // #[cfg(unix)]
        // self.raw.advise_range(Advice:: offset, len).map_err(Into::into)
        todo!()
    }

    pub fn flush(&self) -> Result<()> {
        self.raw.flush_async().map_err(Into::into)
    }

    pub fn flush_sync(&self) -> Result<()> {
        self.raw.flush().map_err(Into::into)
    }

    pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        self.raw.flush_async_range(offset, len).map_err(Into::into)
    }

    pub const fn file(&self) -> &File {
        &self.file
    }

    pub fn len(&self) -> usize {
        // Offset by size of header
        self.raw.len() - HEADER_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_ptr(&self) -> *const u8 {
        // Offset by size of header
        unsafe { self.raw.as_ptr().add(HEADER_SIZE) }
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        // Offset by size of header
        unsafe { self.raw.as_mut_ptr().add(HEADER_SIZE) }
    }

    #[allow(clippy::missing_panics_doc)]
    pub fn load_header(&self) -> Header {
        Header::from_bytes(unsafe {
            &std::slice::from_raw_parts(self.raw.as_ptr(), HEADER_SIZE)
                .try_into()
                .unwrap()
        })
    }

    pub fn update_header(&self, func: impl FnOnce(&mut Header)) {
        let mut header = self.load_header();
        func(&mut header);
        self.write_header(header);
    }

    /// Write the header to the mmap
    fn write_header(&self, header: Header) {
        unsafe { header.write_to(std::slice::from_raw_parts_mut(self.raw.as_mut_ptr(), 16)) }
    }

    /// # Safety
    /// Caller must ensure that offset is less than length of the mmap
    pub unsafe fn range(&self, offset: usize, len: usize) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr().add(offset), len)
    }

    /// # Safety
    /// Caller must ensure that access to the slice is exclusive and offset is
    /// less than length of the mmap
    pub unsafe fn range_mut(&mut self, offset: usize, len: usize) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.as_mut_ptr().add(offset), len)
    }

    /// Like `Drop`, but close the file with specified length. This is intended
    /// to be used in `Drop` implementations of other wrapper types, and
    /// caller must guarantee that this will only run once.
    ///
    /// # Safety
    ///
    /// This function can only be called once.
    pub unsafe fn close(&mut self, final_len: u64) -> Result<()> {
        trace!(final_len, map = ?self, "Closing mmap");

        // Unlock and truncate even if flush failed
        self.raw
            .flush()
            .tap(|_| ManuallyDrop::drop(&mut self.raw)) // Drop the mmap before unlocking the file so that windows won't complain
            .and(self.file.set_len(final_len + HEADER_SIZE as u64))
            .and(self.file.unlock())
            .map_err(Into::into)
    }
}
