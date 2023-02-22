use std::{fs::File, path::Path};

use fs2::FileExt;
use memmap2::{MmapOptions, MmapRaw};

use crate::{consts::HEADER_SIZE, error::Result, formats::Header};

/// A wrapper for [`MmapRaw`], with a 16-byte header.
pub(crate) struct RawMap {
    raw: MmapRaw,
    file: File,
}

impl RawMap {
    pub(crate) fn new(path: &Path, size: u64, header: Header) -> Result<Self> {
        assert!(
            size >= HEADER_SIZE as _,
            "Size of mmap must be at least {} bytes",
            HEADER_SIZE
        );

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.try_lock_exclusive()?;
        file.set_len(size)?;
        let raw = MmapOptions::new().map_raw(&file)?;
        let this = Self { raw, file };
        this.write_header(header);
        Ok(this)
    }

    pub fn advice_write(&self, offset: usize, len: usize) -> Result<()> {
        // #[cfg(unix)]
        // self.raw.advise_range(Advice:: offset, len).map_err(Into::into)
        todo!()
    }

    pub fn flush(&self) -> Result<()> {
        self.raw.flush_async().map_err(Into::into)
    }

    pub fn flush_range(&self, offset: usize, len: usize) -> Result<()> {
        self.raw.flush_async_range(offset, len).map_err(Into::into)
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn len(&self) -> usize {
        // Offset by size of header
        self.raw.len() - HEADER_SIZE
    }

    pub fn as_ptr(&self) -> *const u8 {
        // Offset by size of header
        unsafe { self.raw.as_ptr().add(HEADER_SIZE) }
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        // Offset by size of header
        unsafe { self.raw.as_mut_ptr().add(HEADER_SIZE) }
    }

    pub fn load_header(&self) -> Header {
        Header::from_bytes(unsafe {
            &std::slice::from_raw_parts(self.raw.as_ptr(), HEADER_SIZE)
                .try_into()
                .unwrap()
        })
    }

    pub fn update_header(&self, func: impl FnOnce(Header) -> Header) {
        let header = self.load_header();
        func(header);
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

    pub fn close(&self, final_len: u64) -> Result<()> {
        // Unlock and truncate even if flush failed
        self.raw
            .flush()
            .and(self.file.set_len(final_len))
            .and(self.file.unlock())
            .map_err(Into::into)
    }
}
