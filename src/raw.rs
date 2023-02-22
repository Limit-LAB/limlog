use std::{fs::File, path::Path};

use fs2::FileExt;
use memmap2::{MmapOptions, MmapRaw};

use crate::{consts::HEADER_SIZE, error::Result, formats::log::Header};

/// A wrapper for [`MmapRaw`], with a 16-byte header.
pub(crate) struct Map {
    raw: MmapRaw,

    file: File,
}

impl Map {
    pub(crate) fn new(path: &Path, size: u64, header: Header) -> Result<Self> {
        assert!(size >= HEADER_SIZE as _);

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.try_lock_exclusive()?;
        file.set_len(size)?;
        let raw = MmapOptions::new().map_raw(&file)?;
        let this = Self { raw, file };
        this.write_header(&header);
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

    pub fn update_header(&self, func: impl FnOnce(&mut Header)) {
        func(&self.header);
        self.write_header(&self.header);
    }

    fn load_header(&self) -> Header {
        Header::from_bytes(unsafe {
            &std::slice::from_raw_parts(self.raw.as_ptr(), 16)
                .try_into()
                .unwrap()
        })
    }

    /// Write the header to the mmap
    fn write_header(&self, header: &Header) {
        unsafe { header.write_to(std::slice::from_raw_parts_mut(self.raw.as_mut_ptr(), 16)) }
    }

    /// # Safety
    /// Caller must ensure that no other mutable access to the chunk is held
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr(), self.len())
    }

    /// # Safety
    /// Caller must ensure that access to the slice is exclusive
    pub unsafe fn as_mut_slice(&mut self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len())
    }

    /// # Safety
    /// Caller must ensure that no other mutable access to the chunk is held and
    /// offset is less than length of the mmap
    pub unsafe fn start_at(&self, offset: usize) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr().add(offset), self.len())
    }

    /// # Safety
    /// Caller must ensure that access to the slice is exclusive and offset is
    /// less than length of the mmap
    pub unsafe fn start_at_mut(&mut self, offset: usize) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.as_mut_ptr().add(offset), self.len())
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
