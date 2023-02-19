use std::{fs::File, io::Write, path::Path};

use fs2::FileExt;
use memmap2::{MmapOptions, MmapRaw};

use crate::{error::Result, formats::log::Header};

/// A wrapper for [`MmapRaw`], and has a 16-byte header.
pub(crate) struct Map {
    raw: MmapRaw,
    header: Header,
    file: File,
}

impl Map {
    pub(crate) fn new(path: &Path, size: u64, header: Header) -> Result<Self> {
        assert!(size >= 16);

        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        file.try_lock_exclusive()?;
        file.set_len(size)?;

        let raw = MmapOptions::new().map_raw(&file)?;
        let mut this = Self { raw, header, file };
        // TODO: check header if exist
        this.init_header(&header)?;

        Ok(this)
    }

    pub fn flush(&self) -> Result<()> {
        self.raw.flush_async().map_err(Into::into)
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn len(&self) -> usize {
        // Offset by size of header
        self.raw.len() - 16
    }

    pub fn as_ptr(&self) -> *const u8 {
        // Offset by size of header
        unsafe { self.raw.as_ptr().add(16) }
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        // Offset by size of header
        unsafe { self.raw.as_mut_ptr().add(16) }
    }

    /// Write the header to the mmap
    fn init_header(&mut self, header: &Header) -> Result<()> {
        unsafe { Ok(self.as_slice_mut().write_all(&header.as_bytes())?) }
    }

    /// # Safety
    /// Caller must ensure that no other mutable access to the chunk is held
    pub unsafe fn as_slice(&self) -> &[u8] {
        std::slice::from_raw_parts(self.as_ptr(), self.len())
    }

    /// # Safety
    /// Caller must ensure that access to the slice is exclusive
    pub unsafe fn as_slice_mut(&mut self) -> &mut [u8] {
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
}

impl Drop for Map {
    fn drop(&mut self) {
        self.raw.flush().unwrap();
        self.file.unlock().unwrap();
        self.file.set_len(self.raw.len() as u64).unwrap();
    }
}
