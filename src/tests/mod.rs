use std::{
    io::{Cursor, Read, Seek, Write},
    sync::{Arc, RwLock},
};

use positioned_io::{ReadAt, WriteAt};

#[derive(Debug, Clone)]
struct TestFile(Arc<RwLock<Cursor<Vec<u8>>>>);

impl TestFile {
    fn new(init: Vec<u8>) -> TestFile {
        Self(Arc::new(RwLock::new(Cursor::new(init))))
    }

    fn get_buf(&self) -> Vec<u8> {
        self.0.read().unwrap().get_ref().clone()
    }
}

impl Seek for TestFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.0.write().unwrap().seek(pos)
    }
}

impl Read for TestFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.write().unwrap().read(buf)
    }
}

impl Write for TestFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ReadAt for TestFile {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read().unwrap().get_ref().read_at(pos, buf)
    }
}

impl WriteAt for TestFile {
    fn write_at(&mut self, pos: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write().unwrap().get_mut().write_at(pos, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

mod log_format_test;
