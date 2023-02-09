use anyhow::Result;
use kanal::{unbounded, Receiver, Sender};
use std::{marker::PhantomData, mem::size_of, slice::from_raw_parts, thread};

use crate::{
    checker::IndexChecker,
    formats::log::IndexFileHeader,
    util::{BlockIODevice, LogItem},
};

#[derive(Debug)]
pub(crate) struct IndexWriter<F, I> {
    sender: Sender<Vec<I>>,
    phantom: PhantomData<F>,
}

impl<F: BlockIODevice, I: LogItem> IndexWriter<F, I> {
    pub(crate) fn new(mut file: F, expected_header: IndexFileHeader) -> Result<Self> {
        let mut file_size = file.len()?;
        IndexChecker::check::<I>(&mut file, &mut file_size, expected_header).or_init()?;

        let (sender, receiver) = unbounded();
        let inner = IndexWriterInner { file, receiver };
        thread::spawn(move || inner.exec());

        Ok(Self {
            sender,
            phantom: PhantomData,
        })
    }

    pub(crate) fn append_log_indexes(&self, indexes: Vec<I>) -> Result<()> {
        // submit a task to worker
        Ok(self.sender.send(indexes)?)
    }
}

struct IndexWriterInner<F, T> {
    file: F,
    receiver: Receiver<Vec<T>>,
}

impl<F: BlockIODevice, T: LogItem> IndexWriterInner<F, T> {
    fn exec(mut self) -> Result<()> {
        while let Ok(indexes) = self.receiver.recv() {
            // let mut buf = Vec::with_capacity(256);

            // for index in indexes {
            //     let bytes = bincode::serialize(&index).unwrap();
            //     buf.write_all(&bytes).unwrap();
            // }

            let buf = unsafe {
                from_raw_parts(
                    indexes.as_ptr() as *const u8,
                    indexes.len() * size_of::<T>(),
                )
            };

            self.file.write_all(&buf)?;
            self.file.sync_data()?;
        }

        Ok(())
    }
}
