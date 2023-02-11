use std::{marker::PhantomData, thread};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use kanal::{unbounded, Receiver, Sender};
use smallvec::SmallVec;

use crate::{
    checker::IndexChecker,
    formats::log::IndexFileHeader,
    util::{BlockIODevice, LogItem},
    STACK_BUF_SIZE,
};

#[derive(Debug)]
pub(crate) struct IndexWriter<F, I> {
    sender: Sender<SmallVec<[I; STACK_BUF_SIZE]>>,
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

    pub(crate) fn append_log_indexes(&self, indexes: SmallVec<[I; STACK_BUF_SIZE]>) -> Result<()> {
        // submit a task to worker
        Ok(self.sender.send(indexes)?)
    }
}

struct IndexWriterInner<F, I> {
    file: F,
    receiver: Receiver<SmallVec<[I; STACK_BUF_SIZE]>>,
}

impl<F: BlockIODevice, T: LogItem> IndexWriterInner<F, T> {
    fn exec(mut self) -> Result<()> {
        // 4k buffer
        let mut buf = BytesMut::with_capacity(1 << 12);

        while let Ok(indexes) = self.receiver.recv() {
            let mut w = buf.writer();

            for index in indexes {
                let size = bincode::serialized_size(&index).expect("Serialization failed");
                w.get_mut().reserve(size as usize);
                bincode::serialize_into(&mut w, &index).expect("Serialization failed");
            }

            buf = w.into_inner();

            // Get written bytes from buffer
            let bytes = buf.split().freeze();

            // let buf = unsafe {
            //     from_raw_parts(
            //         indexes.as_ptr() as *const u8,
            //         indexes.len() * size_of::<T>(),
            //     )
            // };

            self.file.write_all(&bytes)?;
            self.file.sync_data()?;
        }

        Ok(())
    }
}
