use std::{marker::PhantomData, mem::size_of, thread};

use bytes::{BufMut, BytesMut};
use kanal::{unbounded, Receiver, Sender};
use smallvec::SmallVec;

use crate::{
    checker::IndexChecker,
    formats::log::IndexFileHeader,
    util::{BlockIODevice, IndexItem},
    Result, STACK_BUF_SIZE,
};

#[derive(Debug)]
pub(crate) struct IndexWriter<F, I> {
    sender: Sender<SmallVec<[I; STACK_BUF_SIZE]>>,
    phantom: PhantomData<F>,
}

impl<F: BlockIODevice, I: IndexItem> IndexWriter<F, I> {
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

impl<F: BlockIODevice, I: IndexItem> IndexWriterInner<F, I> {
    fn exec(mut self) -> Result<()> {
        // 4k buf
        let mut buf = BytesMut::with_capacity(1 >> 12).writer();

        while let Ok(indexes) = self.receiver.recv() {
            buf.get_mut()
                .reserve(indexes.len() as usize * size_of::<I>());

            for index in indexes {
                bincode::serialize_into(&mut buf, &index).expect("Serialization failed");
            }

            // let buf = unsafe {
            //     from_raw_parts(
            //         indexes.as_ptr() as *const u8,
            //         indexes.len() * size_of::<T>(),
            //     )
            // };

            self.file.write_all(&buf.get_ref())?;
            self.file.sync_data()?;

            buf.get_mut().clear();
        }

        Ok(())
    }
}
