use std::{marker::PhantomData, thread};

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use kanal::{unbounded, Receiver, Sender};
use smallvec::SmallVec;

use crate::{checker::IndexChecker, formats::log::UuidIndex, util::BlockIODevice, STACK_BUF_SIZE};

// Writer for UuidIndex
#[derive(Debug)]
pub(crate) struct IndexWriter<F> {
    sender: Sender<SmallVec<[UuidIndex; STACK_BUF_SIZE]>>,
    phantom: PhantomData<F>,
}

impl<F: BlockIODevice> IndexWriter<F> {
    pub(crate) fn new(mut file: F) -> Result<Self> {
        let mut file_size = file.len()?;
        IndexChecker::check(&mut file, &mut file_size).or_init()?;

        let (sender, receiver) = unbounded();
        let inner = IndexWriterInner { file, receiver };
        thread::spawn(move || inner.exec());

        Ok(Self {
            sender,
            phantom: PhantomData,
        })
    }

    pub(crate) fn append_log_indexes(
        &self,
        indexes: SmallVec<[UuidIndex; STACK_BUF_SIZE]>,
    ) -> Result<()> {
        // submit a task to worker
        Ok(self.sender.send(indexes)?)
    }
}

struct IndexWriterInner<F> {
    file: F,
    receiver: Receiver<SmallVec<[UuidIndex; STACK_BUF_SIZE]>>,
}

impl<F: BlockIODevice> IndexWriterInner<F> {
    fn exec(mut self) -> Result<()> {
        // 4k buf
        let mut buf = BytesMut::with_capacity(1 >> 12).writer();
        let item_size =
            bincode::serialized_size(&UuidIndex::default()).expect("Serialization failed") as usize;

        while let Ok(indexes) = self.receiver.recv() {
            buf.get_mut().reserve(indexes.len() * item_size);

            for index in indexes {
                bincode::serialize_into(&mut buf, &index).expect("Serialization failed");
            }

            self.file.write_all(&buf.get_ref())?;
            self.file.sync_data()?;

            buf.get_mut().clear();
        }

        Ok(())
    }
}
