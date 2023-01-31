use anyhow::{anyhow, ensure, Result};
use crossbeam::channel::{self, Receiver, Sender};
use serde::Serialize;
use std::{
    fmt::Display,
    fs::File,
    io::Write,
    mem::size_of,
    path::Path,
    thread::{self, JoinHandle},
};

#[derive(Debug)]
pub struct IndexWriter<T: Serialize + Send + Sync + 'static> {
    handle: JoinHandle<Result<()>>,
    sender: Sender<Vec<T>>,
}

impl<T: Serialize + Send + Sync + 'static> IndexWriter<T> {
    pub fn new(
        path: impl AsRef<Path>,
        file_name: impl Display,
        suffix: impl Display,
    ) -> Result<Self> {
        let (sender, receiver) = channel::bounded(8);
        let file = File::options()
            .append(true)
            .open(path.as_ref().join(format!("{file_name}.{suffix}")))?;

        let file_size = file.metadata()?.len();
        ensure!(
            file_size % size_of::<T>() as u64 == 0,
            "Invalid log index file"
        );

        Ok(Self {
            handle: thread::spawn(move || Self::exec(file, receiver)),
            sender,
        })
    }

    pub fn append_log_indexes(&self, indexes: Vec<T>) -> Result<()> {
        if self.handle.is_finished() {
            Err(anyhow!("Worker thread already exited"))?;
        }

        self.sender.send(indexes)?;
        Ok(())
    }

    fn exec(mut index_file: File, receiver: Receiver<Vec<T>>) -> Result<()> {
        while let Ok(indexes) = receiver.recv() {
            let mut buf = Vec::with_capacity(256);

            for index in indexes {
                let bytes = bincode::serialize(&index).unwrap();
                buf.write_all(&bytes).unwrap();
            }

            index_file.write_all(&buf)?;
            index_file.sync_data()?;
        }

        Ok(())
    }
}
