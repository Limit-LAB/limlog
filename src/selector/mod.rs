pub(crate) mod index_reader;
pub(crate) mod log_reader;

use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    thread,
};

use anyhow::{ensure, Result};
use kanal::{bounded, unbounded, Receiver, Sender};
use uuid7::Uuid;

use self::{index_reader::IndexReader, log_reader::LogReader};
use crate::{
    util::{log_groups, ts_to_uuid},
    Log,
};

#[derive(Copy, Clone, Debug)]
pub enum SelectRange {
    Timestamp(u64, u64),
    Uuid(Uuid, Uuid),
}

impl SelectRange {
    fn to_uuid_range(&self) -> (Uuid, Uuid) {
        match *self {
            SelectRange::Timestamp(start, end) => (ts_to_uuid(start, 0), ts_to_uuid(end, 0xFF)),
            SelectRange::Uuid(start, end) => (start, end),
        }
    }
}

pub type SelectResult = Receiver<Vec<Log>>;

#[derive(Clone, Debug)]
pub struct LogSelector {
    groups: Vec<Uuid>,
    sender: Sender<(Vec<Uuid>, SelectRange, Sender<Vec<Log>>)>,
}

impl LogSelector {
    /// Create a new [LogSelector].
    pub fn new(path: impl AsRef<Path>) -> Result<LogSelector> {
        let mut groups = log_groups(path.as_ref());
        ensure!(!groups.is_empty(), "Empty log directory");
        // for match the last log group
        groups.push(Uuid::MAX);
        groups.sort();

        let (sender, receiver) = unbounded();

        let inner = LogSelectorInner {
            readers: BTreeMap::new(),
            receiver,
            work_dir: path.as_ref().to_path_buf(),
        };
        thread::spawn(move || inner.exec());

        Ok(Self { groups, sender })
    }

    /// Select range by log ID or log timestamp
    pub fn select_range(&self, range: SelectRange) -> Result<SelectResult> {
        // find the log group that intersect with the range
        let range_groups = self
            .groups
            .windows(2)
            .filter_map(|w| {
                let (start, end) = range.to_uuid_range();
                (start <= w[1] && end >= w[0]).then_some(w[0])
            })
            .collect::<Vec<_>>();

        let (sender, receiver) = bounded(1);

        if range_groups.is_empty() {
            sender.send(Vec::new())?;
            return Ok(receiver);
        }

        // submit a task to worker thread
        self.sender.send((range_groups, range, sender))?;

        Ok(receiver)
    }
}

#[derive(Debug)]
struct LogSelectorInner {
    work_dir: PathBuf,
    readers: BTreeMap<Uuid, ReaderSet>,
    receiver: Receiver<(Vec<Uuid>, SelectRange, Sender<Vec<Log>>)>,
}

#[derive(Debug)]
struct ReaderSet {
    log: LogReader<File>,
    idx: IndexReader<File>,
}

impl LogSelectorInner {
    // Runs on selector worker thread
    fn exec(mut self) -> Result<()> {
        while let Ok((
            range_groups, // groups in range
            range,        // select range
            sender,       // sender of result channel returned before
        )) = self.receiver.recv()
        {
            let mut res = Vec::with_capacity(128);

            // select in each group
            for group in range_groups {
                // Create new reader set if not exist
                let set = match self.readers.get_mut(&group) {
                    Some(set) => set,
                    None => {
                        let reader = self.create_reader_set(&group).unwrap();
                        self.readers.entry(group).or_insert(reader)
                    }
                };

                let (start, end) = range.to_uuid_range();
                let Some((begin, count)) = set
                        .idx
                        .select_range(&start, &end)?
                        .map(|(idx, count)| (idx.offset, count))
                 else { continue };

                res.extend(set.log.select_logs(begin, count).unwrap());
            }

            sender.send(res).unwrap();
        }

        Ok(())
    }

    // Create new reader set if not exist
    fn create_reader_set(&self, group: &Uuid) -> Result<ReaderSet> {
        let file_name = group.to_string();

        Ok(ReaderSet {
            log: LogReader::new(File::open(
                self.work_dir.join(format!("{file_name}.limlog")),
            )?)?,
            idx: IndexReader::new(File::open(self.work_dir.join(format!("{file_name}.idx")))?)?,
        })
    }
}
