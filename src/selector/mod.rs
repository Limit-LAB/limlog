mod index_reader;
mod log_reader;

use std::{
    collections::BTreeMap,
    fs::File,
    path::{Path, PathBuf},
    thread,
};

use anyhow::{ensure, Result};
use kanal::{bounded, unbounded, Receiver, Sender};

use crate::{
    formats::log::{Index, Timestamp, INDEX_HEADER, TS_INDEX_HEADER},
    util::{log_groups, LogGroup},
    Log,
};

use self::{index_reader::IndexReader, log_reader::LogReader};

#[derive(Copy, Clone, Debug)]
pub enum SelectRange {
    Timestamp(u64, u64),
    Id(u64, u64),
}

pub type SelectResult = Receiver<Vec<Log>>;

#[derive(Clone, Debug)]
pub struct LogSelector {
    groups: Vec<LogGroup>,
    sender: Sender<(Vec<LogGroup>, SelectRange, Sender<Vec<Log>>)>,
}

impl LogSelector {
    pub fn new(path: impl AsRef<Path>) -> Result<LogSelector> {
        let groups = log_groups(path.as_ref());
        ensure!(!groups.is_empty(), "Empty log directory");

        let (sender, receiver) = unbounded();

        let inner = LogSelectorInner {
            readers: BTreeMap::new(),
            receiver,
            work_dir: path.as_ref().to_path_buf(),
        };
        thread::spawn(move || inner.exec());

        Ok(Self { groups, sender })
    }

    pub fn select_range(&self, range: SelectRange) -> Result<SelectResult> {
        let range_groups = self.groups.windows(2);
        let range_groups = match range {
            SelectRange::Timestamp(start, end) => range_groups
                .filter_map(|w| {
                    let range = w[0].ts..w[1].ts;
                    (range.contains(&start) || range.contains(&end)).then_some(w[0])
                })
                .collect::<Vec<_>>(),
            SelectRange::Id(start, end) => range_groups
                .filter_map(|w| {
                    let range = w[0].id..w[1].id;
                    (range.contains(&start) || range.contains(&end)).then_some(w[0])
                })
                .collect::<Vec<_>>(),
        };

        let (sender, receiver) = bounded(1);

        if range_groups.is_empty() {
            sender.send(Vec::new())?;
            return Ok(receiver);
        }

        self.sender.send((range_groups, range, sender))?;

        Ok(receiver)
    }
}

#[derive(Debug)]
struct LogSelectorInner {
    work_dir: PathBuf,
    readers: BTreeMap<LogGroup, ReaderSet>,
    receiver: Receiver<(Vec<LogGroup>, SelectRange, Sender<Vec<Log>>)>,
}

#[derive(Debug)]
struct ReaderSet {
    log: LogReader<File>,
    idx: IndexReader<File>,
    ts_idx: IndexReader<File>,
}

impl LogSelectorInner {
    fn exec(mut self) -> Result<()> {
        while let Ok((range_groups, range, sender)) = self.receiver.recv() {
            let mut res = Vec::with_capacity(128);
            for group in range_groups {
                let set = match self.readers.get_mut(&group) {
                    Some(set) => set,
                    None => {
                        let reader = self.create_reader_set(group)?;
                        self.readers.entry(group).or_insert(reader)
                    }
                };

                let Some((start, count)) = (match range {
                    SelectRange::Timestamp(start, end) => set
                        .ts_idx
                        .select_range(&Timestamp(start, 0), &Timestamp(end, 0))?
                        .map(|(ts_idx, count)| (ts_idx.1, count)),
                    SelectRange::Id(start, end) => set
                        .idx
                        .select_range(&Index(start, 0), &Index(end, 0))?
                        .map(|(idx, count)| (idx.1, count)),
                }) else { continue };

                res.extend(set.log.select_logs(start, count)?);
            }

            sender.send(res)?;
        }

        Ok(())
    }

    fn create_reader_set(&self, group: LogGroup) -> Result<ReaderSet> {
        let file_name = format!("{}_{}", group.id, group.ts);

        Ok(ReaderSet {
            log: LogReader::new(File::open(
                self.work_dir.join(format!("{file_name}.limlog")),
            )?)?,
            idx: IndexReader::new::<Index>(
                File::open(self.work_dir.join(format!("{file_name}.idx")))?,
                INDEX_HEADER,
            )?,
            ts_idx: IndexReader::new::<Timestamp>(
                File::open(self.work_dir.join(format!("{file_name}.ts.idx")))?,
                TS_INDEX_HEADER,
            )?,
        })
    }
}
