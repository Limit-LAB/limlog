#![feature(once_cell)]

pub mod appender;

pub mod formats;

pub use appender::LogAppender;
pub use formats::log::Log;

use std::{fs, path::Path};

#[derive(Clone, Copy, Debug)]
struct LogEntry {
    id: u64,
    ts: u64,
}

fn log_groups(log_dir: impl AsRef<Path>) -> Vec<LogEntry> {
    let Ok(dirs) = fs::read_dir(log_dir.as_ref()) else {
        return Vec::new();
    };

    dirs.into_iter()
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();

            (path.is_file() && path.extension().unwrap_or_default().eq("limlog")).then_some(())?;

            let name = path.file_name()?.to_str()?;
            let ret = name.split_once('_').and_then(|(id, ts)| {
                Some(LogEntry {
                    id: id.parse::<u64>().ok()?,
                    ts: ts.parse::<u64>().ok()?,
                })
            })?;

            (log_dir.as_ref().join(format!("{name}.idx")).is_file()
                && log_dir.as_ref().join(format!("{name}.ts.idx")).is_file())
            .then_some(ret)
        })
        .collect()
}

#[cfg(test)]
mod tests;
