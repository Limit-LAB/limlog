use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]

pub enum ErrorType {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel sending error: {0}")]
    KanalSend(#[from] kanal::SendError),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Invalid file header")]
    InvalidHeader,

    #[error("{0} is not a directory")]
    BadWorkDir(PathBuf),

    #[error("Invalid log index file: zero-sized header")]
    EmptyIndexFile,
}

pub type Result<T> = std::result::Result<T, ErrorType>;
