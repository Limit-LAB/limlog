use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ErrorType {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid reader offset, maximum {maximum}, got {got}")]
    InvalidOffset { maximum: usize, got: usize },

    #[error("Channel sending error: {0}")]
    KanalSend(#[from] kanal::SendError),

    #[error("Channel receive error: {0}")]
    KanalRecv(#[from] kanal::ReceiveError),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Invalid file header")]
    InvalidHeader,

    #[error("{0} is not a directory")]
    BadWorkDir(PathBuf),

    #[error("Invalid log index file: zero-sized header")]
    EmptyIndexFile,

    #[error("Log index file too small")]
    IndexFileTooSmall,

    #[error("Log index file is full")]
    LogFileFull,
}

pub type Result<T, E = ErrorType> = std::result::Result<T, E>;
