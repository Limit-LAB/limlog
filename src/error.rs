use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]

pub enum ErrorType {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel sending error: {0}")]
    KanalSend(#[from] kanal::SendError),

    #[error("Bincode error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("Error: {0}")]
    AdHoc(String),

    #[error("Invalid file header")]
    InvalidHeader,

    #[error("{0} is not a directory")]
    BadWorkDir(PathBuf),

    #[error("Invalid log index file: zero-sized header")]
    EmptyIndexFile,
}

pub type Result<T> = std::result::Result<T, ErrorType>;

macro_rules! ensure {
    ($pred:expr) => {
        if !$pred {
            return Err($crate::ErrorType::AdHoc(format!(
                "Assertion failed: {}",
                stringify!($pred)
            )));
        }
    };

    ($pred:expr, $($msg:tt)*) => {
        if !$pred {
            return Err($crate::ErrorType::AdHoc(format!($($msg)*)));
        }
    };
}

pub(crate) use ensure;
