use thiserror::Error;

/// The Limlog error type.
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
}

/// A specialized [`Result`] type for Limlog.
pub type Result<T, E = ErrorType> = std::result::Result<T, E>;
