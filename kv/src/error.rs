use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    DatabaseError(String),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("zerocopy error: {0}")]
    ZerocopyError(String),

    #[error("transaction was invalidated by an earlier operation failure")]
    TransactionFailed,
}

impl<A, S, V> From<zerocopy::ConvertError<A, S, V>> for Error {
    fn from(_: zerocopy::ConvertError<A, S, V>) -> Self {
        Error::ZerocopyError("Zerocopy conversion error".to_string())
    }
}

impl<Src, Dst> From<zerocopy::SizeError<Src, Dst>> for Error {
    fn from(_: zerocopy::SizeError<Src, Dst>) -> Self {
        Error::ZerocopyError("Zerocopy size error".to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
