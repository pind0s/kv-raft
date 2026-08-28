mod read_transaction;
mod write_transaction;

pub(crate) type TransactionId = u64;
pub use read_transaction::ReadTransaction;
pub use write_transaction::WriteTransaction;

pub(crate) use read_transaction::ReadTracker;
