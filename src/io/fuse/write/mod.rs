//! APIs to write to Arrow's IPC format.
pub(crate) mod common;
mod serialize;
pub(crate) mod writer;

pub use common::{Compression, WriteOptions};
pub use serialize::write;
pub use writer::FuseWriter;

pub(crate) mod common_sync;
