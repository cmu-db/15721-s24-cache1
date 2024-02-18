// TODO: Remove these annotations after implementation.
#![allow(unused)]
#![allow(clippy::new_without_default)]

pub mod cache;
pub mod disk;
pub mod server;
pub mod storage_manager;
pub mod storage_reader;

use datafusion::error::DataFusionError;

// TODO: Do we define our own error type?
pub type StorageResult<T> = anyhow::Result<T, DataFusionError>;
