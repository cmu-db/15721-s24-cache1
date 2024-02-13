pub mod client;

use arrow::record_batch::RecordBatch;
use datafusion::{error::DataFusionError, physical_plan::SendableRecordBatchStream};

/// Result type that the storage node would return to the execution engine. We use
/// `DataFusionError` here to align with the execution engine.
pub type StorageResult<T> = anyhow::Result<T, DataFusionError>;

/// Id types for table, column, and record. Need to be consistent among all components
/// (e.g. execution engine). We don't want to make any type generic here just for the id,
/// so we simply define them here. Might refine later.
pub type TableId = u64;
pub type ColumnId = u64;
pub type RecordId = u64;

/// [`StorageRequest`] specifies the requests that the execution engine might issue to
/// the storage node.
///
/// Currently we assume the execution engine only requests the whole table/column. We may
/// add `std::ops::RangeBounds` later to support range query from the execution engine.
pub enum StorageRequest {
    /// Requests a whole table from the underlying storage.
    Table(TableId),
    /// Requests one or more columns from the underlying storage.
    Columns(TableId, Vec<ColumnId>),
    /// Requests one or more tuples from the underlying storage.
    /// FIXME: Do we really need this?
    Tuple(Vec<RecordId>),
}

/// [`StorageClient`] provides the interface for the execution engine to query data from the
/// storage node. It resolves the physical location of the tables/columns/tuples by querying
/// the catalog node, and then sends the request to the storage node to get the data from the
/// underlying storage.
#[async_trait::async_trait]
pub trait StorageClient: Send + Sync + 'static {
    /// Returns the requested data as a stream.
    ///
    /// FIXME: Should we use `StorageResult<mpsc::Receiver<RecordBatch>>` instead?
    async fn request_data(
        &self,
        request: StorageRequest,
    ) -> StorageResult<SendableRecordBatchStream>;

    /// Returns all the requested data as a whole.
    async fn request_data_sync(&self, request: StorageRequest) -> StorageResult<Vec<RecordBatch>>;
}
