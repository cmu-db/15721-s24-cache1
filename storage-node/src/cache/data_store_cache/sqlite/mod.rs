use sqlite::Connection;
use tokio::sync::RwLock;

use crate::cache::policy::DataStoreReplacer;

pub struct SqliteStoreCache<R: DataStoreReplacer> {
    replacer: RwLock<R>,
    connection: Connection,
}
