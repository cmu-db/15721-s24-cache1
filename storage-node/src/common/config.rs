use clap::Parser;
use serde::Serialize;

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize)]
pub enum ParpulseConfigDataStore {
    #[default]
    Memdisk,
    Disk,
    Sqlite,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize)]
pub enum ParpulseConfigCachePolicy {
    #[default]
    Lru,
    Lruk,
}

#[derive(Parser, Default)]
pub struct ParpulseConfig {
    #[clap(long, default_value_t, value_enum)]
    pub cache_policy: ParpulseConfigCachePolicy,

    #[clap(long, default_value = None)]
    pub cache_lru_k: Option<usize>,

    #[clap(long, default_value_t, value_enum)]
    pub data_store: ParpulseConfigDataStore,

    #[clap( long, default_value = None)]
    pub data_store_cache_num: Option<usize>,

    #[clap(long, default_value = None)]
    pub mem_cache_size: Option<usize>,

    #[clap(long, default_value = None)]
    pub disk_cache_size: Option<usize>,

    #[clap(long, default_value = None)]
    pub sqlite_cache_size: Option<usize>,

    #[clap( long, default_value = None)]
    pub cache_path: Option<String>,
}
