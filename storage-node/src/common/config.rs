use clap::Parser;
use serde::Serialize;

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ParpulseConfigDataStore {
    #[default]
    Memdisk,
    Disk,
    Sqlite,
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ParpulseConfigCachePolicy {
    #[default]
    Lru,
    Lruk,
}

#[derive(Parser)]
pub struct ParpulseConfig {
    #[clap(short, long, default_value_t, value_enum)]
    pub cache_policy: ParpulseConfigCachePolicy,

    #[clap(short, long, default_value = None)]
    pub cache_lru_k: Option<usize>,

    #[clap(short, long, default_value_t, value_enum)]
    pub data_store: ParpulseConfigDataStore,

    #[clap(short, long, default_value = "3")]
    pub data_store_cache_num: usize,

    #[clap(short, long, default_value = None)]
    pub mem_cache_size: Option<usize>,

    #[clap(short, long, default_value = None)]
    pub disk_cache_size: Option<usize>,

    #[clap(short, long, default_value = None)]
    pub cache_path: Option<String>,
}
