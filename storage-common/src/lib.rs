use enum_as_inner::EnumAsInner;
use serde::Deserialize;

#[derive(Clone, EnumAsInner, Debug)]
pub enum RequestParams {
    File(String),
    /// S3 bucket and keys.
    S3((String, Vec<String>)),
}

#[derive(Deserialize)]
pub struct S3Request {
    pub bucket: String,
    /// Cannot deserialize a vector of strings, might need to customize a deserializer later.
    pub keys: String,
}

/// Initialize the logger
pub fn init_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
}
