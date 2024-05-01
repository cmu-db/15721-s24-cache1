pub mod client;

use enum_as_inner::EnumAsInner;
use serde::Deserialize;

#[derive(Clone, EnumAsInner, Debug)]
pub enum RequestParams {
    /// S3 bucket and keys.
    S3((String, Vec<String>)),
    /// Mock S3 bucket and keys.
    /// This is used for testing purposes.
    MockS3((String, Vec<String>)),
}

#[derive(Deserialize)]
pub struct S3Request {
    pub bucket: String,
    /// Cannot deserialize a vector of strings, might need to customize a deserializer later.
    pub keys: String,
    #[serde(default, rename = "request-id")]
    pub request_id: usize,
    #[serde(default)]
    pub is_test: bool,
}
