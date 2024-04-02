use enum_as_inner::EnumAsInner;

#[derive(Clone, EnumAsInner, Debug)]
pub enum RequestParams {
    File(String),
    /// S3 bucket and keys.
    S3((String, Vec<String>)),
}
