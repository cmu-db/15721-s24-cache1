use enum_as_inner::EnumAsInner;

#[derive(Clone, EnumAsInner)]
pub enum RequestParams {
    File(String),
    S3(String),
}
