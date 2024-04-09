use enum_as_inner::EnumAsInner;

#[derive(Clone, EnumAsInner, Debug)]
pub enum RequestParams {
    File(String),
    /// S3 bucket and keys.
    S3((String, Vec<String>)),
}

/// Initialize the logger for tests
pub fn init_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
}
