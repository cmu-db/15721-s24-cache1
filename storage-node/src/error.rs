use aws_sdk_s3::primitives::ByteStreamError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParpulseError {
    #[error("Disk error: {0}")]
    Disk(#[source] std::io::Error),
    #[error("S3 error: {0}")]
    S3(#[source] Box<dyn std::error::Error>),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<std::io::Error> for ParpulseError {
    fn from(e: std::io::Error) -> Self {
        ParpulseError::Disk(e)
    }
}

impl<E, R> From<aws_smithy_runtime_api::client::result::SdkError<E, R>> for ParpulseError
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    fn from(e: aws_smithy_runtime_api::client::result::SdkError<E, R>) -> Self {
        ParpulseError::S3(Box::new(e))
    }
}

impl From<ByteStreamError> for ParpulseError {
    fn from(e: ByteStreamError) -> Self {
        ParpulseError::Internal(e.to_string())
    }
}

pub type ParpulseResult<T> = std::result::Result<T, ParpulseError>;
