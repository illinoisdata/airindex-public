use derive_more::Display;
use std::error::Error;

pub type GenericError = Box<dyn Error + Send + Sync>;
pub type GResult<T> = Result<T, GenericError>;


/* Storage errors */

#[derive(Display, Debug, Clone)]
pub struct UrlParseFilePathError;
impl Error for UrlParseFilePathError {}
unsafe impl Send for UrlParseFilePathError {}
unsafe impl Sync for UrlParseFilePathError {}

#[derive(Display, Debug, Clone)]
#[display(fmt = "Failed to open {}, due to {}", path, reason)]
pub struct OpenUrlError {
  path: String,
  reason: String,
}
impl OpenUrlError {
  pub fn boxed(path: String, reason: String) -> GenericError {
    Box::new(OpenUrlError { path, reason })
  }
}
impl Error for OpenUrlError {}
unsafe impl Send for OpenUrlError {}
unsafe impl Sync for OpenUrlError {}


#[derive(Display, Debug, Clone)]
pub struct MissingAzureAuthetication {
  reason: String,
}
impl MissingAzureAuthetication {
  pub fn boxed(reason: &str) -> GenericError {
    Box::new(MissingAzureAuthetication { reason: reason.to_string() })
  }
}
impl Error for MissingAzureAuthetication {}
unsafe impl Send for MissingAzureAuthetication {}
unsafe impl Sync for MissingAzureAuthetication {}


#[derive(Display, Debug, Clone)]
pub struct InvalidAzureStorageUrl {
  reason: String,
}
impl InvalidAzureStorageUrl {
  pub fn new(reason: &str) -> InvalidAzureStorageUrl {
    InvalidAzureStorageUrl { reason: reason.to_string() }
  }
}
impl Error for InvalidAzureStorageUrl {}
unsafe impl Send for InvalidAzureStorageUrl {}
unsafe impl Sync for InvalidAzureStorageUrl {}


/* External Store */

#[derive(Display, Debug, Clone)]
pub struct ConflictingStorageScheme {
  scheme: String,
}
impl ConflictingStorageScheme {
  pub fn boxed(scheme: &str) -> GenericError {
    Box::new(ConflictingStorageScheme { scheme: scheme.to_string() })
  }
}
impl Error for ConflictingStorageScheme {}
unsafe impl Send for ConflictingStorageScheme {}
unsafe impl Sync for ConflictingStorageScheme {}


#[derive(Display, Debug, Clone)]
#[display(fmt = "Requested {}, only {:?} supported", scheme, supported)]
pub struct UnavailableStorageScheme {
  scheme: String,
  supported: Vec<String>,
}
impl UnavailableStorageScheme {
  pub fn boxed(scheme: String, supported: Vec<String>) -> GenericError {
    Box::new(UnavailableStorageScheme { scheme, supported: supported.to_vec() })
  }
}
impl Error for UnavailableStorageScheme {}
unsafe impl Send for UnavailableStorageScheme {}
unsafe impl Sync for UnavailableStorageScheme {}


/* Stores */

#[derive(Display, Debug, Clone)]
pub struct IncompleteDataStoreFromMeta {
  reason: String,
}
impl IncompleteDataStoreFromMeta {
  pub fn boxed(reason: &str) -> GenericError {
    Box::new(IncompleteDataStoreFromMeta { reason: reason.to_string() })
  }
}
impl Error for IncompleteDataStoreFromMeta {}
unsafe impl Send for IncompleteDataStoreFromMeta {}
unsafe impl Sync for IncompleteDataStoreFromMeta {}


/* Index */

#[derive(Debug, Clone)]
pub struct OutofCoverageError;

impl std::fmt::Display for OutofCoverageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "submodels in the predicted range does not cover the given key")
    }
}

impl Error for OutofCoverageError {}
unsafe impl Send for OutofCoverageError {}
unsafe impl Sync for OutofCoverageError {}