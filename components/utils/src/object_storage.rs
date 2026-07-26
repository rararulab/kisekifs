// Copyright 2024 kisekifs
//
// JuiceFS, Copyright 2020 Juicedata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt::{Debug, Formatter},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};
use object_store::{
    GetResult, ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, PutResult,
    aws::AmazonS3Builder, buffered::BufWriter, path::Path as StoragePath, prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio::io::AsyncWrite;
use url::Url;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectStorageConfig {
    Memory,
    File {
        root: PathBuf,
    },
    S3 {
        bucket:     String,
        prefix:     Option<String>,
        region:     Option<String>,
        endpoint:   Option<String>,
        allow_http: bool,
    },
}

impl Debug for ObjectStorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory => f
                .debug_struct("ObjectStorageConfig")
                .field("provider", &"memory")
                .finish(),
            Self::File { root } => f
                .debug_struct("ObjectStorageConfig")
                .field("provider", &"file")
                .field("root", root)
                .finish(),
            Self::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                allow_http,
            } => f
                .debug_struct("ObjectStorageConfig")
                .field("provider", &"s3")
                .field("bucket", bucket)
                .field("prefix", prefix)
                .field("region_configured", &region.is_some())
                .field("endpoint_configured", &endpoint.is_some())
                .field("allow_http", allow_http)
                .finish(),
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ObjectStorageConfigError {
    #[snafu(display("invalid object storage DSN: {reason}"))]
    InvalidDsn { reason: String },

    #[snafu(display("failed to build object storage backend"))]
    BuildObjectStore { source: object_store::Error },
}

impl FromStr for ObjectStorageConfig {
    type Err = ObjectStorageConfigError;

    fn from_str(dsn: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(dsn).map_err(|error| invalid_dsn(error.to_string()))?;
        reject_url_secrets(&url)?;

        match url.scheme() {
            "memory" => parse_memory_config(&url),
            "file" => parse_file_config(&url),
            "s3" => parse_s3_config(&url),
            _ => Err(invalid_dsn("unsupported object storage scheme")),
        }
    }
}

impl ObjectStorageConfig {
    pub fn build(&self) -> Result<ObjectStorage, ObjectStorageConfigError> {
        match self {
            Self::Memory => Ok(new_memory_object_store()),
            Self::File { root } => new_local_object_store(root).context(BuildObjectStoreSnafu),
            Self::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_allow_http(*allow_http);
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let store = builder.build().context(BuildObjectStoreSnafu)?;
                match prefix {
                    Some(prefix) => {
                        let prefix = StoragePath::parse(prefix)
                            .map_err(object_store::Error::from)
                            .context(BuildObjectStoreSnafu)?;
                        Ok(ObjectStorage::new(PrefixStore::new(store, prefix)))
                    }
                    None => Ok(ObjectStorage::new(store)),
                }
            }
        }
    }

    pub const fn provider(&self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File { .. } => "file",
            Self::S3 { .. } => "s3",
        }
    }

    pub fn bucket(&self) -> Option<&str> {
        match self {
            Self::S3 { bucket, .. } => Some(bucket),
            _ => None,
        }
    }

    pub fn prefix(&self) -> Option<&str> {
        match self {
            Self::S3 { prefix, .. } => prefix.as_deref(),
            _ => None,
        }
    }
}

fn invalid_dsn(reason: impl Into<String>) -> ObjectStorageConfigError {
    ObjectStorageConfigError::InvalidDsn {
        reason: reason.into(),
    }
}

fn reject_url_secrets(url: &Url) -> Result<(), ObjectStorageConfigError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid_dsn("embedded credentials are not allowed"));
    }

    for (key, _) in url.query_pairs() {
        let key = key.to_ascii_lowercase();
        if [
            "key",
            "secret",
            "token",
            "credential",
            "password",
            "signature",
        ]
        .iter()
        .any(|marker| key.contains(marker))
        {
            return Err(invalid_dsn("credential parameters are not allowed"));
        }
    }
    Ok(())
}

fn reject_query_and_fragment(url: &Url) -> Result<(), ObjectStorageConfigError> {
    if url.query().is_some() || url.fragment().is_some() {
        return Err(invalid_dsn(
            "query parameters and fragments are not supported",
        ));
    }
    Ok(())
}

fn parse_memory_config(url: &Url) -> Result<ObjectStorageConfig, ObjectStorageConfigError> {
    reject_query_and_fragment(url)?;
    if url.host_str().is_some() || !matches!(url.path(), "" | "/") {
        return Err(invalid_dsn("memory DSN must be exactly memory://"));
    }
    Ok(ObjectStorageConfig::Memory)
}

fn parse_file_config(url: &Url) -> Result<ObjectStorageConfig, ObjectStorageConfigError> {
    reject_query_and_fragment(url)?;
    if url.host_str().is_some() {
        return Err(invalid_dsn("file DSN must not contain a host"));
    }
    let root = url
        .to_file_path()
        .map_err(|_| invalid_dsn("file DSN must contain an absolute path"))?;
    if !root.is_absolute() {
        return Err(invalid_dsn("file DSN must contain an absolute path"));
    }
    Ok(ObjectStorageConfig::File { root })
}

fn parse_s3_config(url: &Url) -> Result<ObjectStorageConfig, ObjectStorageConfigError> {
    if url.fragment().is_some() {
        return Err(invalid_dsn("fragments are not supported"));
    }
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| invalid_dsn("S3 DSN requires a bucket"))?
        .to_string();
    if url.port().is_some() {
        return Err(invalid_dsn("S3 DSN must not contain a port"));
    }

    let raw_prefix = url.path().trim_matches('/');
    let prefix = if raw_prefix.is_empty() {
        None
    } else {
        StoragePath::parse(raw_prefix)
            .map_err(|_| invalid_dsn("S3 DSN contains an invalid prefix"))?;
        Some(raw_prefix.to_string())
    };

    let mut region = None;
    let mut endpoint = None;
    let mut allow_http = false;
    let mut allow_http_configured = false;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "region" if region.is_none() && !value.is_empty() => region = Some(value.into_owned()),
            "endpoint" if endpoint.is_none() && !value.is_empty() => {
                validate_endpoint(&value)?;
                endpoint = Some(value.into_owned());
            }
            "allow_http" if !allow_http_configured => {
                allow_http = value
                    .parse()
                    .map_err(|_| invalid_dsn("allow_http must be true or false"))?;
                allow_http_configured = true;
            }
            "allow_http" => return Err(invalid_dsn("duplicate allow_http option")),
            "region" | "endpoint" => {
                return Err(invalid_dsn("empty or duplicate S3 option"));
            }
            _ => return Err(invalid_dsn("unsupported S3 query parameter")),
        }
    }
    if endpoint
        .as_deref()
        .is_some_and(|endpoint| endpoint.starts_with("http://"))
        && !allow_http
    {
        return Err(invalid_dsn("HTTP endpoint requires allow_http=true"));
    }

    Ok(ObjectStorageConfig::S3 {
        bucket,
        prefix,
        region,
        endpoint,
        allow_http,
    })
}

fn validate_endpoint(endpoint: &str) -> Result<(), ObjectStorageConfigError> {
    let endpoint = Url::parse(endpoint).map_err(|_| invalid_dsn("invalid S3 endpoint"))?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(invalid_dsn("invalid S3 endpoint"));
    }
    Ok(())
}

#[derive(Clone)]
pub struct ObjectStorage {
    inner: Arc<dyn ObjectStore>,
}

pub type LocalStorage = ObjectStorage;

pub type ObjectStorageError = object_store::Error;

pub type ObjectStoragePath = object_store::path::Path;

pub type ObjectReader = object_store::GetResult;

pub type ObjectWriter = Box<dyn AsyncWrite + Unpin + Send>;

impl ObjectStorage {
    fn new(store: impl ObjectStore) -> Self {
        Self {
            inner: Arc::new(store),
        }
    }

    pub async fn put(
        &self,
        path: &StoragePath,
        payload: impl Into<PutPayload>,
    ) -> Result<PutResult, ObjectStorageError> {
        self.inner.put(path, payload.into()).await
    }

    pub async fn get(&self, path: &StoragePath) -> Result<GetResult, ObjectStorageError> {
        self.inner.get(path).await
    }

    pub async fn get_range(
        &self,
        path: &StoragePath,
        range: std::ops::Range<usize>,
    ) -> Result<Bytes, ObjectStorageError> {
        self.inner
            .get_range(path, range.start as u64..range.end as u64)
            .await
    }

    pub async fn delete(&self, path: &StoragePath) -> Result<(), ObjectStorageError> {
        self.inner.delete(path).await
    }

    pub fn list(
        &self,
        prefix: Option<&StoragePath>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStorageError>> {
        self.inner.list(prefix)
    }

    pub fn writer(&self, path: &StoragePath) -> ObjectWriter {
        Box::new(BufWriter::new(Arc::clone(&self.inner), path.clone()))
    }

    pub async fn probe(&self) -> Result<(), ObjectStorageError> {
        probe_list(self.inner.list(None)).await
    }
}

async fn probe_list(
    mut objects: BoxStream<'static, Result<ObjectMeta, ObjectStorageError>>,
) -> Result<(), ObjectStorageError> {
    if let Some(result) = objects.next().await {
        result?;
    }
    Ok(())
}

pub const fn is_not_found_error(e: &ObjectStorageError) -> bool {
    matches!(e, ObjectStorageError::NotFound { .. })
}

pub fn new_memory_object_store() -> ObjectStorage {
    ObjectStorage::new(object_store::memory::InMemory::new())
}

pub fn new_local_object_store<P: AsRef<Path>>(
    path: P,
) -> Result<ObjectStorage, object_store::Error> {
    let path = path.as_ref();
    std::fs::create_dir_all(path).map_err(|source| object_store::Error::Generic {
        store:  "LocalFileSystem",
        source: Box::new(source),
    })?;
    Ok(ObjectStorage::new(
        object_store::local::LocalFileSystem::new_with_prefix(path)?.with_fsync(true),
    ))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bytes::Bytes;
    use object_store::path::Path;
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn basic() {
        let object_sto = new_memory_object_store();

        let bytes = Bytes::from_static(b"hello");
        let path = Path::parse("data/large_file").unwrap();
        object_sto.put(&path, bytes).await.unwrap();

        let bytes = object_sto.get(&path).await.unwrap();
        let result = bytes.bytes().await.unwrap();
        assert_eq!(result.as_ref(), b"hello".as_slice());

        let path = Path::parse("data/large_file_multipart").unwrap();
        let mut writer = object_sto.writer(&path);
        let bytes = Bytes::from_static(b"hello");
        writer.write_all(&bytes).await.unwrap();
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();

        let result = object_sto.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(result.as_ref(), b"hello");
    }

    #[test]
    fn parses_supported_storage_dsns() {
        assert_eq!(
            ObjectStorageConfig::from_str("memory://").unwrap(),
            ObjectStorageConfig::Memory
        );

        let local = ObjectStorageConfig::from_str("file:///tmp/kiseki-data").unwrap();
        assert_eq!(
            local,
            ObjectStorageConfig::File {
                root: "/tmp/kiseki-data".into(),
            }
        );

        let s3 = ObjectStorageConfig::from_str(
            "s3://volume-bucket/tenant/volume?region=test-region&endpoint=https%3A%2F%2Fobjects.\
             example",
        )
        .unwrap();
        assert_eq!(
            s3,
            ObjectStorageConfig::S3 {
                bucket:     "volume-bucket".to_string(),
                prefix:     Some("tenant/volume".to_string()),
                region:     Some("test-region".to_string()),
                endpoint:   Some("https://objects.example".to_string()),
                allow_http: false,
            }
        );

        let local_s3 = ObjectStorageConfig::from_str(
            "s3://volume-bucket?endpoint=http%3A%2F%2Flocalhost%3A9000&allow_http=true",
        )
        .unwrap();
        assert!(matches!(
            local_s3,
            ObjectStorageConfig::S3 {
                allow_http: true,
                ..
            }
        ));
    }

    #[test]
    fn rejects_unsafe_or_invalid_storage_dsns_without_echoing_secrets() {
        let invalid = [
            "relative/path",
            "file://relative/path",
            "ftp://objects.example/data",
            "s3:///missing-bucket",
            "s3://volume-bucket?endpoint=http%3A%2F%2Flocalhost%3A9000",
            "s3://volume-bucket?allow_http=true&allow_http=false",
        ];
        for dsn in invalid {
            assert!(
                ObjectStorageConfig::from_str(dsn).is_err(),
                "accepted {dsn}"
            );
        }

        let secret_marker = "do-not-echo-this-value";
        let unsafe_dsns = [
            format!("s3://user:{secret_marker}@volume-bucket/prefix"),
            format!("s3://volume-bucket/prefix?credential={secret_marker}"),
            format!("s3://volume-bucket/prefix?session_token={secret_marker}"),
        ];
        for dsn in unsafe_dsns {
            let error = ObjectStorageConfig::from_str(&dsn).unwrap_err();
            assert!(!error.to_string().contains(secret_marker));
        }

        let safe_query = ObjectStorageConfig::from_str(&format!(
            "s3://volume-bucket/prefix?region={secret_marker}&endpoint=https%3A%2F%2Fprivate.\
             example"
        ))
        .unwrap();
        let debug = format!("{safe_query:?}");
        assert!(!debug.contains(secret_marker));
        assert!(!debug.contains("private.example"));
    }

    #[tokio::test]
    async fn configured_memory_and_local_stores_pass_probe_and_round_trip() {
        let memory = ObjectStorageConfig::Memory.build().unwrap();
        memory.probe().await.unwrap();

        let tempdir = tempdir().unwrap();
        let local = ObjectStorageConfig::File {
            root: tempdir.path().to_path_buf(),
        }
        .build()
        .unwrap();
        local.probe().await.unwrap();

        let path = Path::parse("probe/round-trip").unwrap();
        local
            .put(&path, Bytes::from_static(b"configured"))
            .await
            .unwrap();
        let result = local.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(result.as_ref(), b"configured");
    }

    #[tokio::test]
    async fn probe_preserves_authentication_and_connectivity_errors() {
        for kind in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::ConnectionRefused,
        ] {
            let error = object_store::Error::Generic {
                store:  "TestStore",
                source: Box::new(std::io::Error::new(kind, "probe failed")),
            };
            let objects = futures::stream::once(async move { Err(error) }).boxed();

            let error = probe_list(objects).await.unwrap_err();
            let object_store::Error::Generic { source, .. } = error else {
                panic!("probe changed the backend error type");
            };
            assert_eq!(
                source.downcast_ref::<std::io::Error>().unwrap().kind(),
                kind
            );
        }
    }
}
