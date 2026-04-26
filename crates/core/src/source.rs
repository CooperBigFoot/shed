//! Dataset source parsing for local and object-store backed HFX roots.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use url::Url;

use crate::error::SessionError;

const PUBLIC_R2_CUSTOM_DOMAIN: &str = "basin-delineations-public.upstream.tech";
const PUBLIC_R2_BUCKET_NAME: &str = "basin-delineations-public";

/// A parsed HFX dataset location.
#[derive(Debug, Clone)]
pub enum DatasetSource {
    /// A dataset rooted on the local filesystem.
    Local(PathBuf),
    /// A dataset rooted in an object store.
    Remote {
        /// Object store configured for the backing bucket.
        store: Arc<dyn ObjectStore>,
        /// Prefix within the object store where the dataset root begins.
        root: ObjectPath,
        /// Original URL supplied by the caller.
        url: Url,
    },
}

impl DatasetSource {
    /// Parse a dataset source from a local path or supported URL.
    ///
    /// # Errors
    ///
    /// | Variant | Condition |
    /// |---|---|
    /// | [`SessionError::InvalidDatasetSource`] | The input looks like a URL but cannot be parsed, or a file/R2 URL is malformed |
    /// | [`SessionError::UnsupportedDatasetSource`] | The URL scheme or HTTP endpoint is not supported |
    /// | [`SessionError::DatasetSourcePath`] | The remote URL path cannot be represented as an object-store path |
    /// | [`SessionError::ObjectStoreConfig`] | Object-store configuration fails |
    pub fn parse(input: &str) -> Result<Self, SessionError> {
        match Url::parse(input) {
            Ok(url) => Self::parse_url(input, url),
            Err(source) if input.contains("://") => Err(SessionError::InvalidDatasetSource {
                input: input.to_string(),
                reason: format!("invalid URL: {source}"),
            }),
            Err(_) => Ok(Self::Local(PathBuf::from(input))),
        }
    }

    fn parse_url(input: &str, url: Url) -> Result<Self, SessionError> {
        match url.scheme() {
            "file" => Self::parse_file_url(input, url),
            "s3" => Self::parse_s3_url(input, url),
            "http" | "https" => Self::parse_http_url(input, url),
            scheme => Err(SessionError::UnsupportedDatasetSource {
                input: input.to_string(),
                reason: format!("unsupported URL scheme {scheme:?}"),
            }),
        }
    }

    fn parse_file_url(input: &str, url: Url) -> Result<Self, SessionError> {
        let path = url
            .to_file_path()
            .map_err(|()| SessionError::InvalidDatasetSource {
                input: input.to_string(),
                reason: "file URL cannot be converted to a local path".to_string(),
            })?;

        Ok(Self::Local(path))
    }

    fn parse_s3_url(input: &str, url: Url) -> Result<Self, SessionError> {
        let root = ObjectPath::from_url_path(url.path()).map_err(|source| {
            SessionError::DatasetSourcePath {
                input: input.to_string(),
                source,
            }
        })?;
        let store = AmazonS3Builder::from_env()
            .with_url(input)
            .build()
            .map_err(|source| SessionError::ObjectStoreConfig {
                input: input.to_string(),
                source: Box::new(source),
            })?;

        Ok(Self::Remote {
            store: Arc::new(store),
            root,
            url,
        })
    }

    fn parse_http_url(input: &str, url: Url) -> Result<Self, SessionError> {
        let Some(host) = url.host_str().map(str::to_owned) else {
            return Err(SessionError::InvalidDatasetSource {
                input: input.to_string(),
                reason: "URL is missing a host".to_string(),
            });
        };

        if host == PUBLIC_R2_CUSTOM_DOMAIN {
            return Self::parse_public_r2_custom_domain_url(input, url);
        }

        if let Some(account) = host.strip_suffix(".r2.cloudflarestorage.com") {
            return Self::parse_account_r2_url(input, url, account);
        }

        Err(SessionError::UnsupportedDatasetSource {
            input: input.to_string(),
            reason: "only Cloudflare R2 HTTP(S) endpoints are supported".to_string(),
        })
    }

    fn parse_account_r2_url(input: &str, url: Url, account: &str) -> Result<Self, SessionError> {
        if account.is_empty() {
            return Err(SessionError::InvalidDatasetSource {
                input: input.to_string(),
                reason: "Cloudflare R2 endpoint is missing an account id".to_string(),
            });
        }

        let mut segments = url
            .path_segments()
            .into_iter()
            .flatten()
            .filter(|segment| !segment.is_empty());
        let Some(bucket) = segments.next() else {
            return Err(SessionError::InvalidDatasetSource {
                input: input.to_string(),
                reason: "Cloudflare R2 URL is missing a bucket path segment".to_string(),
            });
        };
        let root_path = segments.collect::<Vec<_>>().join("/");
        let root = ObjectPath::from_url_path(&root_path).map_err(|source| {
            SessionError::DatasetSourcePath {
                input: input.to_string(),
                source,
            }
        })?;
        let endpoint = format!("https://{account}.r2.cloudflarestorage.com");
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_region("auto")
            .with_virtual_hosted_style_request(false)
            .build()
            .map_err(|source| SessionError::ObjectStoreConfig {
                input: input.to_string(),
                source: Box::new(source),
            })?;

        Ok(Self::Remote {
            store: Arc::new(store),
            root,
            url,
        })
    }

    fn parse_public_r2_custom_domain_url(input: &str, url: Url) -> Result<Self, SessionError> {
        let root = ObjectPath::from_url_path(url.path()).map_err(|source| {
            SessionError::DatasetSourcePath {
                input: input.to_string(),
                source,
            }
        })?;
        let endpoint = format!("https://{PUBLIC_R2_CUSTOM_DOMAIN}");
        let store = AmazonS3Builder::new()
            .with_bucket_name(PUBLIC_R2_BUCKET_NAME)
            .with_endpoint(endpoint)
            .with_region("auto")
            .with_virtual_hosted_style_request(true)
            .with_skip_signature(true)
            .build()
            .map_err(|source| SessionError::ObjectStoreConfig {
                input: input.to_string(),
                source: Box::new(source),
            })?;

        Ok(Self::Remote {
            store: Arc::new(store),
            root,
            url,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DatasetSource;
    use crate::error::SessionError;

    #[test]
    fn parses_plain_path_as_local_source() {
        let source = DatasetSource::parse("fixtures/hfx").expect("source should parse");

        match source {
            DatasetSource::Local(path) => {
                assert_eq!(path, std::path::PathBuf::from("fixtures/hfx"))
            }
            DatasetSource::Remote { .. } => panic!("expected local source"),
        }
    }

    #[test]
    fn parses_file_url_as_local_source() {
        let source = DatasetSource::parse("file:///tmp/shed-fixture").expect("source should parse");

        match source {
            DatasetSource::Local(path) => {
                assert_eq!(path, std::path::PathBuf::from("/tmp/shed-fixture"))
            }
            DatasetSource::Remote { .. } => panic!("expected local source"),
        }
    }

    #[test]
    fn parses_s3_url_as_remote_source() {
        let source =
            DatasetSource::parse("s3://shed-test/example/root").expect("source should parse");

        match source {
            DatasetSource::Remote { root, url, .. } => {
                assert_eq!(root.as_ref(), "example/root");
                assert_eq!(url.as_str(), "s3://shed-test/example/root");
            }
            DatasetSource::Local(_) => panic!("expected remote source"),
        }
    }

    #[test]
    fn parses_r2_url_as_remote_source() {
        let source =
            DatasetSource::parse("https://abc123.r2.cloudflarestorage.com/shed-test/example/root")
                .expect("source should parse");

        match source {
            DatasetSource::Remote { root, url, .. } => {
                assert_eq!(root.as_ref(), "example/root");
                assert_eq!(
                    url.as_str(),
                    "https://abc123.r2.cloudflarestorage.com/shed-test/example/root"
                );
            }
            DatasetSource::Local(_) => panic!("expected remote source"),
        }
    }

    #[test]
    fn parses_public_r2_custom_domain_url_as_remote_source() {
        let source =
            DatasetSource::parse("https://basin-delineations-public.upstream.tech/global/hfx")
                .expect("source should parse");

        match source {
            DatasetSource::Remote { root, url, .. } => {
                assert_eq!(root.as_ref(), "global/hfx");
                assert_eq!(
                    url.as_str(),
                    "https://basin-delineations-public.upstream.tech/global/hfx"
                );
            }
            DatasetSource::Local(_) => panic!("expected remote source"),
        }
    }

    #[test]
    fn rejects_unsupported_url_scheme() {
        let error = DatasetSource::parse("gs://bucket/root").expect_err("source should fail");

        assert!(matches!(
            error,
            SessionError::UnsupportedDatasetSource { .. }
        ));
    }

    #[test]
    fn rejects_non_r2_http_endpoint() {
        let error = DatasetSource::parse("https://example.com/bucket/root")
            .expect_err("source should fail");

        assert!(matches!(
            error,
            SessionError::UnsupportedDatasetSource { .. }
        ));
    }
}
