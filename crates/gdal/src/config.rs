//! GDAL runtime configuration for virtual filesystem raster access.

use std::env;
use std::sync::OnceLock;

/// Configuration inputs for GDAL remote raster access.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GdalConfig {
    s3_endpoint: Option<String>,
}

impl GdalConfig {
    /// Create GDAL configuration with no source-specific remote endpoint.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create GDAL configuration for an S3-compatible endpoint.
    pub fn with_s3_endpoint(endpoint: impl Into<String>) -> Self {
        Self {
            s3_endpoint: Some(endpoint.into()),
        }
    }
}

/// Configure GDAL/CPL options needed for remote raster reads.
///
/// Baseline process-wide GDAL configuration is applied once. A source-specific
/// endpoint is applied whenever one is supplied. Credential values are copied
/// from the process environment when present, but are never logged.
pub fn ensure_gdal_configured(config: &GdalConfig) -> Result<(), String> {
    static BASELINE_CONFIGURED: OnceLock<Result<(), String>> = OnceLock::new();

    BASELINE_CONFIGURED
        .get_or_init(apply_baseline_gdal_options)
        .clone()?;

    apply_endpoint_option(config)
}

fn apply_baseline_gdal_options() -> Result<(), String> {
    for option in desired_baseline_gdal_options(|key| env::var(key).ok()) {
        gdal::config::set_config_option(option.key, &option.value)
            .map_err(|source| format!("failed to set GDAL option {}: {source}", option.key))?;
    }

    Ok(())
}

fn apply_endpoint_option(config: &GdalConfig) -> Result<(), String> {
    if let Some(endpoint) = config.s3_endpoint.as_deref().and_then(normalize_endpoint) {
        gdal::config::set_config_option("AWS_S3_ENDPOINT", &endpoint)
            .map_err(|source| format!("failed to set GDAL option AWS_S3_ENDPOINT: {source}"))?;
    }

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GdalOption {
    key: &'static str,
    value: String,
}

fn desired_baseline_gdal_options(get_env: impl Fn(&str) -> Option<String>) -> Vec<GdalOption> {
    let mut options = vec![
        GdalOption {
            key: "AWS_VIRTUAL_HOSTING",
            value: "FALSE".to_string(),
        },
        GdalOption {
            key: "AWS_REGION",
            value: "auto".to_string(),
        },
        GdalOption {
            key: "GDAL_DISABLE_READDIR_ON_OPEN",
            value: "EMPTY_DIR".to_string(),
        },
        GdalOption {
            key: "CPL_VSIL_CURL_CACHE_SIZE",
            value: "16777216".to_string(),
        },
    ];

    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_PROFILE",
    ] {
        if let Some(value) = get_env(key).filter(|value| !value.is_empty()) {
            options.push(GdalOption { key, value });
        }
    }

    options
}

fn normalize_endpoint(endpoint: &str) -> Option<String> {
    let trimmed = endpoint.trim();
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);
    let normalized = without_scheme.trim_end_matches('/');

    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn option_value(options: &[GdalOption], key: &str) -> Option<String> {
        options
            .iter()
            .find(|option| option.key == key)
            .map(|option| option.value.clone())
    }

    #[test]
    fn desired_options_include_safe_remote_defaults() {
        let options = desired_baseline_gdal_options(|_| None);

        assert_eq!(
            option_value(&options, "AWS_VIRTUAL_HOSTING"),
            Some("FALSE".to_string())
        );
        assert_eq!(
            option_value(&options, "AWS_REGION"),
            Some("auto".to_string())
        );
        assert_eq!(
            option_value(&options, "GDAL_DISABLE_READDIR_ON_OPEN"),
            Some("EMPTY_DIR".to_string())
        );
        assert_eq!(
            option_value(&options, "CPL_VSIL_CURL_CACHE_SIZE"),
            Some("16777216".to_string())
        );
        assert_eq!(option_value(&options, "AWS_S3_ENDPOINT"), None);
    }

    #[test]
    fn desired_options_include_env_credentials() {
        let options = desired_baseline_gdal_options(|key| match key {
            "AWS_ACCESS_KEY_ID" => Some("access".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),
            "AWS_SESSION_TOKEN" => Some("token".to_string()),
            _ => None,
        });

        assert_eq!(
            option_value(&options, "AWS_ACCESS_KEY_ID"),
            Some("access".to_string())
        );
        assert_eq!(
            option_value(&options, "AWS_SECRET_ACCESS_KEY"),
            Some("secret".to_string())
        );
        assert_eq!(
            option_value(&options, "AWS_SESSION_TOKEN"),
            Some("token".to_string())
        );
    }

    #[test]
    fn normalize_endpoint_strips_scheme_and_trailing_slash() {
        assert_eq!(
            normalize_endpoint("https://abc123.r2.cloudflarestorage.com/"),
            Some("abc123.r2.cloudflarestorage.com".to_string())
        );
        assert_eq!(normalize_endpoint("  "), None);
    }
}
