//! Smoke test scaffold for the MinIO compose harness.
//!
//! This intentionally avoids S3 clients and async dependencies. Later
//! object-store integration tests can reuse the same environment variables.

use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

const ENDPOINT_ENV: &str = "HFX_S3_TEST_ENDPOINT";
const BUCKET_ENV: &str = "HFX_S3_TEST_BUCKET";
const ACCESS_KEY_ENV: &str = "HFX_S3_TEST_ACCESS_KEY";
const SECRET_KEY_ENV: &str = "HFX_S3_TEST_SECRET_KEY";

#[test]
#[ignore = "requires docker compose MinIO harness and HFX_S3_TEST_ENDPOINT"]
fn minio_health_endpoint_is_reachable() {
    let Some(endpoint) = optional_env(ENDPOINT_ENV) else {
        eprintln!("skipping MinIO smoke test: {ENDPOINT_ENV} is not set");
        return;
    };

    let bucket = optional_env(BUCKET_ENV).unwrap_or_else(|| "hfx-test-data".to_owned());
    let access_key = optional_env(ACCESS_KEY_ENV).unwrap_or_else(|| "shedtest".to_owned());
    let secret_key = optional_env(SECRET_KEY_ENV).unwrap_or_else(|| "shedtestsecret".to_owned());

    assert!(!bucket.trim().is_empty(), "{BUCKET_ENV} must not be empty");
    assert!(
        !access_key.trim().is_empty(),
        "{ACCESS_KEY_ENV} must not be empty"
    );
    assert!(
        !secret_key.trim().is_empty(),
        "{SECRET_KEY_ENV} must not be empty"
    );

    let endpoint = parse_http_endpoint(&endpoint);
    let response = get_health_ready(&endpoint);

    assert!(
        response.starts_with("HTTP/1.1 200") || response.starts_with("HTTP/1.0 200"),
        "MinIO health endpoint at {} returned unexpected response: {}",
        endpoint.base_url,
        response.lines().next().unwrap_or("<empty response>")
    );
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

#[derive(Debug)]
struct HttpEndpoint {
    base_url: String,
    host_header: String,
    socket_addr: SocketAddr,
}

fn parse_http_endpoint(raw: &str) -> HttpEndpoint {
    let base_url = raw.trim().trim_end_matches('/').to_owned();
    let without_scheme = base_url
        .strip_prefix("http://")
        .unwrap_or_else(|| panic!("{ENDPOINT_ENV} must start with http:// for this smoke test"));

    assert!(
        !without_scheme.contains('/'),
        "{ENDPOINT_ENV} must be an origin URL such as http://localhost:9000"
    );

    let (host, port) = match without_scheme.rsplit_once(':') {
        Some((host, port)) => {
            let port = port
                .parse::<u16>()
                .unwrap_or_else(|_| panic!("{ENDPOINT_ENV} has invalid port: {port}"));
            (host, port)
        }
        None => (without_scheme, 80),
    };

    assert!(!host.is_empty(), "{ENDPOINT_ENV} must include a host");

    let host_header = if port == 80 {
        host.to_owned()
    } else {
        format!("{host}:{port}")
    };
    let socket_addr = (host, port)
        .to_socket_addrs()
        .unwrap_or_else(|err| panic!("failed to resolve {host_header}: {err}"))
        .next()
        .unwrap_or_else(|| panic!("no socket addresses resolved for {host_header}"));

    HttpEndpoint {
        base_url,
        host_header,
        socket_addr,
    }
}

fn get_health_ready(endpoint: &HttpEndpoint) -> String {
    let timeout = Duration::from_secs(2);
    let mut stream = TcpStream::connect_timeout(&endpoint.socket_addr, timeout)
        .unwrap_or_else(|err| panic!("failed to connect to {}: {err}", endpoint.base_url));
    stream
        .set_read_timeout(Some(timeout))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(timeout))
        .expect("failed to set write timeout");

    let request = format!(
        "GET /minio/health/ready HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        endpoint.host_header
    );
    stream
        .write_all(request.as_bytes())
        .unwrap_or_else(|err| panic!("failed to write health request: {err}"));

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .unwrap_or_else(|err| panic!("failed to read health response: {err}"));
    response
}
