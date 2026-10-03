//! Raw-socket checks of the S3 endpoint over a real loopback listener, with no
//! S3 client library in the way. `scripts/s3-duckdb.sh` covers DuckDB itself.

use std::io::{Read, Write};
use std::net::TcpStream;

use logless_core::s3_serve::{S3ServeConfig, S3Server};

const KEY: &str = "hour=2026-10-02T23/level=info/a.parquet";

fn start(dir: &std::path::Path) -> S3Server {
    let file = dir.join(KEY);
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    let body: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&file, body).unwrap();
    let cfg = S3ServeConfig {
        enabled: true,
        addr: "127.0.0.1:0".into(),
        ..S3ServeConfig::default()
    };
    S3Server::start(&cfg, dir.to_path_buf()).unwrap()
}

fn request(server: &S3Server, head: &str) -> (String, Vec<u8>) {
    let mut s = TcpStream::connect(server.local_addr()).unwrap();
    write!(s, "{head}\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("header end") + 4;
    (String::from_utf8_lossy(&raw[..split]).to_string(), raw[split..].to_vec())
}

#[test]
fn head_reports_the_object_size_without_chunked_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path());
    let (head, body) = request(&server, &format!("HEAD /logs/{KEY} HTTP/1.1"));
    let lower = head.to_ascii_lowercase();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    // DuckDB sizes a bare s3:// key from HEAD; without a length it downloads the whole object.
    assert!(lower.contains("content-length: 200000"), "{head}");
    assert!(!lower.contains("transfer-encoding"), "{head}");
    assert!(body.is_empty());
    server.shutdown();
}

#[test]
fn ranged_get_returns_only_the_requested_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path());
    let (head, body) = request(
        &server,
        &format!("GET /logs/{KEY} HTTP/1.1\r\nRange: bytes=1000-1099"),
    );
    assert!(head.starts_with("HTTP/1.1 206"), "{head}");
    assert_eq!(body.len(), 100);
    assert_eq!(body[0], (1000u32 % 251) as u8);
    let stats = server.stats();
    assert_eq!(stats.range_requests, 1);
    assert_eq!(stats.bytes_served, 100);
    let (head, _) = request(
        &server,
        &format!("GET /logs/{KEY} HTTP/1.1\r\nRange: bytes=999999-1000000"),
    );
    assert!(head.starts_with("HTTP/1.1 416"), "{head}");
    server.shutdown();
}

#[test]
fn an_open_download_survives_deletion_and_the_next_request_gets_404() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(dir.path());
    let mut s = TcpStream::connect(server.local_addr()).unwrap();
    write!(s, "GET /logs/{KEY} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").unwrap();
    let mut first = [0u8; 512];
    s.read_exact(&mut first).unwrap();
    std::fs::remove_file(dir.path().join(KEY)).unwrap();
    let mut rest = Vec::new();
    s.read_to_end(&mut rest).unwrap();
    assert!(rest.len() + first.len() > 200_000, "download was cut short");
    let (head, _) = request(&server, &format!("GET /logs/{KEY} HTTP/1.1"));
    assert!(head.starts_with("HTTP/1.1 404"), "{head}");
    server.shutdown();
}
