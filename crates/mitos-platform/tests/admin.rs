//! End-to-end smoke test for `/_admin/modules/*`.
//!
//! Drives the real ownership-indexer-module wasm through the
//! real axum router, hits the real validation paths, asserts
//! the artifact lands on disk in the expected layout.

use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use mitos_platform::admin::{AuthToken, ModuleSummary, admin_router};
use mitos_platform::manifest::{
    AbiSection, BuildSection, Manifest, ModuleSection, TrapPolicySection, sha256_hex,
};
use mitos_platform::storage::ModuleStorage;
use tower::ServiceExt;

fn ownership_module_wasm() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = manifest
        .parent()? // crates/
        .parent()? // mitos/
        .join(
            "modules/ownership-indexer/target/wasm32-wasip2/release/\
             ownership_indexer_module.wasm",
        );
    candidate.exists().then_some(candidate)
}

fn tempdir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "mitos-platform-admin-test-{}-{}",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn manifest_for(wasm: &[u8]) -> Manifest {
    Manifest {
        module: ModuleSection {
            id: "ownership".to_owned(),
            sha256: sha256_hex(wasm),
            size_bytes: wasm.len() as u64,
        },
        abi: AbiSection {
            version_major: 1,
            version_minor: 0,
            wit_package: "mitos:platform".to_owned(),
            wit_world: "mitos-module".to_owned(),
        },
        trap_policy: TrapPolicySection {
            strategy: "replay".to_owned(),
            max_retries: 3,
            backoff_cap_ms: 1_000,
        },
        build: BuildSection {
            rust_version: "1.95.0".to_owned(),
            target: "wasm32-wasip2".to_owned(),
            profile: "release".to_owned(),
            build_id: "2026-05-03T12:34:00Z".to_owned(),
            git_sha: None,
            crate_version: "0.0.0".to_owned(),
        },
        interest: Default::default(),
    }
}

/// Construct a minimal multipart body the way reqwest's client
/// would. Format: `--BOUNDARY\r\nContent-Disposition: form-data;
/// name="X"\r\n\r\n<bytes>\r\n--BOUNDARY--\r\n`
fn multipart_body(parts: &[(&str, &[u8])]) -> (String, Vec<u8>) {
    let boundary = "----mitosplatformtest";
    let mut buf: Vec<u8> = Vec::new();
    for (name, bytes) in parts {
        buf.extend_from_slice(b"--");
        buf.extend_from_slice(boundary.as_bytes());
        buf.extend_from_slice(b"\r\n");
        let header = format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n");
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(bytes);
        buf.extend_from_slice(b"\r\n");
    }
    buf.extend_from_slice(b"--");
    buf.extend_from_slice(boundary.as_bytes());
    buf.extend_from_slice(b"--\r\n");
    let content_type = format!("multipart/form-data; boundary={boundary}");
    (content_type, buf)
}

#[tokio::test]
async fn upload_module_happy_path() {
    let Some(wasm_path) = ownership_module_wasm() else {
        eprintln!("skipping: ownership module .wasm not built");
        return;
    };
    let wasm = std::fs::read(&wasm_path).unwrap();
    let manifest = manifest_for(&wasm);
    let manifest_toml = manifest.to_toml().unwrap();

    let storage_dir = tempdir("upload-happy");
    let storage = ModuleStorage::new(&storage_dir);
    let auth = AuthToken(None); // open mode for the test
    let app = admin_router(storage.clone(), auth);

    let (content_type, body) =
        multipart_body(&[("manifest", manifest_toml.as_bytes()), ("wasm", &wasm)]);

    let req = Request::builder()
        .method("POST")
        .uri("/_admin/modules/ownership")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let body_bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "expected 200, got {status}: {}",
        String::from_utf8_lossy(&body_bytes)
    );

    // Artifact landed on disk.
    let read_manifest = storage.read_manifest("ownership").unwrap().unwrap();
    assert_eq!(read_manifest, manifest);
    let read_wasm = storage.read_current_wasm("ownership").unwrap().unwrap();
    assert_eq!(read_wasm, wasm);

    std::fs::remove_dir_all(&storage_dir).ok();
}

#[tokio::test]
async fn upload_id_mismatch_rejected() {
    let Some(wasm_path) = ownership_module_wasm() else {
        return;
    };
    let wasm = std::fs::read(&wasm_path).unwrap();
    let mut manifest = manifest_for(&wasm);
    manifest.module.id = "different-name".to_owned();
    let manifest_toml = manifest.to_toml().unwrap();

    let storage_dir = tempdir("upload-id-mismatch");
    let storage = ModuleStorage::new(&storage_dir);
    let auth = AuthToken(None);
    let app = admin_router(storage.clone(), auth);

    let (content_type, body) =
        multipart_body(&[("manifest", manifest_toml.as_bytes()), ("wasm", &wasm)]);
    let req = Request::builder()
        .method("POST")
        .uri("/_admin/modules/ownership")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    // Nothing should have landed on disk.
    assert!(storage.read_manifest("ownership").unwrap().is_none());
    assert!(storage.read_manifest("different-name").unwrap().is_none());

    std::fs::remove_dir_all(&storage_dir).ok();
}

#[tokio::test]
async fn upload_sha_mismatch_rejected() {
    let Some(wasm_path) = ownership_module_wasm() else {
        return;
    };
    let wasm = std::fs::read(&wasm_path).unwrap();
    let mut manifest = manifest_for(&wasm);
    manifest.module.sha256 = "00".repeat(32);
    let manifest_toml = manifest.to_toml().unwrap();

    let storage_dir = tempdir("upload-sha-mismatch");
    let storage = ModuleStorage::new(&storage_dir);
    let auth = AuthToken(None);
    let app = admin_router(storage.clone(), auth);

    let (content_type, body) =
        multipart_body(&[("manifest", manifest_toml.as_bytes()), ("wasm", &wasm)]);
    let req = Request::builder()
        .method("POST")
        .uri("/_admin/modules/ownership")
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    std::fs::remove_dir_all(&storage_dir).ok();
}

#[tokio::test]
async fn upload_then_list_then_get() {
    let Some(wasm_path) = ownership_module_wasm() else {
        return;
    };
    let wasm = std::fs::read(&wasm_path).unwrap();
    let manifest = manifest_for(&wasm);
    let manifest_toml = manifest.to_toml().unwrap();

    let storage_dir = tempdir("upload-list-get");
    let storage = ModuleStorage::new(&storage_dir);
    let auth = AuthToken(None);

    // Upload.
    {
        let app = admin_router(storage.clone(), auth.clone());
        let (content_type, body) =
            multipart_body(&[("manifest", manifest_toml.as_bytes()), ("wasm", &wasm)]);
        let req = Request::builder()
            .method("POST")
            .uri("/_admin/modules/ownership")
            .header(header::CONTENT_TYPE, content_type)
            .body(Body::from(body))
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    // List.
    {
        let app = admin_router(storage.clone(), auth.clone());
        let req = Request::builder()
            .method("GET")
            .uri("/_admin/modules")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let listed: Vec<ModuleSummary> = serde_json::from_slice(&body).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, "ownership");
        assert_eq!(listed[0].sha256, sha256_hex(&wasm));
        assert_eq!(listed[0].abi_version, "1.0");
        assert_eq!(listed[0].trap_strategy, "replay");
    }

    // Get single.
    {
        let app = admin_router(storage.clone(), auth.clone());
        let req = Request::builder()
            .method("GET")
            .uri("/_admin/modules/ownership")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let summary: ModuleSummary = serde_json::from_slice(&body).unwrap();
        assert_eq!(summary.id, "ownership");
    }

    // Get absent.
    {
        let app = admin_router(storage.clone(), auth);
        let req = Request::builder()
            .method("GET")
            .uri("/_admin/modules/nope")
            .body(Body::empty())
            .unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    std::fs::remove_dir_all(&storage_dir).ok();
}

#[tokio::test]
async fn auth_required_when_token_set() {
    let storage_dir = tempdir("auth");
    let storage = ModuleStorage::new(&storage_dir);
    let auth = AuthToken(Some("secret-test-token".to_owned()));
    let app = admin_router(storage.clone(), auth);

    // No header → 401.
    let req = Request::builder()
        .method("GET")
        .uri("/_admin/modules")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Wrong token → 401.
    let req = Request::builder()
        .method("GET")
        .uri("/_admin/modules")
        .header(header::AUTHORIZATION, "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Correct token → 200.
    let req = Request::builder()
        .method("GET")
        .uri("/_admin/modules")
        .header(header::AUTHORIZATION, "Bearer secret-test-token")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    std::fs::remove_dir_all(&storage_dir).ok();
}
