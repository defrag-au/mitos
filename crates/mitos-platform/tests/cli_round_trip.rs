//! End-to-end test for the deployment CLI: boot a real admin
//! router on a transient TCP port, invoke the actual
//! `mitos-admin upload-module` and `list-modules` binaries, and
//! confirm artifacts round-trip cleanly.
//!
//! Auto-skips when:
//!   - the ownership module .wasm isn't built (same auto-skip
//!     pattern as the other integration tests)
//!   - the `mitos-admin` debug binary isn't built (`cargo build
//!     -p mitos-admin` first)
//!
//! Tests the *actual on-the-wire* multipart format the CLI uses
//! against the *actual* axum route the host serves. The lib-level
//! admin tests cover the router in isolation; this test covers
//! the CLI side too.

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use mitos_platform::admin::{AuthToken, admin_router};
use mitos_platform::manifest::{
    AbiSection, BuildSection, Manifest, ModuleSection, TrapPolicySection, sha256_hex,
};
use mitos_platform::storage::ModuleStorage;
use tokio::net::TcpListener;

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

fn mitos_admin_bin() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = manifest
        .parent()? // crates/
        .parent()? // mitos/
        .join("target/debug/mitos-admin");
    candidate.exists().then_some(candidate)
}

fn tempdir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "mitos-platform-cli-test-{}-{}",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn write_artifact_dir(out: &std::path::Path, manifest: &Manifest, wasm: &[u8]) {
    std::fs::create_dir_all(out).unwrap();
    std::fs::write(out.join(format!("{}.wasm", manifest.module.id)), wasm).unwrap();
    std::fs::write(out.join("manifest.toml"), manifest.to_toml().unwrap()).unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_upload_then_list_round_trip() {
    let Some(wasm_path) = ownership_module_wasm() else {
        eprintln!("skipping: ownership module .wasm not built");
        return;
    };
    let Some(bin) = mitos_admin_bin() else {
        eprintln!(
            "skipping: mitos-admin debug binary not built — \
             run `cargo build -p mitos-admin` first"
        );
        return;
    };
    let wasm = std::fs::read(&wasm_path).unwrap();
    let manifest = manifest_for(&wasm);

    let storage_dir = tempdir("cli-round-trip-storage");
    let artifact_dir = tempdir("cli-round-trip-artifact");
    write_artifact_dir(&artifact_dir, &manifest, &wasm);

    // Boot the admin router on an OS-assigned port.
    let storage = ModuleStorage::new(&storage_dir);
    let app = admin_router(storage.clone(), AuthToken(None));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    // Tiny grace period for the server to be accept()-ready.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let base = format!("http://127.0.0.1:{port}");

    // Step 1: upload-module via the CLI.
    let out = Command::new(&bin)
        .arg("--mitos")
        .arg(&base)
        .arg("upload-module")
        .arg("--artifact")
        .arg(&artifact_dir)
        .output()
        .expect("run mitos-admin upload-module");
    assert!(
        out.status.success(),
        "upload-module exit {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("uploaded module=ownership"),
        "expected upload success line, got: {stdout}"
    );

    // Step 2: list-modules sees what we uploaded.
    let out = Command::new(&bin)
        .arg("--mitos")
        .arg(&base)
        .arg("list-modules")
        .arg("--json")
        .output()
        .expect("run mitos-admin list-modules");
    assert!(
        out.status.success(),
        "list-modules exit {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8(out.stdout).unwrap();
    let listed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let arr = listed.as_array().unwrap();
    assert_eq!(arr.len(), 1, "expected one module, got: {stdout}");
    assert_eq!(arr[0]["id"], "ownership");
    assert_eq!(arr[0]["abi_version"], "1.0");

    // Step 3: get-module sees the same.
    let out = Command::new(&bin)
        .arg("--mitos")
        .arg(&base)
        .arg("get-module")
        .arg("ownership")
        .output()
        .expect("run mitos-admin get-module");
    assert!(
        out.status.success(),
        "get-module exit {:?}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("id:            ownership"));

    // Step 4: artifact actually landed on the host's storage dir.
    let read_manifest = storage.read_manifest("ownership").unwrap().unwrap();
    assert_eq!(read_manifest, manifest);

    server_task.abort();
    std::fs::remove_dir_all(&storage_dir).ok();
    std::fs::remove_dir_all(&artifact_dir).ok();
}
