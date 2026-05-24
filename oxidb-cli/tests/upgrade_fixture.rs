//! Phase 4 carryover: run `oxidb migrate inspect` against the committed
//! upgrade-chain fixture corpus and assert the CLI parses it cleanly.
//!
//! See tests/fixtures/upgrade/README.md for the fixture contract.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is the oxidb-cli/ crate; its parent is the workspace root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn fixture_tarball() -> PathBuf {
    repo_root().join("tests/fixtures/upgrade/v0.28.4.tar.gz")
}

fn oxidb_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_oxidb"))
}

#[test]
fn migrate_inspect_reads_upgrade_fixture() {
    let tarball = fixture_tarball();
    assert!(tarball.exists(), "fixture missing: {}", tarball.display());

    let temp = tempdir();

    // Extract via system `tar` — the fixture is a flat tarball (no wrapper dir).
    let status = Command::new("tar")
        .arg("xzf")
        .arg(&tarball)
        .arg("-C")
        .arg(&temp)
        .status()
        .expect("spawn tar");
    assert!(status.success(), "tar extraction failed");

    let output = Command::new(oxidb_bin())
        .arg("migrate")
        .arg("inspect")
        .arg("--data")
        .arg(&temp)
        .arg("--json")
        .output()
        .expect("spawn oxidb");

    assert!(
        output.status.success(),
        "`oxidb migrate inspect` exited {:?}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr)
    );

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("inspect JSON parses");
    let reports = json.as_array().expect("inspect output is an array");

    assert!(
        !reports.is_empty(),
        "fixture should contain format-versioned files"
    );

    let mut current = 0usize;
    let mut newer = 0usize;
    let mut unreadable = 0usize;
    for r in reports {
        let status = &r["status"];
        if status.get("current").is_some() {
            current += 1;
        } else if status.get("newer").is_some() {
            newer += 1;
        } else if status.get("unreadable").is_some() {
            unreadable += 1;
        }
    }

    assert!(
        current > 0,
        "expected at least one current-format file, reports: {reports:#?}"
    );
    assert_eq!(
        newer, 0,
        "current engine should not see any newer-format files in fixtures"
    );
    assert_eq!(unreadable, 0, "fixture files should all parse cleanly");

    // `migrate run` on the fixture should also succeed (no migration needed).
    let run = Command::new(oxidb_bin())
        .arg("migrate")
        .arg("run")
        .arg("--data")
        .arg(&temp)
        .output()
        .expect("spawn oxidb run");
    assert!(
        run.status.success(),
        "`oxidb migrate run` exited {:?}\nstderr: {}",
        run.status.code(),
        String::from_utf8_lossy(&run.stderr)
    );

    cleanup(&temp);
}

fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "oxidb-upgrade-fixture-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("mkdir tempdir");
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}
