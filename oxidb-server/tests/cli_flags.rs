//! `--version` / `--help` must print and exit — never start a server.
//!
//! Regression guard: before these flags existed the binary ignored them and
//! silently booted a server on the default port with a `./oxidb_data` dir in
//! the cwd, which is a nasty surprise for anyone probing the binary (and hangs
//! a non-interactive shell). Each case below asserts the process **exits on its
//! own** — a spawned server would block until the timeout and fail the test.

use std::process::Command;
use std::time::{Duration, Instant};

/// Run the binary with `args` and return (stdout, exited_within_timeout).
fn run(args: &[&str]) -> (String, bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .args(args)
        // Point at a throwaway cwd/data dir: if a regression ever does boot a
        // server here, it pollutes the tempdir, not the repo.
        .current_dir(dir.path())
        .env("OXIDB_DATA", dir.path())
        // A free-but-unusable address, so a regression can't grab a real port.
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let exited = loop {
        match child.try_wait().unwrap() {
            Some(_) => break true,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break false;
            }
            None => std::thread::sleep(Duration::from_millis(25)),
        }
    };
    let out = child.wait_with_output().unwrap();
    (String::from_utf8_lossy(&out.stdout).to_string(), exited)
}

#[test]
fn version_flag_prints_and_exits() {
    for flag in ["--version", "-V"] {
        let (out, exited) = run(&[flag]);
        assert!(exited, "`{flag}` did not exit — it started a server");
        assert!(
            out.contains(env!("CARGO_PKG_VERSION")),
            "`{flag}` printed no version: {out:?}"
        );
        assert!(out.contains("oxidb-server"), "`{flag}` output: {out:?}");
    }
}

#[test]
fn help_flag_prints_and_exits() {
    for flag in ["--help", "-h"] {
        let (out, exited) = run(&[flag]);
        assert!(exited, "`{flag}` did not exit — it started a server");
        assert!(out.contains("USAGE"), "`{flag}` printed no usage: {out:?}");
        // The help is the discoverability surface for env-var configuration.
        assert!(
            out.contains("OXIDB_ADDR"),
            "`{flag}` omits env vars: {out:?}"
        );
    }
}
