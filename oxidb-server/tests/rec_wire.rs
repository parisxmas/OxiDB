//! The rec engine over the real wire: spawn the binary with OXIDB_REC=1,
//! speak OxiWire (length-prefixed JSON), and pin routing, the read-only
//! gate, persistence across a restart, and the off-by-default refusal.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Guard {
    child: Child,
    _dir: tempfile::TempDir,
    port: u16,
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Ready-file spawn — see pg_wire.rs for why ports are never guessed.
fn spawn_with(envs: &[(&str, &str)]) -> Guard {
    let dir = tempfile::tempdir().unwrap();
    spawn_in(dir, envs)
}

fn spawn_in(dir: tempfile::TempDir, envs: &[(&str, &str)]) -> Guard {
    let ready = dir.path().join("ready");
    let _ = std::fs::remove_file(&ready);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_oxidb-server"));
    cmd.env("OXIDB_DATA", dir.path().join("data"))
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .env("OXIDB_READY_FILE", &ready)
        .env("OXIDB_REC", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let mut child = cmd.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let port: u16 = loop {
        if let Ok(body) = std::fs::read_to_string(&ready) {
            break body
                .lines()
                .find_map(|l| l.strip_prefix("addr="))
                .and_then(|a| a.rsplit(':').next())
                .and_then(|p| p.parse().ok())
                .expect("ready file names the main listener");
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited before becoming ready: {status}");
        }
        assert!(Instant::now() < deadline, "server never became ready");
        std::thread::sleep(Duration::from_millis(20));
    };
    Guard {
        child,
        _dir: dir,
        port,
    }
}

fn call(port: u16, req: &serde_json::Value) -> serde_json::Value {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let body = serde_json::to_vec(req).unwrap();
    s.write_all(&(body.len() as u32).to_le_bytes()).unwrap();
    s.write_all(&body).unwrap();
    let mut len = [0u8; 4];
    s.read_exact(&mut len).unwrap();
    let mut buf = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut buf).unwrap();
    serde_json::from_slice(&buf).unwrap()
}

fn rec(port: u16, extra: serde_json::Value) -> serde_json::Value {
    let mut req = serde_json::json!({ "engine": "rec", "cmd": "rec" });
    req.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    call(port, &req)
}

#[test]
fn track_related_and_stats_over_the_wire() {
    let g = spawn_with(&[]);
    for i in 0..20u64 {
        let r = rec(
            g.port,
            serde_json::json!({
                "op": "track", "model": "purchase", "basket_id": i,
                "items": ["kahve", "süt"], "ts": 1_700_000_000u64
            }),
        );
        assert_eq!(r["ok"], true, "{r}");
        assert_eq!(r["data"]["counted"], true);
    }
    rec(
        g.port,
        serde_json::json!({
            "op": "track", "model": "purchase", "basket_id": 100,
            "items": ["kahve", "filtre"], "ts": 1_700_000_000u64
        }),
    );

    let r = rec(
        g.port,
        serde_json::json!({
            "op": "related", "model": "purchase", "item": "kahve",
            "scoring": "count", "ts": 1_700_000_000u64
        }),
    );
    assert_eq!(r["ok"], true, "{r}");
    let recs = r["data"]["recommendations"].as_array().unwrap();
    assert_eq!(recs[0]["item"], "süt");
    assert_eq!(recs[0]["score"], 20.0);
    assert_eq!(recs[1]["item"], "filtre");

    let r = rec(g.port, serde_json::json!({ "op": "stats" }));
    assert_eq!(r["data"]["models"]["purchase"]["baskets"], 21);

    // A duplicate basket id is not re-counted, over the wire too.
    let r = rec(
        g.port,
        serde_json::json!({
            "op": "track", "model": "purchase", "basket_id": 5,
            "items": ["kahve", "süt"], "ts": 1_700_000_000u64
        }),
    );
    assert_eq!(r["data"]["counted"], false);
}

#[test]
fn for_basket_excludes_and_recommends() {
    let g = spawn_with(&[]);
    for i in 0..20u64 {
        rec(
            g.port,
            serde_json::json!({
                "op": "track", "model": "p", "basket_id": i,
                "items": ["makarna", "sos", "peynir"], "ts": 1_700_000_000u64
            }),
        );
    }
    for i in 20..30u64 {
        rec(
            g.port,
            serde_json::json!({
                "op": "track", "model": "p", "basket_id": i,
                "items": ["ekmek"], "ts": 1_700_000_000u64
            }),
        );
    }
    let r = rec(
        g.port,
        serde_json::json!({
            "op": "for_basket", "model": "p", "items": ["makarna"],
            "exclude": ["sos"], "ts": 1_700_000_000u64
        }),
    );
    let names: Vec<&str> = r["data"]["recommendations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["item"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"peynir"));
    assert!(!names.contains(&"sos"), "exclude ignored");
    assert!(!names.contains(&"makarna"), "the basket itself returned");
}

#[test]
fn state_survives_a_restart() {
    let g = spawn_with(&[]);
    for i in 0..10u64 {
        rec(
            g.port,
            serde_json::json!({
                "op": "track", "model": "p", "basket_id": i,
                "items": ["a", "b"], "ts": 1_700_000_000u64
            }),
        );
    }
    // Restart on the same data dir (steal the tempdir from the guard).
    let dir = {
        let mut g = g;
        let _ = g.child.kill();
        let _ = g.child.wait();
        std::mem::replace(&mut g._dir, tempfile::tempdir().unwrap())
    };
    let g = spawn_in(dir, &[]);
    let r = rec(
        g.port,
        serde_json::json!({
            "op": "related", "model": "p", "item": "a",
            "scoring": "count", "ts": 1_700_000_000u64
        }),
    );
    let recs = r["data"]["recommendations"].as_array().unwrap();
    assert_eq!(recs[0]["item"], "b");
    assert_eq!(recs[0]["score"], 10.0, "WAL replay lost baskets");
}

#[test]
fn off_by_default_refuses_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut child = Command::new(env!("CARGO_BIN_EXE_oxidb-server"))
        .env("OXIDB_DATA", dir.path().join("data"))
        .env("OXIDB_ADDR", "127.0.0.1:0")
        .env("OXIDB_READY_FILE", &ready)
        .env_remove("OXIDB_REC")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let port: u16 = loop {
        if let Ok(body) = std::fs::read_to_string(&ready) {
            break body
                .lines()
                .find_map(|l| l.strip_prefix("addr="))
                .and_then(|a| a.rsplit(':').next())
                .and_then(|p| p.parse().ok())
                .unwrap();
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited: {status}");
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    };
    let r = call(
        port,
        &serde_json::json!({ "engine": "rec", "cmd": "rec", "op": "stats" }),
    );
    assert_eq!(r["ok"], false, "{r}");
    assert!(
        r["error"].as_str().unwrap().contains("OXIDB_REC"),
        "the refusal must name the switch: {r}"
    );
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn bad_requests_are_refused_with_names() {
    let g = spawn_with(&[]);
    let r = rec(g.port, serde_json::json!({ "op": "track", "model": "p" }));
    assert_eq!(r["ok"], false, "{r}");
    let r = rec(
        g.port,
        serde_json::json!({ "op": "related", "model": "p", "item": "x", "scoring": "sihirli" }),
    );
    assert!(r["error"].as_str().unwrap().contains("sihirli"), "{r}");
    let r = rec(g.port, serde_json::json!({ "op": "yok" }));
    assert!(r["error"].as_str().unwrap().contains("yok"), "{r}");
}
