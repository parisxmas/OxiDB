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

// ─── COBRA extension methods (ADR-0025 Phase 4) ─────────────────────────

fn cobra_payload(name: &str) -> String {
    use base64::Engine as _;
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../oxidb-sql/tests/data/cobra")
        .join(format!("{name}.cobrac"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("fixture {path:?}: {e}"));
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn sql(port: u16, stmt: &str) -> serde_json::Value {
    call(
        port,
        &serde_json::json!({ "engine": "sql", "cmd": "sql", "sql": stmt }),
    )
}

/// A stored procedure crosses into the rec engine: track over the wire,
/// CALL a COBRA proc whose body asks db.rec_related, get the same answer a
/// wire client gets — one dispatch, two front doors.
#[test]
fn a_cobra_procedure_reaches_the_rec_engine() {
    let g = spawn_with(&[("OXIDB_SQL", "1")]);
    for i in 0..20u64 {
        rec(
            g.port,
            serde_json::json!({
                "op": "track", "model": "purchase", "basket_id": i,
                "items": ["kahve", "süt"], "ts": 1_700_000_000u64
            }),
        );
    }
    rec(
        g.port,
        serde_json::json!({
            "op": "track", "model": "purchase", "basket_id": 99,
            "items": ["kahve", "filtre"], "ts": 1_700_000_000u64
        }),
    );

    let r = sql(
        g.port,
        &format!(
            "CREATE PROCEDURE rec_related(item TEXT) LANGUAGE COBRA AS '{}'",
            cobra_payload("rec_related")
        ),
    );
    assert_eq!(r["ok"], true, "{r}");

    let r = sql(g.port, "CALL rec_related('kahve')");
    assert_eq!(r["ok"], true, "{r}");
    // execute_json returns one result object per statement.
    let result = &r["data"][0];
    let rows = result["rows"].as_array().unwrap_or_else(|| panic!("{r}"));
    let cols = result["columns"]
        .as_array()
        .unwrap_or_else(|| panic!("{r}"));
    let item_at = cols.iter().position(|c| c == "item").unwrap();
    assert_eq!(rows[0][item_at], "süt");
    assert_eq!(rows[1][item_at], "filtre");
}

/// And into the DOCUMENT engine's vector index — the cross-engine hop.
#[test]
fn a_cobra_procedure_reaches_vector_search() {
    let g = spawn_with(&[("OXIDB_SQL", "1")]);
    // Two 2-d vectors; the query [1,0] must rank "yakın" first.
    call(
        g.port,
        &serde_json::json!({ "cmd": "create_vector_index", "collection": "embed",
            "field": "v", "dimension": 2, "metric": "euclidean" }),
    );
    call(
        g.port,
        &serde_json::json!({ "cmd": "insert", "collection": "embed",
            "doc": {"name": "yakın", "v": [0.9, 0.1]} }),
    );
    call(
        g.port,
        &serde_json::json!({ "cmd": "insert", "collection": "embed",
            "doc": {"name": "uzak", "v": [-1.0, 0.5]} }),
    );

    let r = sql(
        g.port,
        &format!(
            "CREATE PROCEDURE vector_probe() LANGUAGE COBRA AS '{}'",
            cobra_payload("vector_probe")
        ),
    );
    assert_eq!(r["ok"], true, "{r}");

    let r = sql(g.port, "CALL vector_probe()");
    assert_eq!(r["ok"], true, "{r}");
    let rows = r["data"][0]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{r}"));
    assert_eq!(rows[0][0], "yakın", "{r}");
}
