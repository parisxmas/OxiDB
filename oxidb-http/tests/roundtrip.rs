//! The server and client halves of `oxidb-http` talk to each other: `serve`
//! handles a request that `client` sends. This is the contract `oxibase` relies
//! on (its own listener + its calls to `oxidb-server`).

use oxidb_http::client;
use oxidb_http::message::HttpResponse;

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

#[test]
fn server_and_client_round_trip() {
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");

    let addr_srv = addr.clone();
    std::thread::spawn(move || {
        oxidb_http::server::serve(&addr_srv, 2, 16, |req| {
            // Echo the method + path + body back as JSON, and the bearer token.
            let bearer = req
                .headers
                .get("authorization")
                .cloned()
                .unwrap_or_default();
            let body = String::from_utf8_lossy(&req.body);
            let json = format!(
                r#"{{"method":"{}","path":"{}","auth":"{}","body":{}}}"#,
                req.method, req.path, bearer, body
            );
            HttpResponse::data(json.into_bytes(), "application/json")
        })
        .unwrap();
    });

    // Give the listener a moment to bind.
    for _ in 0..50 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    let url = format!("http://{addr}/platform/v1/projects");
    let resp = client::post_json(&url, Some("tok123"), br#"{"n":1}"#).unwrap();
    assert_eq!(resp.status, 200);
    let text = resp.text();
    assert!(text.contains(r#""method":"POST""#), "got: {text}");
    assert!(
        text.contains(r#""path":"/platform/v1/projects""#),
        "got: {text}"
    );
    assert!(text.contains(r#""auth":"Bearer tok123""#), "got: {text}");
    assert!(text.contains(r#""body":{"n":1}"#), "got: {text}");

    // A GET with no body, no bearer.
    let g = client::get(&format!("http://{addr}/health"), None).unwrap();
    assert_eq!(g.status, 200);
    assert!(g.text().contains(r#""method":"GET""#));
    assert!(g.text().contains(r#""path":"/health""#));
}
