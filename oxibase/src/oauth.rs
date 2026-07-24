//! Per-project end-user OAuth — the social-login half of OxiBase's GoTrue
//! analog. An *app's own users* sign in with Google or GitHub against a
//! project; the project's developer configures the provider credentials.
//!
//! Two shapes are supported:
//!
//! - **Authorization-code redirect** (`/auth/authorize/{provider}` →  provider
//!   →  `/auth/callback/{provider}`), which is what "Sign in with GitHub" means
//!   for an ordinary web app. Needs a client id **and** secret, sealed per
//!   project with the seal key.
//! - **Google ID token** (`POST /auth/oauth/google`), for apps that already run
//!   Google Identity Services in the browser and hold a `credential`. Needs
//!   only the (public) client id.
//!
//! Everything here is transport + validation; session minting stays in
//! `handlers`, so this module has no notion of users or refresh tokens.

use serde_json::{Value, json};

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// How long a signed `state` blob stays valid. Long enough for a human to get
/// through a provider's consent screen, short enough to bound replay.
const STATE_TTL_SECS: u64 = 600;

/// An identity a provider vouched for. The email is always one the provider
/// says it verified — an unverified address would let anyone claim another
/// user's account by signing up at the provider with their address.
pub struct Identity {
    pub provider: String,
    pub subject: String,
    pub email: String,
    pub name: Option<String>,
}

pub struct Provider {
    pub name: &'static str,
    pub authorize_url: &'static str,
    pub token_url: &'static str,
    pub scope: &'static str,
}

pub fn provider(name: &str) -> Option<Provider> {
    match name {
        "google" => Some(Provider {
            name: "google",
            authorize_url: "https://accounts.google.com/o/oauth2/v2/auth",
            token_url: "https://oauth2.googleapis.com/token",
            scope: "openid email profile",
        }),
        "github" => Some(Provider {
            name: "github",
            authorize_url: "https://github.com/login/oauth/authorize",
            token_url: "https://github.com/login/oauth/access_token",
            scope: "read:user user:email",
        }),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Signed state (CSRF + carrying `redirect_to` across the provider round-trip)
// ---------------------------------------------------------------------------

fn b64url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .ok()
}

fn mac(secret: &str, data: &str) -> String {
    let mut m = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes()).unwrap();
    m.update(data.as_bytes());
    b64url(&m.finalize().into_bytes())
}

/// Sign the state payload so the callback can trust it without server-side
/// storage. Stateless by design: a control plane restart mid-flow still works,
/// and nothing accumulates for abandoned sign-ins.
pub fn sign_state(secret: &str, payload: &Value) -> String {
    let body = b64url(payload.to_string().as_bytes());
    let sig = mac(secret, &body);
    format!("{body}.{sig}")
}

/// Verify and decode a state blob. Constant-shape errors — the caller shows the
/// user a generic failure, never which check failed.
pub fn verify_state(secret: &str, state: &str, now: u64) -> Result<Value, String> {
    let (body, sig) = state.split_once('.').ok_or("malformed state")?;
    if mac(secret, body) != sig {
        return Err("state signature mismatch".into());
    }
    let raw = b64url_decode(body).ok_or("malformed state")?;
    let v: Value = serde_json::from_slice(&raw).map_err(|_| "malformed state")?;
    let exp = v.get("exp").and_then(|x| x.as_u64()).unwrap_or(0);
    if now > exp {
        return Err("state expired".into());
    }
    Ok(v)
}

pub fn state_payload(project_ref: &str, provider: &str, redirect_to: &str, now: u64) -> Value {
    json!({
        "ref": project_ref,
        "provider": provider,
        "redirect_to": redirect_to,
        "exp": now + STATE_TTL_SECS,
        "nonce": gen_nonce(),
    })
}

/// Random per-flow nonce, so two sign-ins started in the same second by the
/// same project never share a state blob.
fn gen_nonce() -> String {
    use rand::RngCore;
    let mut b = [0u8; 16];
    rand::rng().fill_bytes(&mut b);
    b64url(&b)
}

// ---------------------------------------------------------------------------
// Redirect allow-list
// ---------------------------------------------------------------------------

/// Is `target` an allowed place to hand a freshly minted session to?
///
/// This is the security-critical check of the whole flow: the callback appends
/// the access + refresh tokens to this URL, so an unchecked `redirect_to` is a
/// token-exfiltration hole dressed as an open redirect. A project must list its
/// URLs explicitly; an empty allow-list allows nothing.
///
/// An entry may end in `*` to allow a prefix (`https://app.example.com/*`).
/// The wildcard cannot widen the host: the literal part must still cover the
/// scheme and a full authority, so `https://*` and `https://evil.com*` are
/// rejected as entries rather than silently matching everything.
pub fn redirect_allowed(allow: &[String], target: &str) -> bool {
    if target.is_empty() || target.len() > 2048 {
        return false;
    }
    // No control characters (CR/LF would let a crafted target split headers).
    if target.chars().any(|c| c.is_control()) {
        return false;
    }
    if !(target.starts_with("http://") || target.starts_with("https://")) {
        return false;
    }
    allow.iter().any(|entry| {
        let entry = entry.trim();
        match entry.strip_suffix('*') {
            None => entry == target,
            Some(prefix) => {
                // The prefix must reach past the authority, i.e. contain the
                // "/" that ends the host, so a wildcard can never match a
                // different host.
                let after_scheme = prefix
                    .strip_prefix("https://")
                    .or_else(|| prefix.strip_prefix("http://"));
                match after_scheme {
                    Some(rest) if rest.contains('/') => target.starts_with(prefix),
                    _ => false,
                }
            }
        }
    })
}

/// Append the minted session to the app's redirect URL as a **fragment**, the
/// standard choice: fragments are not sent to servers, so the tokens stay out
/// of access logs, `Referer` headers and proxy caches.
pub fn redirect_with_session(
    redirect_to: &str,
    access: &str,
    refresh: &str,
    expires_in: u64,
) -> String {
    let sep = if redirect_to.contains('#') { "&" } else { "#" };
    format!(
        "{redirect_to}{sep}access_token={}&refresh_token={}&token_type=bearer&expires_in={expires_in}",
        urlencode(access),
        urlencode(refresh)
    )
}

pub fn redirect_with_error(redirect_to: &str, message: &str) -> String {
    let sep = if redirect_to.contains('#') { "&" } else { "#" };
    format!("{redirect_to}{sep}error={}", urlencode(message))
}

pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Build the provider's consent URL.
pub fn authorize_url(p: &Provider, client_id: &str, redirect_uri: &str, state: &str) -> String {
    let mut url = format!(
        "{}?client_id={}&redirect_uri={}&state={}&scope={}&response_type=code",
        p.authorize_url,
        urlencode(client_id),
        urlencode(redirect_uri),
        urlencode(state),
        urlencode(p.scope),
    );
    if p.name == "google" {
        // Ask for a fresh consent-free sign-in; we never use refresh tokens
        // from the provider (our own refresh token is the session handle).
        url.push_str("&access_type=online&prompt=select_account");
    }
    url
}

// ---------------------------------------------------------------------------
// Provider round-trips
// ---------------------------------------------------------------------------

/// Exchange an authorization code for the provider's token response.
fn exchange_code(
    p: &Provider,
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
) -> Result<Value, String> {
    let resp = ureq::post(p.token_url)
        .set("Accept", "application/json")
        .set("User-Agent", "oxibase")
        .send_form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("grant_type", "authorization_code"),
        ]);
    let body = match resp {
        Ok(r) => r.into_string().map_err(|e| e.to_string())?,
        Err(ureq::Error::Status(_, r)) => {
            // Providers describe the failure in the body; surface nothing of it
            // to the user, but keep it for the operator's log.
            let detail = r.into_string().unwrap_or_default();
            return Err(format!("code exchange rejected: {detail}"));
        }
        Err(e) => return Err(format!("could not reach the provider: {e}")),
    };
    let v: Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    if let Some(err) = v.get("error").and_then(|x| x.as_str()) {
        return Err(format!("code exchange failed: {err}"));
    }
    Ok(v)
}

/// Full authorization-code flow: code → tokens → a verified identity.
pub fn identity_from_code(
    p: &Provider,
    client_id: &str,
    client_secret: &str,
    code: &str,
    redirect_uri: &str,
) -> Result<Identity, String> {
    let tokens = exchange_code(p, client_id, client_secret, code, redirect_uri)?;
    match p.name {
        // Google returns an OIDC id_token in the exchange — the same artifact
        // the browser flow produces, so both paths converge on one verifier.
        "google" => {
            let id_token = tokens
                .get("id_token")
                .and_then(|x| x.as_str())
                .ok_or("no id_token in Google's response")?;
            verify_google_id_token(id_token, client_id)
        }
        "github" => {
            let access = tokens
                .get("access_token")
                .and_then(|x| x.as_str())
                .ok_or("no access_token in GitHub's response")?;
            github_identity(access)
        }
        other => Err(format!("unsupported provider {other}")),
    }
}

/// Verify a Google ID token by asking Google's `tokeninfo` endpoint (Google
/// checks the RS256 signature + expiry against its rotating keys), then apply
/// [`check_google_claims`] with the audience we expect.
pub fn verify_google_id_token(credential: &str, client_id: &str) -> Result<Identity, String> {
    let url = format!("https://oauth2.googleapis.com/tokeninfo?id_token={credential}");
    let body = match ureq::get(&url).call() {
        Ok(r) => r.into_string().map_err(|e| e.to_string())?,
        // Google returns 400 for a bad/expired token.
        Err(ureq::Error::Status(_, _)) => return Err("invalid Google credential".into()),
        Err(e) => return Err(format!("could not reach Google to verify: {e}")),
    };
    let v: Value = serde_json::from_str(&body).map_err(|e| e.to_string())?;
    check_google_claims(&v, client_id)
}

/// Pure validation of the claims Google returns for an ID token: the audience
/// must be the client ID we expect, the issuer must be Google, and the email
/// must be verified. Split out from the network fetch so it is unit-testable.
pub fn check_google_claims(v: &Value, client_id: &str) -> Result<Identity, String> {
    if v.get("aud").and_then(|x| x.as_str()) != Some(client_id) {
        return Err("token audience mismatch".into());
    }
    let iss = v.get("iss").and_then(|x| x.as_str()).unwrap_or("");
    if iss != "accounts.google.com" && iss != "https://accounts.google.com" {
        return Err("unexpected token issuer".into());
    }
    let verified = match v.get("email_verified") {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => s == "true",
        _ => false,
    };
    if !verified {
        return Err("Google email is not verified".into());
    }
    let email = v
        .get("email")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .ok_or("no email in Google token")?
        .to_lowercase();
    let sub = v
        .get("sub")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string();
    let name = v.get("name").and_then(|x| x.as_str()).map(String::from);
    Ok(Identity {
        provider: "google".into(),
        subject: sub,
        email,
        name,
    })
}

/// GitHub has no ID token: fetch the user, then their addresses, and take the
/// **primary verified** one. `/user`'s own `email` field is the public profile
/// email — it can be unverified or absent, so it is not trustworthy here.
fn github_identity(access_token: &str) -> Result<Identity, String> {
    let user = github_get("https://api.github.com/user", access_token)?;
    let subject = user
        .get("id")
        .map(|v| v.to_string())
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    let name = user
        .get("name")
        .and_then(|x| x.as_str())
        .or_else(|| user.get("login").and_then(|x| x.as_str()))
        .map(String::from);
    let emails = github_get("https://api.github.com/user/emails", access_token)?;
    let email = pick_github_email(&emails).ok_or(
        "no verified primary email on the GitHub account — verify one on GitHub and try again",
    )?;
    Ok(Identity {
        provider: "github".into(),
        subject,
        email,
        name,
    })
}

fn github_get(url: &str, access_token: &str) -> Result<Value, String> {
    let resp = ureq::get(url)
        .set("Authorization", &format!("Bearer {access_token}"))
        .set("Accept", "application/vnd.github+json")
        // GitHub rejects requests without a User-Agent.
        .set("User-Agent", "oxibase")
        .call();
    let body = match resp {
        Ok(r) => r.into_string().map_err(|e| e.to_string())?,
        Err(ureq::Error::Status(code, _)) => {
            return Err(format!("GitHub rejected the request ({code})"));
        }
        Err(e) => return Err(format!("could not reach GitHub: {e}")),
    };
    serde_json::from_str(&body).map_err(|e| e.to_string())
}

/// The primary verified address, else any verified one. Never an unverified
/// address — that is the account-takeover vector.
pub fn pick_github_email(emails: &Value) -> Option<String> {
    let list = emails.as_array()?;
    let verified = |e: &&Value| e.get("verified").and_then(|v| v.as_bool()) == Some(true);
    let addr = |e: &Value| {
        e.get("email")
            .and_then(|v| v.as_str())
            .map(|s| s.to_lowercase())
    };
    list.iter()
        .find(|e| verified(e) && e.get("primary").and_then(|v| v.as_bool()) == Some(true))
        .and_then(addr)
        .or_else(|| list.iter().find(verified).and_then(addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn state_round_trips_and_rejects_tampering() {
        let payload = json!({ "ref": "abc", "redirect_to": "https://app.test/cb", "exp": 100 });
        let s = sign_state("secret", &payload);
        assert_eq!(
            verify_state("secret", &s, 50).unwrap()["redirect_to"],
            json!("https://app.test/cb")
        );
        // Wrong key, edited payload, and expiry all fail.
        assert!(verify_state("other", &s, 50).is_err());
        let (body, sig) = s.split_once('.').unwrap();
        let evil = json!({ "redirect_to": "https://evil.test", "exp": 100 });
        let forged = format!("{}.{sig}", b64url(evil.to_string().as_bytes()));
        assert!(verify_state("secret", &forged, 50).is_err());
        assert!(verify_state("secret", &format!("{body}.{sig}"), 200).is_err());
    }

    #[test]
    fn redirect_allowlist_is_exact_or_bounded_prefix() {
        let list = allow(&["https://app.test/callback", "https://app.test/auth/*"]);
        assert!(redirect_allowed(&list, "https://app.test/callback"));
        assert!(redirect_allowed(&list, "https://app.test/auth/done?x=1"));
        // Not listed, wrong scheme, or a look-alike host.
        assert!(!redirect_allowed(&list, "https://app.test/other"));
        assert!(!redirect_allowed(&list, "http://app.test/callback"));
        assert!(!redirect_allowed(
            &list,
            "https://app.test.evil.com/callback"
        ));
        assert!(!redirect_allowed(
            &list,
            "https://evil.com/#https://app.test/callback"
        ));
        // An empty allow-list allows nothing.
        assert!(!redirect_allowed(&[], "https://app.test/callback"));
    }

    #[test]
    fn wildcard_cannot_widen_the_host() {
        // These entries would be catastrophic if the wildcard were naive.
        assert!(!redirect_allowed(
            &allow(&["https://*"]),
            "https://evil.com/x"
        ));
        assert!(!redirect_allowed(
            &allow(&["https://app.test*"]),
            "https://app.test.evil.com/x"
        ));
        assert!(!redirect_allowed(&allow(&["*"]), "https://evil.com/x"));
    }

    #[test]
    fn control_characters_are_rejected() {
        let list = allow(&["https://app.test/*"]);
        assert!(!redirect_allowed(
            &list,
            "https://app.test/a\r\nSet-Cookie: x=1"
        ));
    }

    #[test]
    fn session_lands_in_the_fragment_not_the_query() {
        let u = redirect_with_session(
            "https://app.test/cb?next=/home",
            "acc.ess",
            "ref+resh",
            3600,
        );
        assert!(u.starts_with("https://app.test/cb?next=/home#access_token=acc.ess"));
        assert!(u.contains("refresh_token=ref%2Bresh"));
    }

    #[test]
    fn google_claims_accept_verified_matching_audience() {
        let v = json!({
            "aud": "cid.apps.googleusercontent.com",
            "iss": "https://accounts.google.com",
            "email": "User@Example.com",
            "email_verified": "true",
            "sub": "123",
        });
        let id = check_google_claims(&v, "cid.apps.googleusercontent.com").unwrap();
        assert_eq!(id.email, "user@example.com");
        assert_eq!(id.provider, "google");
        // A token minted for a different client must not be accepted.
        assert!(check_google_claims(&v, "other-client").is_err());
    }

    #[test]
    fn google_claims_reject_unverified_email() {
        let v = json!({
            "aud": "cid", "iss": "accounts.google.com",
            "email": "user@example.com", "email_verified": false, "sub": "1",
        });
        assert!(check_google_claims(&v, "cid").is_err());
    }

    #[test]
    fn github_email_prefers_primary_verified_and_never_unverified() {
        let emails = json!([
            { "email": "Alt@example.com", "primary": false, "verified": true },
            { "email": "Main@example.com", "primary": true, "verified": true },
        ]);
        assert_eq!(pick_github_email(&emails).unwrap(), "main@example.com");

        let unverified = json!([{ "email": "x@example.com", "primary": true, "verified": false }]);
        assert!(pick_github_email(&unverified).is_none());
    }
}
