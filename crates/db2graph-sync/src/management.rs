//! Management HTTP API (feature `service`).
//!
//! [`router`] returns an `axum::Router`; the library never binds a socket.
//! The standalone binary serves it on `management.listen`; an embedding
//! application may mount it in its own server behind its own TLS.
//!
//! * Authentication: `Authorization: Bearer <token>`; only the SHA-256 of
//!   each token is configured, compared in constant time.
//! * Authorization: scopes `read`, `operate`, `recover` per token.
//! * Every response carries `x-correlation-id` (echoed or generated).
//! * Errors are JSON `{ "code": <ErrorKind>, "message", "correlation_id" }`.
//! * Operations are idempotent; an `Idempotency-Key` header is echoed.
//! * Invalid requests are rejected without affecting synchronization.
//! * There is no endpoint that executes arbitrary database queries.
//!
//! The contract is `api/openapi.json` in this crate (version `v1`).

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::config::{ManagementToken, Scope};
use crate::error::ErrorKind;
use crate::handle::{Control, Phase};
use crate::policy::Principal;
use crate::retrieval::{Hit, VersionRule};

#[derive(Clone)]
struct AppState {
    control: Control,
    tokens: Arc<Vec<ManagementToken>>,
}

#[derive(Serialize)]
struct ApiError {
    code: ErrorKind,
    message: String,
    correlation_id: String,
}

fn correlation(headers: &HeaderMap) -> String {
    headers
        .get("x-correlation-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty() && v.len() <= 128 && v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let mut b = [0u8; 12];
            let _ = getrandom::fill(&mut b);
            hex::encode(b)
        })
}

fn error(status: StatusCode, code: ErrorKind, message: impl Into<String>, cid: &str) -> Response {
    (status, axum::Json(ApiError { code, message: message.into(), correlation_id: cid.to_owned() })).into_response()
}

#[derive(Clone)]
struct Cid(String);

async fn correlation_layer(mut req: Request, next: Next) -> Response {
    let cid = correlation(req.headers());
    req.extensions_mut().insert(Cid(cid.clone()));
    let idem = req.headers().get("idempotency-key").cloned();
    let mut resp = next.run(req).await;
    if let Ok(v) = HeaderValue::from_str(&cid) {
        resp.headers_mut().insert("x-correlation-id", v);
    }
    if let Some(k) = idem {
        resp.headers_mut().insert("idempotency-key", k);
    }
    resp
}

// The error is the ready-to-send HTTP response; it is returned straight to axum.
#[allow(clippy::result_large_err)]
fn authorize(state: &AppState, headers: &HeaderMap, scope: Scope, cid: &str) -> Result<String, Response> {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, ErrorKind::Unauthorized, "bearer token required", cid))?;
    let digest = hex::encode(Sha256::digest(token.as_bytes()));
    let mut found: Option<&ManagementToken> = None;
    for t in state.tokens.iter() {
        // Constant-time comparison; iterate all tokens regardless of match.
        if bool::from(t.sha256.to_ascii_lowercase().as_bytes().ct_eq(digest.as_bytes())) {
            found = Some(t);
        }
    }
    let t = found.ok_or_else(|| error(StatusCode::UNAUTHORIZED, ErrorKind::Unauthorized, "invalid token", cid))?;
    if !t.scopes.contains(&scope) {
        return Err(error(StatusCode::FORBIDDEN, ErrorKind::Unauthorized, format!("token lacks scope {scope:?}"), cid));
    }
    Ok(t.name.clone())
}

fn cid_of(req_ext: &axum::http::Extensions) -> String {
    req_ext.get::<Cid>().map(|c| c.0.clone()).unwrap_or_default()
}

/// Build the management router for a running engine.
pub fn router(control: Control, tokens: Vec<ManagementToken>) -> Router {
    let state = AppState { control, tokens: Arc::new(tokens) };
    Router::new()
        .route("/v1/about", get(about))
        .route("/v1/health/live", get(live))
        .route("/v1/health/ready", get(ready))
        .route("/v1/status", get(status))
        .route("/v1/metrics", get(metrics))
        .route("/v1/pause", post(pause))
        .route("/v1/resume", post(resume))
        .route("/v1/mapping/approve", post(approve))
        .route("/v1/retrieval/validate", post(validate))
        .layer(middleware::from_fn(correlation_layer))
        .with_state(state)
}

async fn about() -> impl IntoResponse {
    axum::Json(crate::about())
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn ready(State(s): State<AppState>) -> Response {
    let st = s.control.status();
    if st.ready {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, format!("{:?}", st.phase).to_lowercase()).into_response()
    }
}

async fn status(State(s): State<AppState>, req: Request) -> Response {
    let cid = cid_of(req.extensions());
    if let Err(r) = authorize(&s, req.headers(), Scope::Read, &cid) {
        return r;
    }
    axum::Json(s.control.status()).into_response()
}

async fn metrics(State(s): State<AppState>, req: Request) -> Response {
    let cid = cid_of(req.extensions());
    if let Err(r) = authorize(&s, req.headers(), Scope::Read, &cid) {
        return r;
    }
    let st = s.control.status();
    let mut out = String::new();
    let mut g = |name: &str, help: &str, v: f64| {
        let kind = if name.ends_with("_total") { "counter" } else { "gauge" };
        out.push_str(&format!("# HELP d2g_{name} {help}\n# TYPE d2g_{name} {kind}\nd2g_{name}{{pipeline=\"{}\"}} {v}\n", st.pipeline));
    };
    g("ready", "1 when live replication is established", if st.ready { 1.0 } else { 0.0 });
    g("failed", "1 after a terminal failure", if st.failure.is_some() { 1.0 } else { 0.0 });
    g("units_captured_total", "source units journaled", st.counters.units_captured as f64);
    g("units_applied_total", "units applied to the graph", st.counters.units_applied as f64);
    g("duplicates_total", "replayed units recognized by receipt", st.counters.duplicates as f64);
    g("graph_retries_total", "retried graph batches", st.counters.graph_retries as f64);
    g("source_reconnects_total", "source reconnects", st.counters.source_reconnects as f64);
    g("journal_backlog", "journaled units not yet applied", st.backlog.journal_units as f64);
    g("vector_backlog", "vector tasks pending", st.backlog.vector_tasks as f64);
    g("pending_edges", "relationships awaiting an endpoint", st.backlog.pending_edges as f64);
    g("audit_spool", "audit records awaiting forwarding", st.backlog.audit_spool as f64);
    if let Some(l) = st.freshness.last_lag_ms {
        g("last_lag_ms", "commit-to-graph lag of the last applied transaction", l as f64);
    }
    ([("content-type", "text/plain; version=0.0.4")], out).into_response()
}

#[derive(Serialize)]
struct Ack {
    ok: bool,
    phase: Phase,
}

async fn pause(State(s): State<AppState>, req: Request) -> Response {
    let cid = cid_of(req.extensions());
    let who = match authorize(&s, req.headers(), Scope::Operate, &cid) {
        Ok(w) => w,
        Err(r) => return r,
    };
    tracing::info!(token = %who, correlation_id = %cid, "management: pause");
    s.control.pause();
    axum::Json(Ack { ok: true, phase: s.control.status().phase }).into_response()
}

async fn resume(State(s): State<AppState>, req: Request) -> Response {
    let cid = cid_of(req.extensions());
    let who = match authorize(&s, req.headers(), Scope::Operate, &cid) {
        Ok(w) => w,
        Err(r) => return r,
    };
    tracing::info!(token = %who, correlation_id = %cid, "management: resume");
    s.control.resume();
    axum::Json(Ack { ok: true, phase: s.control.status().phase }).into_response()
}

#[derive(Deserialize)]
struct ApproveBody {
    digest: String,
}

async fn approve(State(s): State<AppState>, req: Request) -> Response {
    let cid = cid_of(req.extensions());
    let who = match authorize(&s, req.headers(), Scope::Operate, &cid) {
        Ok(w) => w,
        Err(r) => return r,
    };
    let body = match axum::body::to_bytes(req.into_body(), 64 * 1024).await {
        Ok(b) => b,
        Err(_) => return error(StatusCode::BAD_REQUEST, ErrorKind::InvalidRequest, "body too large", &cid),
    };
    let parsed: ApproveBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return error(StatusCode::BAD_REQUEST, ErrorKind::InvalidRequest, format!("invalid body: {e}"), &cid),
    };
    if parsed.digest.len() != 32 || !parsed.digest.chars().all(|c| c.is_ascii_hexdigit()) {
        return error(StatusCode::BAD_REQUEST, ErrorKind::InvalidRequest, "digest must be 32 hex characters", &cid);
    }
    tracing::info!(token = %who, correlation_id = %cid, digest = %parsed.digest, "management: approve mapping");
    s.control.approve_mapping(&parsed.digest);
    axum::Json(Ack { ok: true, phase: s.control.status().phase }).into_response()
}

#[derive(Deserialize)]
struct ValidateBody {
    principal: Principal,
    hits: Vec<Hit>,
    #[serde(default)]
    version_rule: VersionRule,
}

async fn validate(State(s): State<AppState>, req: Request) -> Response {
    let cid = cid_of(req.extensions());
    if let Err(r) = authorize(&s, req.headers(), Scope::Read, &cid) {
        return r;
    }
    let body = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => return error(StatusCode::BAD_REQUEST, ErrorKind::InvalidRequest, "body too large", &cid),
    };
    let parsed: ValidateBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return error(StatusCode::BAD_REQUEST, ErrorKind::InvalidRequest, format!("invalid body: {e}"), &cid),
    };
    if parsed.hits.len() > 1000 {
        return error(StatusCode::BAD_REQUEST, ErrorKind::InvalidRequest, "at most 1000 hits per request", &cid);
    }
    match s.control.retrieval_validator().validate(&parsed.principal, &parsed.hits, parsed.version_rule).await {
        Ok(v) => axum::Json(serde_json::json!({ "verdicts": v })).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e.kind(), e.message(), &cid),
    }
}
