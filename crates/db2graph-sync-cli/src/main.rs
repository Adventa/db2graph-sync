//! `db2graph-sync` — standalone binary and operator CLI.
//!
//! db2graph-sync — an open-source project by Adventa. https://www.adventa.com.au
//!
//! The binary owns its process, so (unlike the library) it installs a
//! logger and signal handlers. Exit codes are documented in
//! `docs/reference/errors.md`.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use db2graph_sync::capability::CheckStatus;
use db2graph_sync::{Config, Error, ErrorKind, StartOptions};
use sha2::{Digest, Sha256};

#[derive(Parser)]
#[command(
    name = "db2graph-sync",
    version,
    about = "One-way CDC projection from operational databases into graph databases and vector indexes.",
    after_help = "db2graph-sync — an open-source project by Adventa. https://www.adventa.com.au"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Validate configuration offline (no network access).
    Validate(Cfg),
    /// Check source and graph prerequisites.
    Preflight(Cfg),
    /// Discover the schema and print the mapping plan and its digest.
    Plan {
        #[command(flatten)]
        cfg: Cfg,
        #[arg(long)]
        json: bool,
    },
    /// Create graph constraints/indexes and the vector collection (explicit operator step).
    Bootstrap(Cfg),
    /// Print the statements an administrator runs to enable capture (never executed).
    SourceSetup(Cfg),
    /// Run the engine in the foreground (standalone mode).
    Run {
        #[command(flatten)]
        cfg: Cfg,
        #[arg(long, default_value = "text", value_parser = ["text", "json"])]
        log_format: String,
        /// Seconds to drain journaled work on shutdown.
        #[arg(long, default_value_t = 30)]
        drain_secs: u64,
    },
    /// Remote: status of a running engine.
    Status(Remote),
    /// Remote: pause capture and application.
    Pause(Remote),
    /// Remote: resume.
    Resume(Remote),
    /// Remote: approve a mapping plan digest.
    ApproveMapping {
        #[command(flatten)]
        remote: Remote,
        #[arg(long)]
        digest: String,
    },
    /// Compare source and graph (counts + verified readback hashes). Read-only.
    Reconcile {
        #[command(flatten)]
        cfg: Cfg,
        #[arg(long, default_value_t = 100)]
        sample: u32,
    },
    /// Export an evidence document (JSON).
    Evidence {
        #[command(flatten)]
        cfg: Cfg,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 0)]
        sample: u32,
    },
    /// Authorized recovery operations (engine must be stopped).
    Recover {
        #[command(flatten)]
        cfg: Cfg,
        /// Accept the graph target's progress evidence after state loss/restore.
        #[arg(long)]
        accept_state_reset: bool,
        /// Accept a changed source identity (restore/failover to a different
        /// history): new source epoch, fresh snapshot.
        #[arg(long)]
        accept_source_change: bool,
        /// Reason recorded in the audit log.
        #[arg(long)]
        reason: String,
    },
    /// Re-encrypt durable state under the active key (engine must be stopped).
    RotateKeys(Cfg),
    /// Verify the audit file's HMAC chain.
    AuditVerify {
        #[command(flatten)]
        cfg: Cfg,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Print a new random 32-byte state key (base64) for a secret store.
    Keygen,
    /// Read a management token from stdin and print its SHA-256 for configuration.
    TokenHash,
    /// Print attribution, version and compiled features.
    About,
}

#[derive(clap::Args)]
struct Cfg {
    /// Configuration file (TOML).
    #[arg(short, long, env = "D2G_CONFIG")]
    config: PathBuf,
}

#[derive(clap::Args)]
struct Remote {
    /// Base URL of the management API, e.g. https://sync.internal:8443
    #[arg(long, env = "D2G_URL")]
    url: String,
    /// File containing the bearer token.
    #[arg(long, env = "D2G_TOKEN_FILE")]
    token_file: PathBuf,
    /// Extra CA certificate (PEM) for the management endpoint.
    #[arg(long)]
    ca_file: Option<PathBuf>,
}

fn load(c: &Cfg) -> Result<Config, Error> {
    Config::from_file(&c.config)
}

fn fail(e: &Error) -> ExitCode {
    eprintln!("error [{}]: {}", e.kind(), e.message());
    ExitCode::from(e.kind().exit_code() as u8)
}

fn print_reports(reports: &[db2graph_sync::capability::PreflightReport]) -> bool {
    let mut ok = true;
    for r in reports {
        println!("{}", r.component);
        for c in &r.checks {
            let mark = match c.status {
                CheckStatus::Pass => "PASS",
                CheckStatus::Warn => "WARN",
                CheckStatus::Fail => {
                    ok = false;
                    "FAIL"
                }
                CheckStatus::NotExecuted => "NOT-EXECUTED",
            };
            println!("  [{mark}] {}: {}", c.name, c.detail);
        }
    }
    ok
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(e) => {
            let code = if e.use_stderr() { 64 } else { 0 };
            let _ = e.print();
            return ExitCode::from(code);
        }
    };
    match run(cli).await {
        Ok(code) => code,
        Err(e) => fail(&e),
    }
}

async fn run(cli: Cli) -> Result<ExitCode, Error> {
    match cli.cmd {
        Cmd::Validate(c) => {
            let cfg = load(&c)?;
            let findings = cfg.findings();
            if findings.is_empty() {
                println!("configuration valid (profile {:?})", cfg.profile());
                return Ok(ExitCode::SUCCESS);
            }
            for f in &findings {
                println!("[{}] {}: {}", f.kind, f.path, f.message);
            }
            let kind = findings.iter().map(|f| f.kind).find(|k| *k != ErrorKind::Config).unwrap_or(ErrorKind::Config);
            Ok(ExitCode::from(kind.exit_code() as u8))
        }
        Cmd::Preflight(c) => {
            let cfg = load(&c)?;
            cfg.validate()?;
            let reports = db2graph_sync::admin::preflight(&cfg, StartOptions::default()).await?;
            Ok(if print_reports(&reports) { ExitCode::SUCCESS } else { ExitCode::from(3) })
        }
        Cmd::Plan { cfg: c, json } => {
            let cfg = load(&c)?;
            let (plan, digest) = db2graph_sync::admin::plan(&cfg, StartOptions::default()).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&serde_json::json!({"digest": digest, "plan": plan})).unwrap_or_default());
            } else {
                println!("mapping plan digest: {digest}");
                println!("schema version: {}", plan.schema_version);
                for r in plan.relations.values() {
                    println!("  {} -> :{} key {:?}", r.relation, r.labels.join(":"), r.key_columns);
                    for e in &r.edges {
                        println!("      -[:{}]-> {} via {:?} ({})", e.rel_type, e.target.relation, e.columns, e.constraint);
                    }
                }
                for j in plan.join_tables.values() {
                    println!("  {} -> relationship :{}", j.relation, j.rel_type);
                }
                for (r, why) in &plan.excluded {
                    println!("  excluded {r}: {why}");
                }
                let approved = cfg.mapping.approved_digest.as_deref() == Some(digest.as_str());
                println!("approved: {approved} (set mapping.approved_digest = \"{digest}\" to approve)");
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Bootstrap(c) => {
            let cfg = load(&c)?;
            cfg.validate()?;
            let digest = db2graph_sync::admin::bootstrap_graph(&cfg, StartOptions::default()).await?;
            println!("graph constraints and indexes ready for plan {digest}");
            if let Some(msg) = db2graph_sync::admin::bootstrap_vector(&cfg).await? {
                println!("{msg}");
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::SourceSetup(c) => {
            let cfg = load(&c)?;
            for s in db2graph_sync::admin::source_setup_statements(&cfg)? {
                println!("{s}");
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Run { cfg: c, log_format, drain_secs } => run_engine(load(&c)?, &log_format, Duration::from_secs(drain_secs)).await,
        Cmd::Status(r) => remote(&r, reqwest::Method::GET, "/v1/status", None).await,
        Cmd::Pause(r) => remote(&r, reqwest::Method::POST, "/v1/pause", None).await,
        Cmd::Resume(r) => remote(&r, reqwest::Method::POST, "/v1/resume", None).await,
        Cmd::ApproveMapping { remote: r, digest } => {
            remote(&r, reqwest::Method::POST, "/v1/mapping/approve", Some(serde_json::json!({ "digest": digest }))).await
        }
        Cmd::Reconcile { cfg: c, sample } => {
            let cfg = load(&c)?;
            let report = db2graph_sync::admin::reconcile(&cfg, StartOptions::default(), sample).await?;
            println!("{}", serde_json::to_string_pretty(&report).unwrap_or_default());
            Ok(if report.consistent { ExitCode::SUCCESS } else { ExitCode::from(7) })
        }
        Cmd::Evidence { cfg: c, out, sample } => {
            let cfg = load(&c)?;
            let doc = db2graph_sync::admin::evidence(&cfg, StartOptions::default(), sample).await?;
            std::fs::write(&out, serde_json::to_vec_pretty(&doc).unwrap_or_default())
                .map_err(|e| Error::new(ErrorKind::Config, format!("cannot write {}: {e}", out.display())))?;
            println!("evidence written to {}", out.display());
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Recover { cfg: c, accept_state_reset, accept_source_change, reason } => {
            if accept_state_reset == accept_source_change {
                return Err(Error::new(
                    ErrorKind::InvalidRequest,
                    "specify exactly one recovery action: --accept-state-reset or --accept-source-change",
                ));
            }
            if reason.trim().len() < 8 {
                return Err(Error::new(ErrorKind::InvalidRequest, "--reason must describe the incident (min 8 characters)"));
            }
            let cfg = load(&c)?;
            if accept_state_reset {
                let epoch = db2graph_sync::admin::accept_state_reset(&cfg, StartOptions::default(), &reason).await?;
                println!("state epoch advanced to {epoch}; journaled work will replay idempotently on next start");
            } else {
                let epoch = db2graph_sync::admin::accept_source_change(&cfg, StartOptions::default(), &reason).await?;
                println!("source epoch advanced to {epoch}; the next start takes a new snapshot of the new source history");
            }
            Ok(ExitCode::SUCCESS)
        }
        Cmd::RotateKeys(c) => {
            let cfg = load(&c)?;
            let n = db2graph_sync::admin::rotate_state_keys(&cfg, StartOptions::default()).await?;
            println!("re-encrypted {n} record(s) under key {}", cfg.state.active_key);
            Ok(ExitCode::SUCCESS)
        }
        Cmd::AuditVerify { cfg: c, file } => {
            let cfg = load(&c)?;
            let path = file
                .or_else(|| cfg.audit.file.clone())
                .ok_or_else(|| Error::new(ErrorKind::Config, "no audit file configured or given"))?;
            let n = db2graph_sync::admin::verify_audit(&cfg, &path).await?;
            println!("audit chain verified: {n} record(s)");
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Keygen => {
            use base64::Engine;
            let mut k = [0u8; 32];
            getrandom::fill(&mut k).map_err(|e| Error::internal(e.to_string()))?;
            println!("{}", base64::engine::general_purpose::STANDARD.encode(k));
            Ok(ExitCode::SUCCESS)
        }
        Cmd::TokenHash => {
            let mut s = String::new();
            std::io::stdin().read_line(&mut s).map_err(|e| Error::internal(e.to_string()))?;
            println!("{}", hex::encode(Sha256::digest(s.trim_end_matches(['\r', '\n']).as_bytes())));
            Ok(ExitCode::SUCCESS)
        }
        Cmd::About => {
            println!("{}", serde_json::to_string_pretty(&db2graph_sync::about()).unwrap_or_default());
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn tls_client(ca: Option<&Path>) -> Result<reqwest::Client, Error> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(c);
    }
    if let Some(p) = ca {
        for c in CertificateDer::pem_file_iter(p).map_err(|e| Error::new(ErrorKind::Security, e.to_string()))? {
            roots
                .add(c.map_err(|e| Error::new(ErrorKind::Security, e.to_string()))?)
                .map_err(|e| Error::new(ErrorKind::Security, e.to_string()))?;
        }
    }
    let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::new(ErrorKind::Security, e.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    reqwest::Client::builder()
        .use_preconfigured_tls(cfg)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| Error::internal(e.to_string()))
}

async fn remote(r: &Remote, method: reqwest::Method, path: &str, body: Option<serde_json::Value>) -> Result<ExitCode, Error> {
    let token =
        std::fs::read_to_string(&r.token_file).map_err(|e| Error::new(ErrorKind::Secret, format!("cannot read token file: {e}")))?;
    let client = tls_client(r.ca_file.as_deref())?;
    let mut rb = client.request(method, format!("{}{path}", r.url.trim_end_matches('/'))).bearer_auth(token.trim());
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    let resp = rb.send().await.map_err(|e| Error::source(format!("request failed: {e}")))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let pretty =
        serde_json::from_str::<serde_json::Value>(&text).map(|v| serde_json::to_string_pretty(&v).unwrap_or_default()).unwrap_or(text);
    println!("{pretty}");
    Ok(match status.as_u16() {
        200..=299 => ExitCode::SUCCESS,
        401 | 403 => ExitCode::from(5),
        400 => ExitCode::from(64),
        _ => ExitCode::from(4),
    })
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = term => {} }
}

fn init_logging(format: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_env("D2G_LOG").unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let b = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    if format == "json" {
        b.json().init();
    } else {
        b.init();
    }
}

async fn serve_management(cfg: &Config, control: db2graph_sync::Control, stop: tokio::sync::watch::Receiver<bool>) -> Result<(), Error> {
    let Some(addr) = cfg.management.listen.clone() else { return Ok(()) };
    let router = db2graph_sync::management::router(control, cfg.management.tokens.clone());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| Error::new(ErrorKind::Config, format!("cannot bind management listener {addr}: {e}")))?;
    let acceptor = match (&cfg.management.tls_cert, &cfg.management.tls_key) {
        (Some(cert), Some(key)) => Some(tls_acceptor(cert, key)?),
        _ => None,
    };
    tracing::info!(%addr, tls = acceptor.is_some(), "management API listening");
    tokio::spawn(async move {
        let mut stop = stop;
        loop {
            let (stream, _) = tokio::select! {
                r = listener.accept() => match r { Ok(v) => v, Err(_) => continue },
                _ = stop.changed() => return,
            };
            let router = router.clone();
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let svc = hyper_util::service::TowerToHyperService::new(router);
                let builder = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
                match acceptor {
                    Some(a) => {
                        if let Ok(tls) = a.accept(stream).await {
                            let _ = builder.serve_connection(hyper_util::rt::TokioIo::new(tls), svc).await;
                        }
                    }
                    None => {
                        let _ = builder.serve_connection(hyper_util::rt::TokioIo::new(stream), svc).await;
                    }
                }
            });
        }
    });
    Ok(())
}

fn tls_acceptor(cert: &Path, key: &Path) -> Result<tokio_rustls::TlsAcceptor, Error> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let sec = |e: String| Error::new(ErrorKind::Security, e);
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .map_err(|e| sec(format!("management certificate: {e}")))?
        .collect::<Result<_, _>>()
        .map_err(|e| sec(format!("management certificate: {e}")))?;
    let key = PrivateKeyDer::from_pem_file(key).map_err(|e| sec(format!("management key: {e}")))?;
    let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| sec(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| sec(format!("management TLS: {e}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(cfg)))
}

async fn run_engine(cfg: Config, log_format: &str, drain: Duration) -> Result<ExitCode, Error> {
    init_logging(log_format);
    tracing::info!(pipeline = %cfg.pipeline.id, profile = ?cfg.profile(), "starting db2graph-sync — an open-source project by Adventa");
    let sync = db2graph_sync::start(cfg.clone()).await?;
    let control = sync.control();
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    serve_management(&cfg, control.clone(), stop_rx).await?;

    let mut status = control.subscribe();
    let failure = async move {
        loop {
            if let Some(f) = status.borrow_and_update().failure.clone() {
                return f;
            }
            if status.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    };
    let outcome = tokio::select! {
        _ = shutdown_signal() => {
            tracing::info!("shutdown requested; draining");
            let report = sync.shutdown(drain).await;
            let _ = stop_tx.send(true);
            match report {
                Ok(r) => {
                    tracing::info!(drained = r.drained, remaining = r.remaining_journal_units, "stopped");
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => Err(e),
            }
        }
        f = failure => {
            let _ = stop_tx.send(true);
            let _ = sync.wait().await;
            Err(Error::new(f.kind, f.message))
        }
    };
    outcome
}
