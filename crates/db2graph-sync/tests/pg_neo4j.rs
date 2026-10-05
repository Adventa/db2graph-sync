//! PostgreSQL -> Neo4j integration tests against real servers.
//!
//! Requires:
//!   D2G_IT_PG_HOST, D2G_IT_PG_PORT, D2G_IT_PG_USER, D2G_IT_PG_PASSWORD, D2G_IT_PG_DB
//!   D2G_IT_NEO4J_URI (bolt://...), D2G_IT_NEO4J_USER, D2G_IT_NEO4J_PASSWORD
//!
//! `scripts/integration.ps1` / `scripts/integration.sh` start the services
//! (deploy/test/docker-compose.yml) and set these. When the variables are
//! missing each test prints `UNEXECUTED:` and returns; the qualification
//! scripts and CI treat any `UNEXECUTED` line as a failure, so this is never
//! reported as passing coverage.
#![cfg(all(feature = "postgres", feature = "neo4j"))]

use std::time::Duration;

use db2graph_sync::config::Config;
use db2graph_sync::error::ErrorKind;
use db2graph_sync::{Phase, StartOptions};
use tokio_postgres::NoTls;

const T: Duration = Duration::from_secs(60);

struct Env {
    pg_host: String,
    pg_port: u16,
    pg_user: String,
    pg_password: String,
    pg_db: String,
    neo_uri: String,
    neo_user: String,
    neo_password: String,
}

fn env() -> Option<Env> {
    let v = |k: &str| std::env::var(k).ok();
    Some(Env {
        pg_host: v("D2G_IT_PG_HOST")?,
        pg_port: v("D2G_IT_PG_PORT")?.parse().ok()?,
        pg_user: v("D2G_IT_PG_USER")?,
        pg_password: v("D2G_IT_PG_PASSWORD")?,
        pg_db: v("D2G_IT_PG_DB")?,
        neo_uri: v("D2G_IT_NEO4J_URI")?,
        neo_user: v("D2G_IT_NEO4J_USER")?,
        neo_password: v("D2G_IT_NEO4J_PASSWORD")?,
    })
}

macro_rules! require_env {
    ($name:expr) => {
        match env() {
            Some(e) => e,
            None => {
                println!("UNEXECUTED: {} (integration environment variables not set)", $name);
                return;
            }
        }
    };
}

async fn pg(e: &Env) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(
        &format!("host={} port={} user={} password={} dbname={}", e.pg_host, e.pg_port, e.pg_user, e.pg_password, e.pg_db),
        NoTls,
    )
    .await
    .expect("connect to test postgres");
    tokio::spawn(conn);
    c
}

fn suffix() -> String {
    format!("{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() % 0xffff_ffff)
}

/// Synthetic schema exercising composite keys, self references, multiple FKs
/// between the same tables, a unique-key reference, and a join table.
async fn setup_schema(c: &tokio_postgres::Client, s: &str) {
    c.batch_execute(&format!(
        r#"
CREATE SCHEMA {s};
CREATE TABLE {s}.customers (id bigint PRIMARY KEY, name text NOT NULL, email text UNIQUE, notes text, ssn text);
CREATE TABLE {s}.employees (id int PRIMARY KEY, name text, manager_id int REFERENCES {s}.employees(id));
CREATE TABLE {s}.orders (
  id bigint PRIMARY KEY,
  customer_id bigint REFERENCES {s}.customers(id) DEFERRABLE INITIALLY DEFERRED,
  billing_customer_id bigint REFERENCES {s}.customers(id),
  billing_email text REFERENCES {s}.customers(email),
  total numeric(12,2),
  placed_at timestamptz DEFAULT '2024-01-02 03:04:05+00');
CREATE TABLE {s}.order_lines (
  order_id bigint REFERENCES {s}.orders(id),
  line_no int,
  sku text,
  qty int,
  PRIMARY KEY (order_id, line_no));
CREATE TABLE {s}.tags (id int PRIMARY KEY, label text);
CREATE TABLE {s}.order_tags (id int PRIMARY KEY, order_id bigint REFERENCES {s}.orders(id), tag_id int REFERENCES {s}.tags(id), weight int);
INSERT INTO {s}.customers VALUES (1, 'Ada', 'ada@example.test', 'n1', '000-00-0001'), (2, 'Grace', 'grace@example.test', NULL, NULL);
INSERT INTO {s}.employees VALUES (1, 'Root', NULL), (2, 'Report', 1), (3, 'Self', 3);
INSERT INTO {s}.orders (id, customer_id, billing_customer_id, billing_email, total) VALUES (10, 1, 2, 'ada@example.test', 10.50);
INSERT INTO {s}.order_lines VALUES (10, 1, 'SKU-1', 2), (10, 2, 'SKU-2', 1);
INSERT INTO {s}.tags VALUES (1, 'priority');
INSERT INTO {s}.order_tags VALUES (1, 10, 1, 5);
"#
    ))
    .await
    .expect("create schema");
}

fn config(e: &Env, s: &str, root: &std::path::Path) -> Config {
    let key = root.join("state.key");
    std::fs::write(&key, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=").unwrap();
    std::fs::write(root.join("pg.pw"), &e.pg_password).unwrap();
    std::fs::write(root.join("neo.pw"), &e.neo_password).unwrap();
    let toml = format!(
        r#"
[pipeline]
id = "it-{s}"
tenant = "it"
profile = "development"

[engine]
allow_plaintext = true
retry_max_backoff_ms = 1000

[source]
type = "postgres"
host = "{host}"
port = {port}
database = "{db}"
user = "{user}"
password = 'file:{pgpw}'
publication = "pub_{s}"
slot = "slot_{s}"
tls = {{ mode = "disable" }}
snapshot = {{ enabled = true, chunk_rows = 1 }}

[graph]
type = "neo4j"
uri = "{neo}"
user = "{neo_user}"
password = 'file:{neopw}'
max_connections = 4

[state]
dir = '{state}'
active_key = 1
keys = [{{ id = 1, key = 'file:{key}' }}]

[policy]
[[policy.datasets]]
relation = "{s}.customers"
deny_fields = ["ssn"]
[[policy.datasets]]
relation = "{s}.employees"
[[policy.datasets]]
relation = "{s}.orders"
[[policy.datasets]]
relation = "{s}.order_lines"
[[policy.datasets]]
relation = "{s}.tags"
[[policy.datasets]]
relation = "{s}.order_tags"

[[mapping.relations]]
relation = "{s}.orders"
labels = ["Order"]
[[mapping.relations.edges]]
constraint = "orders_customer_id_fkey"
type = "PLACED_BY"

[[mapping.join_tables]]
relation = "{s}.order_tags"
from_constraint = "order_tags_order_id_fkey"
to_constraint = "order_tags_tag_id_fkey"
type = "TAGGED"
"#,
        host = e.pg_host,
        port = e.pg_port,
        db = e.pg_db,
        user = e.pg_user,
        pgpw = root.join("pg.pw").display(),
        neo = e.neo_uri,
        neo_user = e.neo_user,
        neopw = root.join("neo.pw").display(),
        state = root.join("state").display(),
        key = key.display(),
    );
    Config::from_toml_str(&toml).expect("config")
}

async fn neo(e: &Env) -> neo4rs::Graph {
    neo4rs::Graph::new(&e.neo_uri, &e.neo_user, &e.neo_password).await.expect("neo4j")
}

async fn scalar(g: &neo4rs::Graph, q: neo4rs::Query) -> i64 {
    let mut s = g.execute(q).await.unwrap();
    s.next().await.unwrap().map(|r| r.get::<i64>("c").unwrap()).unwrap_or(0)
}

async fn count_label(g: &neo4rs::Graph, pipeline: &str, label: &str) -> i64 {
    scalar(
        g,
        neo4rs::query(&format!("MATCH (n:`{label}` {{_d2g_pipeline: $p}}) WHERE NOT coalesce(n._d2g_deleted, false) RETURN count(n) AS c"))
            .param("p", pipeline),
    )
    .await
}

async fn count_rel(g: &neo4rs::Graph, pipeline: &str, t: &str) -> i64 {
    scalar(g, neo4rs::query(&format!("MATCH ()-[r:`{t}` {{_d2g_pipeline: $p}}]->() RETURN count(r) AS c")).param("p", pipeline)).await
}

async fn wait_until<F, Fut>(what: &str, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while !f().await {
        assert!(start.elapsed() < T, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn cleanup(c: &tokio_postgres::Client, s: &str) {
    let _ = c
        .batch_execute(&format!("SELECT pg_drop_replication_slot('slot_{s}') FROM pg_replication_slots WHERE slot_name = 'slot_{s}'"))
        .await;
    let _ = c.batch_execute(&format!("DROP PUBLICATION IF EXISTS pub_{s}; DROP SCHEMA IF EXISTS {s} CASCADE;")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_to_neo4j_end_to_end() {
    let e = require_env!("postgres_to_neo4j_end_to_end");
    let s = format!("it_{}", suffix());
    let c = pg(&e).await;
    setup_schema(&c, &s).await;
    c.batch_execute(&format!("CREATE PUBLICATION pub_{s} FOR TABLES IN SCHEMA {s}")).await.expect("publication");
    let root = tempfile::tempdir().unwrap();
    let cfg = config(&e, &s, root.path());
    let pipeline = cfg.pipeline.id.clone();

    // Preflight and explicit graph bootstrap (operator steps).
    let reports = db2graph_sync::admin::preflight(&cfg, StartOptions::default()).await.expect("preflight");
    for r in &reports {
        for chk in &r.checks {
            println!("preflight {} {:?} {}: {}", r.component, chk.status, chk.name, chk.detail);
        }
    }
    assert!(reports[0].passed(), "source preflight: {:?}", reports[0].failures());
    db2graph_sync::admin::bootstrap_graph(&cfg, StartOptions::default()).await.expect("bootstrap");

    // The one-line startup.
    let sync = db2graph_sync::start(cfg.clone()).await.expect("start");
    sync.ready(T).await.expect("ready");
    let st = sync.status();
    assert_eq!(st.phase, Phase::Streaming);
    let snap = st.snapshot.clone().expect("snapshot progress");
    assert_eq!(snap.consistency, Some(db2graph_sync::capability::SnapshotConsistency::TransactionConsistent));
    let g = neo(&e).await;
    assert_eq!(count_label(&g, &pipeline, "Customers").await, 2);
    assert_eq!(count_label(&g, &pipeline, "Order").await, 1);
    assert_eq!(count_label(&g, &pipeline, "OrderLines").await, 2, "composite primary keys");
    assert_eq!(count_rel(&g, &pipeline, "PLACED_BY").await, 1, "approved semantic name");
    assert_eq!(count_rel(&g, &pipeline, "FK_ORDERS_BILLING_CUSTOMER_ID_FKEY").await, 1, "second FK between same tables");
    assert_eq!(count_rel(&g, &pipeline, "FK_ORDERS_BILLING_EMAIL_FKEY").await, 1, "unique-key reference");
    assert_eq!(count_rel(&g, &pipeline, "FK_EMPLOYEES_MANAGER_ID_FKEY").await, 2, "self references incl. a cycle of one");
    assert_eq!(count_rel(&g, &pipeline, "TAGGED").await, 1, "join table as relationship");
    assert_eq!(
        scalar(
            &g,
            neo4rs::query("MATCH (n:Customers {_d2g_pipeline: $p}) WHERE n.ssn IS NOT NULL RETURN count(n) AS c")
                .param("p", pipeline.as_str())
        )
        .await,
        0,
        "denied field not projected"
    );

    // Streaming: FK change, PK change, unchanged TOAST, delete, rollback.
    let big = "x".repeat(10_000);
    c.batch_execute(&format!(
        "BEGIN; UPDATE {s}.customers SET notes = '{big}' WHERE id = 2; COMMIT;
         BEGIN; INSERT INTO {s}.customers VALUES (3, 'Hopper', 'hopper@example.test', NULL, NULL);
                UPDATE {s}.orders SET customer_id = 3 WHERE id = 10; COMMIT;
         BEGIN; UPDATE {s}.customers SET name = 'Grace H.' WHERE id = 2; COMMIT;
         BEGIN; INSERT INTO {s}.customers VALUES (99, 'Rolled', NULL, NULL, NULL); ROLLBACK;
         BEGIN; UPDATE {s}.order_lines SET line_no = 3 WHERE order_id = 10 AND line_no = 2; COMMIT;
         BEGIN; INSERT INTO {s}.orders (id, customer_id, total) VALUES (11, 7, 1.00);
                INSERT INTO {s}.customers VALUES (7, 'Late', NULL, NULL, NULL); COMMIT;
         BEGIN; DELETE FROM {s}.order_tags WHERE id = 1; COMMIT;"
    ))
    .await
    .expect("changes");
    let p2 = pipeline.clone();
    let g2 = &g;
    wait_until("stream applied", || async { count_rel(g2, &p2, "TAGGED").await == 0 && count_label(g2, &p2, "Order").await == 2 }).await;
    let placed_to: String = {
        let mut s2 = g
            .execute(
                neo4rs::query("MATCH (o:Order {_d2g_pipeline: $p, id: 10})-[:PLACED_BY]->(c) RETURN c.name AS n")
                    .param("p", pipeline.as_str()),
            )
            .await
            .unwrap();
        s2.next().await.unwrap().unwrap().get("n").unwrap()
    };
    assert_eq!(placed_to, "Hopper", "obsolete relationship replaced on FK change");
    assert_eq!(
        scalar(
            &g,
            neo4rs::query(
                "MATCH (n:Customers {_d2g_pipeline: $p, id: 2}) WHERE size(n.notes) = 10000 AND n.name = 'Grace H.' RETURN count(n) AS c"
            )
            .param("p", pipeline.as_str())
        )
        .await,
        1,
        "unchanged TOAST value preserved across a later update"
    );
    assert_eq!(
        scalar(&g, neo4rs::query("MATCH (n:Customers {_d2g_pipeline: $p, id: 99}) RETURN count(n) AS c").param("p", pipeline.as_str()))
            .await,
        0,
        "rolled-back transaction never visible"
    );
    assert_eq!(
        scalar(
            &g,
            neo4rs::query("MATCH (n:OrderLines {_d2g_pipeline: $p}) RETURN collect(n.line_no) AS l, count(n) AS c")
                .param("p", pipeline.as_str())
        )
        .await,
        2,
        "primary-key change replaced the identity"
    );
    assert_eq!(count_rel(&g, &pipeline, "PLACED_BY").await, 2, "edge to customer created in the same transaction");

    // Restart: changes made while stopped are caught up from the slot.
    let report = sync.shutdown(T).await.expect("shutdown");
    assert!(report.drained);
    c.batch_execute(&format!("DELETE FROM {s}.order_lines WHERE order_id = 10; DELETE FROM {s}.order_tags; DELETE FROM {s}.orders WHERE id = 11; TRUNCATE {s}.tags CASCADE;"))
        .await
        .expect("offline changes");
    let sync = db2graph_sync::start(cfg.clone()).await.expect("restart");
    sync.ready(T).await.expect("ready after restart");
    let start = std::time::Instant::now();
    loop {
        let (lines, tags) = (count_label(g2, &p2, "OrderLines").await, count_label(g2, &p2, "Tags").await);
        if lines == 0 && tags == 0 {
            break;
        }
        if start.elapsed() > T {
            panic!(
                "timed out waiting for caught up: OrderLines={lines} Tags={tags}\nstatus: {}",
                serde_json::to_string_pretty(&sync.status()).unwrap()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        scalar(&g, neo4rs::query("MATCH (t:_D2G_Tombstone {_d2g_pipeline: $p}) RETURN count(t) AS c").param("p", pipeline.as_str())).await
            >= 2,
        "deletes leave tombstones"
    );
    let st = sync.status();
    assert!(st.failure.is_none(), "{:?}", st.failure);
    sync.shutdown(T).await.expect("final shutdown");
    cleanup(&c, &s).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn neo4j_sink_conformance() {
    let e = require_env!("neo4j_sink_conformance");
    let sink = db2graph_sync::sink::neo4j::Neo4jSink::connect(
        serde_json::from_value(serde_json::json!({
            "uri": e.neo_uri, "user": e.neo_user, "password": "literal:unused", "max_connections": 4
        }))
        .unwrap(),
        db2graph_sync::secret::Secret::new(e.neo_password.clone().into_bytes()),
    )
    .await
    .expect("connect");
    use db2graph_sync::sink::GraphSink;
    let plan = db2graph_sync::mapping::MappingPlan {
        schema_version: String::new(),
        relations: Default::default(),
        join_tables: Default::default(),
        excluded: Default::default(),
    };
    sink.bootstrap(&plan).await.expect("bootstrap");
    // The unique-key check in the suite uses this lookup property.
    let g = neo(&e).await;
    g.run(neo4rs::query(
        "CREATE INDEX d2g_uk_customers_email_key IF NOT EXISTS FOR (n:_D2G_Entity) ON (n._d2g_pipeline, n._d2g_uk_customers_email_key)",
    ))
    .await
    .unwrap();
    db2graph_sync::sink::conformance::run_all(&sink, &format!("conf-{}", suffix())).await.expect("Neo4j sink conforms");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_slot_after_progress_is_resume_history_lost() {
    let e = require_env!("missing_slot_after_progress_is_resume_history_lost");
    let s = format!("it_{}", suffix());
    let c = pg(&e).await;
    setup_schema(&c, &s).await;
    c.batch_execute(&format!("CREATE PUBLICATION pub_{s} FOR TABLES IN SCHEMA {s}")).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let cfg = config(&e, &s, root.path());
    db2graph_sync::admin::bootstrap_graph(&cfg, StartOptions::default()).await.unwrap();
    let sync = db2graph_sync::start(cfg.clone()).await.unwrap();
    sync.ready(T).await.unwrap();
    sync.shutdown(T).await.unwrap();
    // Operator (or failover without slot sync) loses the slot.
    c.batch_execute(&format!("SELECT pg_drop_replication_slot('slot_{s}')")).await.unwrap();
    let sync = db2graph_sync::start(cfg).await.unwrap();
    let err = sync.ready(T).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResumeHistoryLost, "{err}");
    let _ = sync.wait().await;
    cleanup(&c, &s).await;
}
