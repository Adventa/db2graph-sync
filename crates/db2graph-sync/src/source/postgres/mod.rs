//! PostgreSQL source connector (logical replication, `pgoutput` protocol v1).
//!
//! * Change capture: a logical replication slot owned by the pipeline and a
//!   publication created by the operator. No business-table polling.
//! * Initial snapshot: on first start the slot is created with an exported
//!   snapshot; tables are read inside that snapshot (transaction-consistent at
//!   the slot's consistent point). An interrupted snapshot resumes with fresh
//!   reads and is reported as `convergent`.
//! * The connector never creates publications, roles, or changes replica
//!   identity or server settings; `setup_statements` prints them for an
//!   operator. It creates its own replication slot if missing (the slot is the
//!   consumer's resource) and never drops slots.
//!
//! See `docs/connectors/postgresql.md` for prerequisites and limitations.

mod pgoutput;
mod stream;
mod wire;

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tokio_postgres::SimpleQueryMessage;

use crate::capability::{CheckStatus, OrderingScopeKind, PreflightReport, SnapshotConsistency, SourceCapabilities};
use crate::config::{PostgresSourceConfig, TlsMode};
use crate::error::{Error, ErrorKind, Result};
use crate::policy::PolicyConfig;
use crate::schema::{Column, ForeignKey, ImageMode, Relation, SourceSchema, UniqueConstraint};
use crate::secret::Secret;
use crate::source::{ChangeStream, SourceConnector, SourceIdentity, StartRequest};
use crate::types::RelationName;

/// Minimum server version (wal_status in pg_replication_slots).
const MIN_VERSION: i32 = 130_000;
/// Type OIDs converted to typed values; others are kept as text (`Unknown`).
const KNOWN_TYPES: &[u32] =
    &[16, 17, 19, 20, 21, 23, 25, 26, 114, 142, 700, 701, 1042, 1043, 1082, 1083, 1114, 1184, 1266, 1700, 2950, 3802];

pub(crate) struct Inner {
    pub cfg: PostgresSourceConfig,
    pub password: Option<Secret>,
    pub known_schema: RwLock<Option<SourceSchema>>,
    pub source_id: RwLock<Option<String>>,
}

/// See module docs.
#[derive(Clone)]
pub struct PostgresSource {
    inner: Arc<Inner>,
}

impl PostgresSource {
    pub fn new(cfg: PostgresSourceConfig, password: Option<Secret>, allow_plaintext: bool) -> Result<Self> {
        if cfg.tls.mode == TlsMode::Disable && !allow_plaintext {
            return Err(Error::new(ErrorKind::Security, "plaintext PostgreSQL connections require the development exception"));
        }
        Ok(Self { inner: Arc::new(Inner { cfg, password, known_schema: RwLock::new(None), source_id: RwLock::new(None) }) })
    }
}

impl Inner {
    pub(crate) fn params(&self, replication: bool) -> wire::ConnParams<'_> {
        wire::ConnParams {
            host: &self.cfg.host,
            port: self.cfg.port,
            user: &self.cfg.user,
            database: &self.cfg.database,
            password: self.password.as_ref(),
            tls: self.cfg.tls.mode,
            ca_file: self.cfg.tls.ca_file.as_deref(),
            replication,
            application_name: "db2graph-sync",
        }
    }

    pub(crate) async fn replication_conn(&self) -> Result<wire::Conn> {
        wire::Conn::connect(&self.params(true)).await
    }

    /// Regular (non-replication) connection via tokio-postgres.
    pub(crate) async fn client(&self) -> Result<tokio_postgres::Client> {
        let mut c = tokio_postgres::Config::new();
        c.host(&self.cfg.host)
            .port(self.cfg.port)
            .user(&self.cfg.user)
            .dbname(&self.cfg.database)
            .application_name("db2graph-sync")
            .connect_timeout(std::time::Duration::from_secs(15))
            .options(wire::SESSION_GUCS.iter().map(|(k, v)| format!("-c {k}={v}")).collect::<Vec<_>>().join(" "));
        if let Some(p) = &self.password {
            c.password(p.expose());
        }
        let map = |e: tokio_postgres::Error| {
            let msg = crate::secret::redact_url(&e.to_string());
            match e.code().map(|c| c.code()) {
                Some("28000") | Some("28P01") | Some("42501") => Error::new(ErrorKind::Security, msg),
                _ => Error::source(format!("postgres: {msg}")),
            }
        };
        match self.cfg.tls.mode {
            TlsMode::Disable => {
                c.ssl_mode(tokio_postgres::config::SslMode::Disable);
                let (client, conn) = c.connect(tokio_postgres::NoTls).await.map_err(map)?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("postgres connection closed: {e}");
                    }
                });
                Ok(client)
            }
            TlsMode::VerifyFull => {
                c.ssl_mode(tokio_postgres::config::SslMode::Require);
                let tls =
                    tokio_postgres_rustls::MakeRustlsConnect::new((*crate::tls::client_config(self.cfg.tls.ca_file.as_deref())?).clone());
                let (client, conn) = c.connect(tls).await.map_err(map)?;
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("postgres connection closed: {e}");
                    }
                });
                Ok(client)
            }
        }
    }

    pub(crate) async fn discover(&self, client: &tokio_postgres::Client) -> Result<SourceSchema> {
        let q = |e: tokio_postgres::Error| Error::source(format!("schema discovery: {e}"));
        let version: i32 = client.query_one("SELECT current_setting('server_version_num')::int", &[]).await.map_err(q)?.get(0);
        let source_id = self.source_id.read().expect("poisoned").clone().unwrap_or_else(|| "pg:unidentified".into());
        let pub_sql = if version >= 150_000 {
            "SELECT c.oid, n.nspname::text, c.relname::text, c.relreplident::text, pt.attnames::text[] \
             FROM pg_publication_tables pt JOIN pg_namespace n ON n.nspname = pt.schemaname \
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename WHERE pt.pubname = $1"
        } else {
            "SELECT c.oid, n.nspname::text, c.relname::text, c.relreplident::text, NULL::text[] \
             FROM pg_publication_tables pt JOIN pg_namespace n ON n.nspname = pt.schemaname \
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename WHERE pt.pubname = $1"
        };
        let rows = client.query(pub_sql, &[&self.cfg.publication]).await.map_err(q)?;
        struct T {
            name: String,
            replident: String,
            cols: Option<Vec<String>>,
        }
        let mut tables: BTreeMap<u32, T> = BTreeMap::new();
        for r in rows {
            let oid: u32 = r.get(0);
            let ns: String = r.get(1);
            let rel: String = r.get(2);
            tables.insert(oid, T { name: format!("{ns}.{rel}"), replident: r.get(3), cols: r.get(4) });
        }
        let oids: Vec<u32> = tables.keys().copied().collect();

        let cols = client
            .query(
                "SELECT a.attrelid, a.attname::text, format_type(a.atttypid, a.atttypmod), a.atttypid, a.attnotnull \
                 FROM pg_attribute a WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped \
                 ORDER BY a.attrelid, a.attnum",
                &[&oids],
            )
            .await
            .map_err(q)?;
        let mut columns: BTreeMap<u32, Vec<Column>> = BTreeMap::new();
        for r in cols {
            let oid: u32 = r.get(0);
            let name: String = r.get(1);
            if let Some(list) = tables.get(&oid).and_then(|t| t.cols.as_ref())
                && !list.contains(&name)
            {
                continue; // excluded by publication column list
            }
            columns.entry(oid).or_default().push(Column {
                name,
                type_name: r.get(2),
                type_id: Some(r.get::<_, u32>(3)),
                nullable: !r.get::<_, bool>(4),
            });
        }

        let idx = client
            .query(
                "SELECT i.indrelid, ic.relname::text, i.indisprimary, \
                 ARRAY(SELECT a.attname::text FROM unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, ord) \
                       JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum ORDER BY k.ord) \
                 FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid \
                 WHERE i.indrelid = ANY($1) AND i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL",
                &[&oids],
            )
            .await
            .map_err(q)?;
        let mut pks: BTreeMap<u32, (String, Vec<String>)> = BTreeMap::new();
        let mut uniques: BTreeMap<u32, Vec<UniqueConstraint>> = BTreeMap::new();
        for r in idx {
            let oid: u32 = r.get(0);
            let name: String = r.get(1);
            let cols: Vec<String> = r.get(3);
            if r.get::<_, bool>(2) {
                pks.insert(oid, (name, cols));
            } else {
                uniques.entry(oid).or_default().push(UniqueConstraint { name, columns: cols });
            }
        }

        let fks = client
            .query(
                "SELECT con.conrelid, con.conname::text, fn.nspname::text || '.' || fc.relname::text, \
                 COALESCE(ic.relname::text, ''), COALESCE(fi.indisprimary, false), \
                 ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY k(n, o) \
                       JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.n ORDER BY k.o), \
                 ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY k(n, o) \
                       JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.n ORDER BY k.o) \
                 FROM pg_constraint con \
                 JOIN pg_class fc ON fc.oid = con.confrelid JOIN pg_namespace fn ON fn.oid = fc.relnamespace \
                 LEFT JOIN pg_class ic ON ic.oid = con.conindid \
                 LEFT JOIN pg_index fi ON fi.indexrelid = con.conindid \
                 WHERE con.contype = 'f' AND con.conrelid = ANY($1) ORDER BY con.conrelid, con.conname",
                &[&oids],
            )
            .await
            .map_err(q)?;
        let mut foreign: BTreeMap<u32, Vec<ForeignKey>> = BTreeMap::new();
        for r in fks {
            let oid: u32 = r.get(0);
            let referenced_is_pk: bool = r.get(4);
            foreign.entry(oid).or_default().push(ForeignKey {
                name: r.get(1),
                target: RelationName(r.get(2)),
                target_constraint: if referenced_is_pk { String::new() } else { r.get(3) },
                columns: r.get(5),
                target_columns: r.get(6),
            });
        }

        let mut relations = BTreeMap::new();
        for (oid, t) in tables {
            let primary_key = pks.get(&oid).map(|(_, c)| c.clone()).unwrap_or_default();
            let image_mode = match t.replident.as_str() {
                "f" => ImageMode::Full,
                "n" => ImageMode::Nothing,
                "d" if primary_key.is_empty() => ImageMode::Nothing,
                _ => ImageMode::KeyOnly,
            };
            let name = RelationName(t.name);
            relations.insert(
                name.clone(),
                Relation {
                    name,
                    columns: columns.remove(&oid).unwrap_or_default(),
                    primary_key,
                    unique_constraints: uniques.remove(&oid).unwrap_or_default(),
                    foreign_keys: foreign.remove(&oid).unwrap_or_default(),
                    image_mode,
                },
            );
        }
        Ok(SourceSchema { source_id, relations })
    }

    async fn identify(&self) -> Result<SourceIdentity> {
        let mut conn = self.replication_conn().await?;
        let rows = conn.simple_query("IDENTIFY_SYSTEM").await?;
        let version = conn.simple_query("SHOW server_version").await.ok().and_then(|r| r.first()?.first()?.clone());
        conn.terminate().await;
        let row = rows.first().ok_or_else(|| Error::source("IDENTIFY_SYSTEM returned no rows"))?;
        let sysid = row.first().cloned().flatten().ok_or_else(|| Error::source("IDENTIFY_SYSTEM: no systemid"))?;
        let tli = row.get(1).cloned().flatten().unwrap_or_default();
        let id = SourceIdentity {
            source_id: format!("pg:{sysid}"),
            history: format!("timeline:{tli}"),
            server_version: version.unwrap_or_default(),
        };
        *self.source_id.write().expect("poisoned") = Some(id.source_id.clone());
        Ok(id)
    }
}

#[async_trait]
impl SourceConnector for PostgresSource {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            connector: "postgres-pgoutput".into(),
            transaction_boundaries: true,
            ordering: OrderingScopeKind::TotalCommitOrder,
            snapshot: SnapshotConsistency::TransactionConsistent,
            resumed_snapshot: SnapshotConsistency::Convergent,
            before_images: "key-only (REPLICA IDENTITY DEFAULT/INDEX) or full (REPLICA IDENTITY FULL)",
            truncate: true,
            schema_change_signal: true,
            qualified_versions: Vec::new(),
            notes: vec![
                "pgoutput protocol v1: in-progress transactions are not streamed; large transactions are buffered up to max_transaction_changes".into(),
                "prepared transactions are delivered at COMMIT PREPARED (two_phase disabled); not separately qualified".into(),
                "logical decoding from standbys, RDS and Aurora are not qualified in this release".into(),
            ],
        }
    }

    async fn preflight(&self) -> Result<PreflightReport> {
        let mut r = PreflightReport::new(format!("source:postgres:{}:{}", self.inner.cfg.host, self.inner.cfg.port));
        let client = match self.inner.client().await {
            Ok(c) => c,
            Err(e) if e.kind() == ErrorKind::Security => {
                r.push("connect", CheckStatus::Fail, e.message());
                return Ok(r);
            }
            Err(e) => return Err(e),
        };
        r.push("connect", CheckStatus::Pass, "connected");
        let one = |sql: &'static str| {
            let c = &client;
            async move { c.query_one(sql, &[]).await.map_err(|e| Error::source(format!("{sql}: {e}"))) }
        };
        let version: i32 = one("SELECT current_setting('server_version_num')::int").await?.get(0);
        r.push(
            "engine_version",
            if version >= MIN_VERSION { CheckStatus::Pass } else { CheckStatus::Fail },
            format!("server_version_num={version} (minimum {MIN_VERSION})"),
        );
        let ssl: Option<bool> =
            client.query_opt("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[]).await.ok().flatten().map(|row| row.get(0));
        match (self.inner.cfg.tls.mode, ssl) {
            (TlsMode::VerifyFull, Some(true)) => r.push("tls", CheckStatus::Pass, "TLS with verified certificate"),
            (TlsMode::Disable, _) => r.push("tls", CheckStatus::Warn, "plaintext (development exception)"),
            (_, other) => r.push("tls", CheckStatus::Fail, format!("TLS not confirmed by server: {other:?}")),
        }
        let wal_level: String = one("SELECT current_setting('wal_level')").await?.get(0);
        r.push(
            "cdc_enabled",
            if wal_level == "logical" { CheckStatus::Pass } else { CheckStatus::Fail },
            format!("wal_level={wal_level} (requires logical)"),
        );
        let scs: String = one("SELECT current_setting('standard_conforming_strings')").await?.get(0);
        r.push(
            "standard_conforming_strings",
            if scs == "on" { CheckStatus::Pass } else { CheckStatus::Fail },
            format!("standard_conforming_strings={scs}"),
        );
        let in_recovery: bool = one("SELECT pg_is_in_recovery()").await?.get(0);
        r.push(
            "primary",
            if in_recovery { CheckStatus::Fail } else { CheckStatus::Pass },
            if in_recovery { "server is a standby; standby decoding is not qualified" } else { "primary" },
        );
        let priv_row = client
            .query_one(
                "SELECT r.rolreplication OR r.rolsuper OR \
                 (EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') AND pg_has_role(current_user, 'rds_replication', 'member')) \
                 FROM pg_roles r WHERE r.rolname = current_user",
                &[],
            )
            .await
            .map_err(|e| Error::source(e.to_string()))?;
        let can_replicate: bool = priv_row.get(0);
        r.push(
            "privileges.replication",
            if can_replicate { CheckStatus::Pass } else { CheckStatus::Fail },
            if can_replicate { "role may use replication" } else { "role lacks REPLICATION (or rds_replication)" },
        );
        let pub_exists: bool = client
            .query_one("SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", &[&self.inner.cfg.publication])
            .await
            .map_err(|e| Error::source(e.to_string()))?
            .get(0);
        r.push(
            "publication",
            if pub_exists { CheckStatus::Pass } else { CheckStatus::Fail },
            format!(
                "publication `{}` {}",
                self.inner.cfg.publication,
                if pub_exists { "exists" } else { "is missing (operator must create it)" }
            ),
        );
        if pub_exists {
            let schema = self.inner.discover(&client).await?;
            r.push(
                "schema_visibility",
                if schema.relations.is_empty() { CheckStatus::Fail } else { CheckStatus::Pass },
                format!("{} published relation(s)", schema.relations.len()),
            );
            let noselect: Vec<String> = client
                .query(
                    "SELECT pt.schemaname || '.' || pt.tablename FROM pg_publication_tables pt \
                     WHERE pt.pubname = $1 AND NOT has_table_privilege(quote_ident(pt.schemaname) || '.' || quote_ident(pt.tablename), 'SELECT')",
                    &[&self.inner.cfg.publication],
                )
                .await
                .map_err(|e| Error::source(e.to_string()))?
                .iter()
                .map(|row| row.get(0))
                .collect();
            r.push(
                "privileges.select",
                if noselect.is_empty() { CheckStatus::Pass } else { CheckStatus::Fail },
                if noselect.is_empty() {
                    "SELECT granted on published tables".to_owned()
                } else {
                    format!("missing SELECT on {noselect:?}")
                },
            );
            for rel in schema.relations.values() {
                if rel.primary_key.is_empty() {
                    r.push(&format!("key:{}", rel.name), CheckStatus::Warn, "no primary key; excluded unless mapped with key_constraint");
                }
                if rel.image_mode == ImageMode::Nothing {
                    r.push(&format!("image:{}", rel.name), CheckStatus::Fail, "REPLICA IDENTITY NOTHING: deletes cannot be captured");
                }
                let unknown: Vec<String> = rel
                    .columns
                    .iter()
                    .filter(|c| !KNOWN_TYPES.contains(&c.type_id.unwrap_or(0)))
                    .map(|c| format!("{} {}", c.name, c.type_name))
                    .collect();
                if !unknown.is_empty() {
                    r.push(
                        &format!("types:{}", rel.name),
                        CheckStatus::Warn,
                        format!("kept as text with type tag (not interpreted): {unknown:?}"),
                    );
                }
            }
            *self.inner.known_schema.write().expect("poisoned") = Some(schema);
        }
        let slot = client
            .query_opt(
                "SELECT plugin::text, database::text, active, wal_status::text, confirmed_flush_lsn::text, \
                 pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.inner.cfg.slot],
            )
            .await
            .map_err(|e| Error::source(e.to_string()))?;
        match slot {
            None => {
                let free: i64 = client
                    .query_one("SELECT current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots)", &[])
                    .await
                    .map_err(|e| Error::source(e.to_string()))?
                    .get(0);
                r.push(
                    "slot",
                    if free > 0 { CheckStatus::Pass } else { CheckStatus::Fail },
                    format!("slot `{}` will be created on first start ({free} free slot(s))", self.inner.cfg.slot),
                );
            }
            Some(row) => {
                let plugin: Option<String> = row.get(0);
                let db: Option<String> = row.get(1);
                let active: bool = row.get(2);
                let wal_status: Option<String> = row.get(3);
                let retained: Option<i64> = row.get(5);
                let mut problems = Vec::new();
                if plugin.as_deref() != Some("pgoutput") {
                    problems.push(format!("plugin {plugin:?}"));
                }
                if db.as_deref() != Some(self.inner.cfg.database.as_str()) {
                    problems.push(format!("database {db:?}"));
                }
                if active {
                    problems.push("slot is active (another consumer is attached)".into());
                }
                if wal_status.as_deref() == Some("lost") {
                    problems.push("wal_status=lost: resume history is gone".into());
                }
                r.push(
                    "slot",
                    if problems.is_empty() { CheckStatus::Pass } else { CheckStatus::Fail },
                    if problems.is_empty() { format!("slot `{}` usable", self.inner.cfg.slot) } else { problems.join("; ") },
                );
                let retained = retained.unwrap_or(0);
                let limit = self.inner.cfg.max_slot_retained_bytes;
                r.push(
                    "retention",
                    if limit.is_some_and(|l| retained as u64 > l) { CheckStatus::Warn } else { CheckStatus::Pass },
                    format!("slot retains {retained} bytes of WAL (wal_status={wal_status:?})"),
                );
            }
        }
        Ok(r)
    }

    async fn identity(&self) -> Result<SourceIdentity> {
        self.inner.identify().await
    }

    async fn discover_schema(&self) -> Result<SourceSchema> {
        if self.inner.source_id.read().expect("poisoned").is_none() {
            self.inner.identify().await?;
        }
        let client = self.inner.client().await?;
        let schema = self.inner.discover(&client).await?;
        *self.inner.known_schema.write().expect("poisoned") = Some(schema.clone());
        Ok(schema)
    }

    async fn start(&self, req: StartRequest) -> Result<Box<dyn ChangeStream>> {
        Ok(Box::new(stream::PgStream::open(self.inner.clone(), req).await?))
    }

    fn setup_statements(&self) -> Vec<String> {
        setup_statements(&self.inner.cfg, &PolicyConfig::default())
    }

    async fn row_count(&self, relation: &RelationName) -> Result<u64> {
        let (s, t) = relation.0.split_once('.').ok_or_else(|| Error::config("relation must be schema-qualified"))?;
        let client = self.inner.client().await?;
        let rows = simple_rows(&client, &format!("SELECT count(*) FROM {}.{}", wire::quote_ident(s), wire::quote_ident(t))).await?;
        rows.first()
            .and_then(|r| r.first().cloned().flatten())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Error::source("count query returned no value"))
    }

    async fn sample_rows(&self, relation: &RelationName, limit: u32) -> Result<Vec<crate::types::Image>> {
        let client = self.inner.client().await?;
        let schema = self.inner.discover(&client).await?;
        let rel = schema.relations.get(relation).ok_or_else(|| Error::config(format!("{relation} is not published")))?;
        if rel.primary_key.is_empty() {
            return Ok(Vec::new());
        }
        let sql = stream::chunk_query_pub(rel, limit)?;
        let rows = simple_rows(&client, &sql).await?;
        Ok(rows
            .iter()
            .map(|row| {
                rel.columns
                    .iter()
                    .zip(row)
                    .map(|(c, v)| {
                        let val = match v {
                            None => crate::types::Value::Null,
                            Some(t) => pgoutput::text_to_value(c.type_id.unwrap_or(0), t, Some(&c.type_name)),
                        };
                        (c.name.clone(), crate::types::FieldState::Present(val))
                    })
                    .collect()
            })
            .collect())
    }
}

/// Statements an operator reviews and runs to enable capture. Never executed.
pub fn setup_statements(cfg: &PostgresSourceConfig, policy: &PolicyConfig) -> Vec<String> {
    let q = wire::quote_ident;
    let tables: Vec<String> = policy.datasets.iter().map(|d| d.relation.split('.').map(q).collect::<Vec<_>>().join(".")).collect();
    let table_list = if tables.is_empty() { "<schema>.<table>, ...".to_owned() } else { tables.join(", ") };
    vec![
        "-- Review and run as an administrator. db2graph-sync never executes these.".into(),
        "-- 1. Enable logical decoding (requires a restart). On RDS/Aurora set rds.logical_replication = 1 in the parameter group.".into(),
        "ALTER SYSTEM SET wal_level = 'logical';".into(),
        format!("-- 2. Replication role with least privilege (RDS/Aurora: GRANT rds_replication TO {};)", q(&cfg.user)),
        format!("ALTER ROLE {} WITH REPLICATION;", q(&cfg.user)),
        "-- 3. Publication limited to approved tables.".to_string(),
        format!("CREATE PUBLICATION {} FOR TABLE {table_list};", q(&cfg.publication)),
        format!("GRANT SELECT ON {table_list} TO {};", q(&cfg.user)),
        "-- 4. Optional: full before-images where needed (increases WAL volume).".into(),
        "-- ALTER TABLE <schema>.<table> REPLICA IDENTITY FULL;".into(),
        format!("-- The replication slot `{}` is created by db2graph-sync on first start.", cfg.slot),
    ]
}

/// Simple-query helper returning text rows from a tokio-postgres client.
pub(crate) async fn simple_rows(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let msgs = client.simple_query(sql).await.map_err(|e| Error::source(format!("query failed: {e}")))?;
    Ok(msgs
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect()),
            _ => None,
        })
        .collect())
}
