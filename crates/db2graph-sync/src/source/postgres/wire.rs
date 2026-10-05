//! Minimal PostgreSQL frontend for *replication* connections.
//!
//! `tokio-postgres` (crates.io) does not implement the CopyBoth sub-protocol
//! used by `START_REPLICATION`, so this module speaks it directly. Frontend
//! messages and SCRAM-SHA-256 come from `postgres-protocol`; backend messages
//! are framed here (tag + length) because the replication responses
//! (`CopyBothResponse`) are not parsed by that crate.
//!
//! Reading is cancel-safe: bytes are accumulated in an owned buffer and a
//! message is consumed only once complete.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::authentication::sasl::{self, ChannelBinding, ScramSha256};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::config::TlsMode;
use crate::error::{Error, ErrorKind, Result};
use crate::secret::Secret;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Session settings forced on every source connection so that the text form
/// of values is identical between snapshot reads and pgoutput.
pub(crate) const SESSION_GUCS: &[(&str, &str)] =
    &[("DateStyle", "ISO,MDY"), ("TimeZone", "UTC"), ("extra_float_digits", "3"), ("IntervalStyle", "iso_8601"), ("bytea_output", "hex")];

pub(crate) struct ConnParams<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub database: &'a str,
    pub password: Option<&'a Secret>,
    pub tls: TlsMode,
    pub ca_file: Option<&'a std::path::Path>,
    pub replication: bool,
    pub application_name: &'a str,
}

pub(crate) struct Conn {
    io: Box<dyn Io>,
    rbuf: BytesMut,
    pub tls: bool,
}

pub(crate) fn pg_err(msg: impl Into<String>) -> Error {
    Error::source(msg)
}

/// Parsed ErrorResponse/NoticeResponse fields.
fn parse_fields(mut body: Bytes) -> (String, String, String) {
    let (mut sev, mut code, mut msg) = (String::new(), String::new(), String::new());
    while body.has_remaining() {
        let t = body.get_u8();
        if t == 0 {
            break;
        }
        let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
        let v = String::from_utf8_lossy(&body[..end]).into_owned();
        body.advance((end + 1).min(body.len()));
        match t {
            b'S' => sev = v,
            b'C' => code = v,
            b'M' => msg = v,
            _ => {}
        }
    }
    (sev, code, msg)
}

/// Convert a server ErrorResponse into a typed error.
fn server_error(body: Bytes) -> Error {
    let (sev, code, msg) = parse_fields(body);
    let text = format!("postgres {sev} {code}: {msg}");
    match code.as_str() {
        // insufficient_privilege, invalid_authorization_specification, invalid_password
        "42501" | "28000" | "28P01" => Error::new(ErrorKind::Security, text),
        // undefined_object (publication/slot missing): operator action needed.
        "42704" => Error::new(ErrorKind::Preflight, text),
        // object_not_in_prerequisite_state: e.g. slot invalidated (wal_status lost)
        "55000" => Error::new(ErrorKind::ResumeHistoryLost, text),
        // obsolete wal / requested WAL segment removed
        "58P01" => Error::new(ErrorKind::ResumeHistoryLost, text),
        // object_in_use (slot active elsewhere)
        // object_in_use (slot active): retryable; the server may still hold our
        // previous connection until wal_sender_timeout.
        "55006" => Error::retryable(ErrorKind::OwnershipConflict, text),
        // syntax/feature errors are configuration problems
        "42601" | "0A000" | "42P01" => Error::new(ErrorKind::Source, text),
        _ => Error::source(text),
    }
}

impl Conn {
    pub async fn connect(p: &ConnParams<'_>) -> Result<Conn> {
        let tcp = tokio::time::timeout(Duration::from_secs(15), TcpStream::connect((p.host, p.port)))
            .await
            .map_err(|_| pg_err(format!("connect to {}:{} timed out", p.host, p.port)))?
            .map_err(|e| pg_err(format!("connect to {}:{}: {e}", p.host, p.port)))?;
        let _ = tcp.set_nodelay(true);
        let mut conn = match p.tls {
            TlsMode::Disable => Conn { io: Box::new(tcp), rbuf: BytesMut::with_capacity(64 * 1024), tls: false },
            TlsMode::VerifyFull => {
                let mut tcp = tcp;
                let mut buf = BytesMut::new();
                frontend::ssl_request(&mut buf);
                tcp.write_all(&buf).await.map_err(|e| pg_err(e.to_string()))?;
                let mut b = [0u8; 1];
                tcp.read_exact(&mut b).await.map_err(|e| pg_err(e.to_string()))?;
                if b[0] != b'S' {
                    // No plaintext fallback.
                    return Err(Error::new(ErrorKind::Security, "server refused TLS; plaintext fallback is not permitted"));
                }
                let cfg = crate::tls::client_config(p.ca_file)?;
                let name = rustls::pki_types::ServerName::try_from(p.host.to_owned())
                    .map_err(|_| Error::new(ErrorKind::Security, format!("invalid TLS server name `{}`", p.host)))?;
                let tls = tokio_rustls::TlsConnector::from(cfg)
                    .connect(name, tcp)
                    .await
                    .map_err(|e| Error::new(ErrorKind::Security, format!("TLS handshake/verification failed: {e}")))?;
                Conn { io: Box::new(tls), rbuf: BytesMut::with_capacity(64 * 1024), tls: true }
            }
        };
        conn.startup(p).await?;
        Ok(conn)
    }

    async fn startup(&mut self, p: &ConnParams<'_>) -> Result<()> {
        let options: String = SESSION_GUCS.iter().map(|(k, v)| format!("-c {k}={v}")).collect::<Vec<_>>().join(" ");
        let mut params: Vec<(&str, &str)> =
            vec![("user", p.user), ("database", p.database), ("application_name", p.application_name), ("options", &options)];
        if p.replication {
            params.push(("replication", "database"));
        }
        let mut buf = BytesMut::new();
        frontend::startup_message(params.iter().copied(), &mut buf).map_err(|e| pg_err(e.to_string()))?;
        self.send(&buf).await?;
        let mut scram: Option<ScramSha256> = None;
        loop {
            let (tag, mut body) = self.read_msg().await?;
            match tag {
                b'R' => {
                    let kind = body.get_i32();
                    match kind {
                        0 => {}
                        3 => {
                            if !self.tls {
                                return Err(Error::new(
                                    ErrorKind::Security,
                                    "server requested cleartext password over an unencrypted connection",
                                ));
                            }
                            let pw = p.password.ok_or_else(|| Error::new(ErrorKind::Security, "server requires a password"))?;
                            let mut b = BytesMut::new();
                            frontend::password_message(pw.expose(), &mut b).map_err(|e| pg_err(e.to_string()))?;
                            self.send(&b).await?;
                        }
                        5 => {
                            let pw = p.password.ok_or_else(|| Error::new(ErrorKind::Security, "server requires a password"))?;
                            let mut salt = [0u8; 4];
                            body.copy_to_slice(&mut salt);
                            let hash = postgres_protocol::authentication::md5_hash(p.user.as_bytes(), pw.expose(), salt);
                            let mut b = BytesMut::new();
                            frontend::password_message(hash.as_bytes(), &mut b).map_err(|e| pg_err(e.to_string()))?;
                            self.send(&b).await?;
                        }
                        10 => {
                            let mechs: Vec<String> = body
                                .split(|b| *b == 0)
                                .filter(|s| !s.is_empty())
                                .map(|s| String::from_utf8_lossy(s).into_owned())
                                .collect();
                            if !mechs.iter().any(|m| m == sasl::SCRAM_SHA_256) {
                                return Err(Error::new(ErrorKind::Security, format!("unsupported SASL mechanisms: {mechs:?}")));
                            }
                            let pw = p.password.ok_or_else(|| Error::new(ErrorKind::Security, "server requires a password"))?;
                            let cb = if self.tls { ChannelBinding::unrequested() } else { ChannelBinding::unsupported() };
                            let s = ScramSha256::new(pw.expose(), cb);
                            let mut b = BytesMut::new();
                            frontend::sasl_initial_response(sasl::SCRAM_SHA_256, s.message(), &mut b).map_err(|e| pg_err(e.to_string()))?;
                            self.send(&b).await?;
                            scram = Some(s);
                        }
                        11 => {
                            let s = scram.as_mut().ok_or_else(|| pg_err("unexpected SASL continue"))?;
                            s.update(&body).map_err(|e| Error::new(ErrorKind::Security, format!("SCRAM: {e}")))?;
                            let mut b = BytesMut::new();
                            frontend::sasl_response(s.message(), &mut b).map_err(|e| pg_err(e.to_string()))?;
                            self.send(&b).await?;
                        }
                        12 => {
                            let s = scram.as_mut().ok_or_else(|| pg_err("unexpected SASL final"))?;
                            s.finish(&body)
                                .map_err(|e| Error::new(ErrorKind::Security, format!("SCRAM server verification failed: {e}")))?;
                        }
                        other => return Err(Error::new(ErrorKind::Security, format!("unsupported authentication method {other}"))),
                    }
                }
                b'E' => return Err(server_error(body)),
                b'Z' => return Ok(()),
                _ => {} // ParameterStatus, BackendKeyData, NoticeResponse
            }
        }
    }

    pub async fn send(&mut self, b: &[u8]) -> Result<()> {
        self.io.write_all(b).await.map_err(|e| pg_err(format!("write: {e}")))?;
        self.io.flush().await.map_err(|e| pg_err(format!("flush: {e}")))
    }

    /// Read one backend message. Cancel-safe.
    pub async fn read_msg(&mut self) -> Result<(u8, Bytes)> {
        loop {
            if self.rbuf.len() >= 5 {
                let len = i32::from_be_bytes(self.rbuf[1..5].try_into().expect("4 bytes")) as usize;
                if len < 4 {
                    return Err(pg_err("protocol error: invalid message length"));
                }
                if self.rbuf.len() > len {
                    let mut msg = self.rbuf.split_to(1 + len);
                    let tag = msg[0];
                    msg.advance(5);
                    return Ok((tag, msg.freeze()));
                }
            }
            let n = self.io.read_buf(&mut self.rbuf).await.map_err(|e| pg_err(format!("read: {e}")))?;
            if n == 0 {
                return Err(pg_err("connection closed by server"));
            }
        }
    }

    /// Run a simple query; returns rows as text values.
    pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let mut b = BytesMut::new();
        frontend::query(sql, &mut b).map_err(|e| pg_err(e.to_string()))?;
        self.send(&b).await?;
        let mut rows = Vec::new();
        let mut err = None;
        loop {
            let (tag, mut body) = self.read_msg().await?;
            match tag {
                b'D' => {
                    let n = body.get_i16();
                    let mut row = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        let len = body.get_i32();
                        if len < 0 {
                            row.push(None);
                        } else {
                            let v = body.split_to(len as usize);
                            row.push(Some(String::from_utf8_lossy(&v).into_owned()));
                        }
                    }
                    rows.push(row);
                }
                b'E' => err = Some(server_error(body)),
                b'Z' => break,
                _ => {}
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }

    /// Issue `START_REPLICATION` and wait for CopyBothResponse.
    pub async fn start_copy_both(&mut self, sql: &str) -> Result<()> {
        let mut b = BytesMut::new();
        frontend::query(sql, &mut b).map_err(|e| pg_err(e.to_string()))?;
        self.send(&b).await?;
        loop {
            let (tag, body) = self.read_msg().await?;
            match tag {
                b'W' => return Ok(()),
                b'E' => return Err(server_error(body)),
                b'N' | b'S' => {}
                other => return Err(pg_err(format!("unexpected message `{}` starting replication", other as char))),
            }
        }
    }

    /// Send a standby status update.
    pub async fn send_status(&mut self, lsn: u64, reply: bool) -> Result<()> {
        let mut payload = BytesMut::with_capacity(34);
        payload.put_u8(b'r');
        payload.put_u64(lsn); // written
        payload.put_u64(lsn); // flushed
        payload.put_u64(lsn); // applied
        payload.put_i64(pg_now_micros());
        payload.put_u8(reply as u8);
        let mut b = BytesMut::new();
        frontend::CopyData::new(&payload[..]).map_err(|e| pg_err(e.to_string()))?.write(&mut b);
        self.send(&b).await
    }

    pub async fn terminate(mut self) {
        let mut b = BytesMut::new();
        frontend::terminate(&mut b);
        let _ = self.send(&b).await;
        let _ = self.io.shutdown().await;
    }
}

/// Microseconds since 2000-01-01 (PostgreSQL epoch).
pub(crate) fn pg_now_micros() -> i64 {
    const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    (d.as_secs() as i64 - PG_EPOCH_OFFSET_SECS) * 1_000_000 + i64::from(d.subsec_micros())
}

/// RFC 3339 from PostgreSQL-epoch microseconds.
pub(crate) fn pg_time_rfc3339(micros: i64) -> String {
    const PG_EPOCH_OFFSET_SECS: i64 = 946_684_800;
    let secs = micros.div_euclid(1_000_000) + PG_EPOCH_OFFSET_SECS;
    let nanos = (micros.rem_euclid(1_000_000) * 1000) as u32;
    chrono::DateTime::from_timestamp(secs, nanos).map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)).unwrap_or_default()
}

pub(crate) fn parse_lsn(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}

pub(crate) fn format_lsn(v: u64) -> String {
    format!("{:X}/{:X}", v >> 32, v & 0xffff_ffff)
}

pub(crate) fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Quote a literal for a *simple query* (requires standard_conforming_strings
/// = on, which preflight verifies).
pub(crate) fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        assert_eq!(parse_lsn("16/B374D848"), Some(0x16_B374_D848));
        assert_eq!(format_lsn(0x16_B374_D848), "16/B374D848");
        assert_eq!(parse_lsn("0/0"), Some(0));
        assert_eq!(parse_lsn("garbage"), None);
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
    }

    #[test]
    fn pg_epoch_time() {
        assert_eq!(pg_time_rfc3339(0), "2000-01-01T00:00:00.000000Z");
    }

    #[test]
    fn error_codes_are_classified() {
        let mut b = BytesMut::new();
        b.put_u8(b'S');
        b.put_slice(b"ERROR\0");
        b.put_u8(b'C');
        b.put_slice(b"58P01\0");
        b.put_u8(b'M');
        b.put_slice(b"requested WAL segment has already been removed\0");
        b.put_u8(0);
        let e = server_error(b.freeze());
        assert_eq!(e.kind(), ErrorKind::ResumeHistoryLost);
        assert!(!e.is_retryable());
    }
}
