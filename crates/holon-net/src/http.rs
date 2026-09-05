//! The HTTP/1.1 face: asks, tells, publish, SSE and NDJSON long-poll
//! (ADR-0017 §3).
//!
//! Hand-rolled on purpose: the parser accepts exactly what the routes need
//! (request line, headers, `Content-Length` bodies, keep-alive), refuses the
//! rest with a status code, and adds no dependency. Requests on one
//! connection are served strictly in order — one in flight at a time — so a
//! pipelined client gets its responses in request order.
//!
//! # Routes
//!
//! | method + path | body | reply |
//! |---|---|---|
//! | `POST /actors/{name}/{schema}` | payload bytes | `200` + reply bytes, `X-Holon-Schema` |
//! | `POST /actors/{name}/{schema}?tell` | payload bytes | `202` |
//! | `POST /topics/{topic}[?event=&window=]` | message bytes | `200 {"seq":N}` |
//! | `GET /topics/{topic}/sse[?from=]` (`Last-Event-ID`) | — | `text/event-stream` |
//! | `GET /topics/{topic}[?from=&max=&wait_ms=]` | — | `application/x-ndjson` |
//! | `GET /topics/{topic}/head` | — | `200 {"head":N}` |
//! | `DELETE /topics/{topic}` | — | `204`, window released (slot tombstoned) |
//! | `GET /objects/{key}[?version=]` | — | `200` + streamed body, `ETag`, `410` when superseded |
//! | `GET /healthz` | — | `200 ok` |
//!
//! SSE frames are `id: <seq>`, `event: <name>` (omitted when empty) and
//! `data:` lines. A payload that is not UTF-8 is base64-encoded and flagged
//! with a `:b64` comment line before it. Lag is `event: lagged` with
//! `{"resume":N,"head":M}`; the browser's automatic `Last-Event-ID` reconnect
//! then resumes exactly there.

use std::time::{Duration, Instant};

use holon_core::{ActorId, Envelope};
use holon_topic::MessageView;

use crate::conn::{AskOrigin, Connection, InFlight, SubMode};
use crate::error::{Error, Result};
use crate::gateway::{GatewayStats, ShardCtx};
use crate::wire;

/// Largest request body accepted.
pub const MAX_BODY: usize = 1 << 20;

/// Largest request head (line + headers) accepted.
pub const MAX_HEAD: usize = 16 << 10;

/// Default long-poll wait.
const DEFAULT_WAIT: Duration = Duration::from_secs(30);

/// Per-connection HTTP state.
#[derive(Debug)]
pub struct HttpState {
    /// A request is being answered (ask in flight or long-poll waiting):
    /// do not read the next one yet.
    pub busy: bool,
    /// The connection is an SSE stream.
    pub sse: bool,
    /// Last byte written (heartbeats).
    pub last_write: Instant,
    /// Reply with `Connection: close`.
    pub close: bool,
}

impl Default for HttpState {
    fn default() -> HttpState {
        HttpState {
            busy: false,
            sse: false,
            last_write: Instant::now(),
            close: false,
        }
    }
}

/// A parsed request.
#[derive(Debug)]
pub struct Request {
    /// `GET` / `POST` / ...
    pub method: String,
    /// Path without the query.
    pub path: String,
    /// Query parameters, in order.
    pub query: Vec<(String, String)>,
    /// Lower-cased header names and their values.
    pub headers: Vec<(String, String)>,
    /// Body bytes.
    pub body: Vec<u8>,
    /// `Connection: close` (or HTTP/1.0 without keep-alive).
    pub close: bool,
}

impl Request {
    /// A header value.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// A query parameter.
    pub fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// A numeric query parameter.
    pub fn num<T: std::str::FromStr>(&self, name: &str) -> Option<T> {
        self.param(name).and_then(|v| v.parse().ok())
    }
}

/// Parse one request from the front of `buf`. `Ok(None)` = incomplete.
pub fn parse_request(buf: &[u8]) -> Result<Option<(Request, usize)>> {
    let Some(head_end) = find(buf, b"\r\n\r\n") else {
        if buf.len() > MAX_HEAD {
            return Err(Error::Http {
                status: 431,
                reason: "request head too large",
            });
        }
        return Ok(None);
    };
    if head_end > MAX_HEAD {
        return Err(Error::Http {
            status: 431,
            reason: "request head too large",
        });
    }
    let head = std::str::from_utf8(&buf[..head_end]).map_err(|_| Error::Http {
        status: 400,
        reason: "non-UTF-8 request head",
    })?;
    let mut lines = head.split("\r\n");
    let line = lines.next().unwrap_or("");
    let mut parts = line.split(' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("");
    let version = parts.next().unwrap_or("HTTP/1.1");
    if method.is_empty() || target.is_empty() || !version.starts_with("HTTP/1.") {
        return Err(Error::Http {
            status: 400,
            reason: "bad request line",
        });
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target.to_string(), Vec::new()),
    };
    let mut headers = Vec::new();
    let mut content_length = 0usize;
    let mut close = version == "HTTP/1.0";
    for l in lines {
        let Some((k, v)) = l.split_once(':') else {
            continue;
        };
        let k = k.trim().to_ascii_lowercase();
        let v = v.trim().to_string();
        match k.as_str() {
            "content-length" => {
                content_length = v.parse().map_err(|_| Error::Http {
                    status: 400,
                    reason: "bad content-length",
                })?
            }
            "connection" => {
                let v = v.to_ascii_lowercase();
                if v.contains("close") {
                    close = true;
                } else if v.contains("keep-alive") {
                    close = false;
                }
            }
            "transfer-encoding" => {
                return Err(Error::Http {
                    status: 411,
                    reason: "chunked requests are not accepted; send content-length",
                })
            }
            _ => {}
        }
        headers.push((k, v));
    }
    if content_length > MAX_BODY {
        return Err(Error::Http {
            status: 413,
            reason: "body too large",
        });
    }
    let body_start = head_end + 4;
    let total = body_start + content_length;
    if buf.len() < total {
        return Ok(None);
    }
    Ok(Some((
        Request {
            method,
            path,
            query,
            headers,
            body: buf[body_start..total].to_vec(),
            close,
        },
        total,
    )))
}

fn parse_query(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (kv.to_string(), String::new()),
        })
        .collect()
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Handle every complete request in the read buffer (one at a time).
pub fn dispatch(ctx: &mut ShardCtx<'_>, conn: &mut Connection) -> Result<()> {
    loop {
        if conn.http.busy || conn.http.sse || conn.close_after_flush {
            return Ok(());
        }
        let (req, n) = match parse_request(conn.inbound()) {
            Ok(Some(v)) => v,
            Ok(None) => return Ok(()),
            Err(Error::Http { status, reason }) => {
                write_response(conn, status, "text/plain", reason.as_bytes(), true);
                conn.close_after_flush = true;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        conn.consume(n);
        conn.requests += 1;
        conn.http.close = req.close;
        GatewayStats::inc(&ctx.stats.requests);
        if let Err(e) = route(ctx, conn, &req) {
            let (status, reason) = match e {
                Error::Http { status, reason } => (status, reason),
                _ => (500, "internal error"),
            };
            write_response(conn, status, "text/plain", reason.as_bytes(), req.close);
        }
        if conn.http.close && !conn.http.busy {
            conn.close_after_flush = true;
        }
    }
}

fn bad(status: u16, reason: &'static str) -> Error {
    Error::Http { status, reason }
}

fn route(ctx: &mut ShardCtx<'_>, conn: &mut Connection, req: &Request) -> Result<()> {
    let segs: Vec<&str> = req.path.split('/').filter(|s| !s.is_empty()).collect();
    match (req.method.as_str(), segs.as_slice()) {
        ("GET", ["healthz"]) => {
            write_response(conn, 200, "text/plain", b"ok", req.close);
            Ok(())
        }
        ("POST", ["actors", name, schema]) => {
            let schema: u32 = schema
                .parse()
                .map_err(|_| bad(400, "schema must be a u32"))?;
            let to = ActorId::named(name);
            if req.param("tell").is_some() {
                ctx.aref
                    .tell_raw_to(to, schema, &req.body)
                    .map_err(|_| bad(503, "tell rejected"))?;
                GatewayStats::inc(&ctx.stats.tells);
                write_response(conn, 202, "text/plain", b"", req.close);
                return Ok(());
            }
            if req.body.len() > wire::MAX_WIRE_BODY {
                return Err(bad(413, "payload too large for an inline message"));
            }
            let pending = ctx
                .aref
                .ask_start_sized(to, schema, &req.body, ctx.cfg.max_reply)
                .map_err(|_| bad(503, "ask rejected"))?;
            GatewayStats::inc(&ctx.stats.asks);
            conn.asks.push(InFlight {
                pending,
                origin: AskOrigin::Http,
                started: Instant::now(),
            });
            conn.http.busy = true;
            Ok(())
        }
        ("POST", ["topics", topic]) => {
            let event = req.param("event").unwrap_or("");
            let window: u32 = req.num("window").unwrap_or(0);
            let ct = req.header("content-type").unwrap_or("");
            let ct = &ct[..ct.len().min(wire::MAX_CONTENT_TYPE)];
            let seq = ctx
                .topics
                .publish(topic, ctx.policy(window), event, ct, &req.body)
                .map_err(|e| match e {
                    holon_topic::Error::BadName(_) => bad(400, "bad topic or event name"),
                    _ => bad(503, "publish failed"),
                })?;
            GatewayStats::inc(&ctx.stats.publishes);
            let body = format!("{{\"seq\":{seq}}}");
            write_response(conn, 200, "application/json", body.as_bytes(), req.close);
            Ok(())
        }
        ("DELETE", ["topics", topic]) => {
            ctx.topics
                .delete(topic)
                .map_err(|_| bad(400, "bad topic name"))?;
            ctx.forget_topic(topic);
            write_response(conn, 204, "text/plain", b"", req.close);
            Ok(())
        }
        ("GET", ["topics", topic, "head"]) => {
            let head = ctx
                .topics
                .head(topic)
                .map_err(|_| bad(400, "bad topic name"))?;
            let body = format!("{{\"head\":{head}}}");
            write_response(conn, 200, "application/json", body.as_bytes(), req.close);
            Ok(())
        }
        ("GET", ["topics", topic, "sse"]) => {
            let from = req
                .header("last-event-id")
                .and_then(|v| v.parse::<u64>().ok())
                .or_else(|| req.num::<u64>("from"))
                .unwrap_or(u64::MAX);
            ctx.subscribe(conn, topic, from, SubMode::Stream)
                .map_err(|_| bad(400, "bad topic name"))?;
            let out = conn.out();
            out.extend_from_slice(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nX-Accel-Buffering: no\r\n\r\n:ok\n\n",
            );
            conn.http.sse = true;
            conn.http.last_write = Instant::now();
            Ok(())
        }
        ("GET", ["topics", topic]) => {
            let from: u64 = req.num("from").unwrap_or(0);
            let max: u32 = req.num("max").unwrap_or(1024).clamp(1, 65536);
            let wait = req
                .num::<u64>("wait_ms")
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_WAIT);
            ctx.subscribe(
                conn,
                topic,
                from,
                SubMode::LongPoll {
                    max,
                    deadline: Instant::now() + wait,
                },
            )
            .map_err(|_| bad(400, "bad topic name"))?;
            conn.http.busy = true;
            Ok(())
        }
        // An object key may contain `/`, so it is the whole tail. The body is
        // not written here: the head goes out and `object::advance` pumps the
        // payload across as many reactor passes as it takes.
        ("GET", ["objects", key @ ..]) => crate::object::get(ctx, conn, req, &key.join("/")),
        (_, ["actors", ..] | ["topics", ..] | ["objects", ..] | ["healthz"]) => {
            Err(bad(405, "method not allowed"))
        }
        _ => Err(bad(404, "not found")),
    }
}

/// Write a full response.
pub fn write_response(conn: &mut Connection, status: u16, ct: &str, body: &[u8], close: bool) {
    let reason = reason(status);
    let out = conn.out();
    out.extend_from_slice(format!("HTTP/1.1 {status} {reason}\r\n").as_bytes());
    out.extend_from_slice(
        format!("Content-Type: {ct}\r\nContent-Length: {}\r\n", body.len()).as_bytes(),
    );
    if close {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    conn.http.last_write = Instant::now();
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        410 => "Gone",
        411 => "Length Required",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Status",
    }
}

/// Answer an HTTP ask with the actor's reply.
pub fn write_ask_reply(conn: &mut Connection, renv: &Envelope, body: &[u8]) {
    let close = conn.http.close;
    let out = conn.out();
    out.extend_from_slice(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n");
    out.extend_from_slice(
        format!(
            "X-Holon-Schema: {}\r\nContent-Length: {}\r\n",
            renv.schema_id,
            body.len()
        )
        .as_bytes(),
    );
    if close {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    conn.http.last_write = Instant::now();
    conn.http.busy = false;
    if close {
        conn.close_after_flush = true;
    }
}

/// Answer an HTTP ask that failed.
pub fn write_ask_error(conn: &mut Connection, code: u32, msg: &str) {
    let status = match code {
        wire::code::CANCELLED => 504,
        wire::code::OVERLOADED => 503,
        _ => 502,
    };
    let close = conn.http.close;
    write_response(conn, status, "text/plain", msg.as_bytes(), close);
    conn.http.busy = false;
    if close {
        conn.close_after_flush = true;
    }
}

/// Write one SSE event.
pub fn write_sse_event(conn: &mut Connection, m: &MessageView<'_>) {
    let out = conn.out();
    out.extend_from_slice(b"id: ");
    out.extend_from_slice(m.seq.to_string().as_bytes());
    out.push(b'\n');
    if !m.event.is_empty() {
        out.extend_from_slice(b"event: ");
        out.extend_from_slice(m.event.as_bytes());
        out.push(b'\n');
    }
    match std::str::from_utf8(m.data) {
        Ok(text) => {
            for line in text.split('\n') {
                out.extend_from_slice(b"data: ");
                out.extend_from_slice(line.as_bytes());
                out.push(b'\n');
            }
        }
        Err(_) => {
            out.extend_from_slice(b":b64\ndata: ");
            base64_into(out, m.data);
            out.push(b'\n');
        }
    }
    out.push(b'\n');
    conn.http.last_write = Instant::now();
}

/// Write an SSE `lagged` event.
pub fn write_lagged(conn: &mut Connection, _topic: &str, resume_seq: u64, head: u64) {
    let out = conn.out();
    out.extend_from_slice(
        format!("event: lagged\ndata: {{\"resume\":{resume_seq},\"head\":{head}}}\n\n").as_bytes(),
    );
    conn.http.last_write = Instant::now();
}

/// Append one NDJSON line for a long-poll answer.
pub fn push_ndjson(out: &mut Vec<u8>, m: &MessageView<'_>) {
    out.extend_from_slice(b"{\"seq\":");
    out.extend_from_slice(m.seq.to_string().as_bytes());
    out.extend_from_slice(b",\"ts_ms\":");
    out.extend_from_slice(m.ts_ms.to_string().as_bytes());
    out.extend_from_slice(b",\"event\":\"");
    json_escape_into(out, m.event);
    out.extend_from_slice(b"\",\"content_type\":\"");
    json_escape_into(out, m.content_type);
    match std::str::from_utf8(m.data) {
        Ok(text) => {
            out.extend_from_slice(b"\",\"data\":\"");
            json_escape_into(out, text);
        }
        Err(_) => {
            out.extend_from_slice(b"\",\"data_b64\":\"");
            base64_into(out, m.data);
        }
    }
    out.extend_from_slice(b"\"}\n");
}

/// Finish a long-poll: write the NDJSON body and drop the subscription.
pub fn finish_longpoll(conn: &mut Connection, sub_index: usize, body: Vec<u8>, cursor: u64) {
    conn.subs.swap_remove(sub_index);
    let close = conn.http.close;
    let out = conn.out();
    out.extend_from_slice(b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\n");
    out.extend_from_slice(
        format!(
            "X-Next-Cursor: {cursor}\r\nContent-Length: {}\r\n",
            body.len()
        )
        .as_bytes(),
    );
    if close {
        out.extend_from_slice(b"Connection: close\r\n");
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&body);
    conn.http.last_write = Instant::now();
    conn.http.busy = false;
    if close {
        conn.close_after_flush = true;
    }
}

/// Timer pass: SSE heartbeat, long-poll deadline.
pub fn tick(ctx: &mut ShardCtx<'_>, conn: &mut Connection, now: Instant) {
    if conn.http.sse && now.duration_since(conn.http.last_write) >= ctx.cfg.sse_heartbeat {
        conn.out().extend_from_slice(b":hb\n\n");
        conn.http.last_write = now;
    }
    if let Some(i) = conn.subs.iter().position(|s| match s.mode {
        SubMode::LongPoll { deadline, .. } => now >= deadline,
        SubMode::Stream => false,
    }) {
        let cursor = conn.subs[i].cursor;
        finish_longpoll(conn, i, Vec::new(), cursor);
    }
}

fn json_escape_into(out: &mut Vec<u8>, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => {
                out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes())
            }
            c => {
                let mut b = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
            }
        }
    }
}

/// Standard base64 (with padding).
pub fn base64_into(out: &mut Vec<u8>, data: &[u8]) {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(T[(n >> 18) as usize & 63]);
        out.push(T[(n >> 12) as usize & 63]);
        out.push(if chunk.len() > 1 {
            T[(n >> 6) as usize & 63]
        } else {
            b'='
        });
        out.push(if chunk.len() > 2 {
            T[n as usize & 63]
        } else {
            b'='
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_requests_incrementally() {
        let raw = b"POST /actors/echo/1001?tell HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhelloGET /healthz HTTP/1.1\r\n\r\n";
        assert!(parse_request(&raw[..20]).unwrap().is_none());
        let (r, n) = parse_request(raw).unwrap().unwrap();
        assert_eq!(r.method, "POST");
        assert_eq!(r.path, "/actors/echo/1001");
        assert_eq!(r.param("tell"), Some(""));
        assert_eq!(r.header("host"), Some("x"));
        assert_eq!(r.body, b"hello");
        assert!(r.close);
        let (r2, n2) = parse_request(&raw[n..]).unwrap().unwrap();
        assert_eq!(r2.method, "GET");
        assert_eq!(r2.path, "/healthz");
        assert!(!r2.close);
        assert_eq!(n + n2, raw.len());
        assert!(matches!(
            parse_request(b"GET / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"),
            Err(Error::Http { status: 411, .. })
        ));
        assert!(matches!(
            parse_request(b"nonsense\r\n\r\n"),
            Err(Error::Http { status: 400, .. })
        ));
    }

    #[test]
    fn json_and_base64() {
        let mut out = Vec::new();
        json_escape_into(&mut out, "a\"b\\c\nd\u{1}");
        assert_eq!(out, b"a\\\"b\\\\c\\nd\\u0001");
        let mut b = Vec::new();
        base64_into(&mut b, b"hi");
        assert_eq!(b, b"aGk=");
        b.clear();
        base64_into(&mut b, b"hello");
        assert_eq!(b, b"aGVsbG8=");
        b.clear();
        base64_into(&mut b, b"abc");
        assert_eq!(b, b"YWJj");
    }
}
