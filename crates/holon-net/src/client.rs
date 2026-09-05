//! Blocking clients for tests, demos and benches: a TCP frame client and a
//! minimal HTTP/1.1 client. Neither is the product; the wire is.

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use holon_core::{ActorId, Envelope, MessageKind, ENVELOPE_SIZE};

use crate::error::{Error, Result};
use crate::wire::{
    self, decode_frame, encode_frame, encode_pod_frame, read_pod, read_pod_and_rest, ErrorHeader,
    EventHeader, Lagged, PublishAck, PublishHeader, Subscribe, SubscribeAck, SCHEMA_ERROR,
    SCHEMA_EVENT, SCHEMA_LAGGED, SCHEMA_PING, SCHEMA_PUBLISH, SCHEMA_SUBSCRIBE,
};

/// A decoded inbound frame.
#[derive(Clone, Debug)]
pub enum Inbound {
    /// A reply to an ask (`corr` identifies it).
    Reply {
        /// The envelope.
        env: Envelope,
        /// The body.
        body: Vec<u8>,
    },
    /// An error reply.
    Err {
        /// The envelope (`corr` of the request; 0 for a tell).
        env: Envelope,
        /// The error code.
        code: u32,
        /// The message.
        msg: String,
    },
    /// A topic message.
    Event {
        /// The header.
        head: EventHeader,
        /// The data.
        data: Vec<u8>,
    },
    /// A lag notice.
    Lagged(Lagged),
    /// Anything else.
    Other {
        /// The envelope.
        env: Envelope,
        /// The body.
        body: Vec<u8>,
    },
}

/// A blocking TCP frame client.
pub struct FrameClient {
    stream: TcpStream,
    rbuf: Vec<u8>,
    rpos: usize,
    wbuf: Vec<u8>,
    corr: u64,
    me: ActorId,
}

impl FrameClient {
    /// Connect.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<FrameClient> {
        let stream = TcpStream::connect(addr)?;
        let _ = stream.set_nodelay(true);
        Ok(FrameClient {
            stream,
            rbuf: Vec::with_capacity(1 << 16),
            rpos: 0,
            wbuf: Vec::with_capacity(1 << 16),
            corr: 1,
            me: ActorId::new(0, std::process::id()),
        })
    }

    /// Set a read timeout (`None` = block forever).
    pub fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<()> {
        self.stream.set_read_timeout(t)?;
        Ok(())
    }

    /// Queue an ask frame; returns its `corr`. Call [`FrameClient::flush`].
    pub fn queue_ask(&mut self, to: ActorId, schema_id: u32, body: &[u8]) -> Result<u64> {
        let corr = self.corr;
        self.corr += 1;
        let env = wire::ask(to, self.me, corr, schema_id);
        encode_frame(&mut self.wbuf, &env, body)?;
        Ok(corr)
    }

    /// Queue a tell frame.
    pub fn queue_tell(&mut self, to: ActorId, schema_id: u32, body: &[u8]) -> Result<()> {
        let env = wire::tell(to, self.me, schema_id);
        encode_frame(&mut self.wbuf, &env, body)
    }

    /// Queue a control ask with a POD body.
    pub fn queue_control<T: bytemuck::Pod>(&mut self, schema_id: u32, body: &T) -> Result<u64> {
        let corr = self.corr;
        self.corr += 1;
        let env = wire::ask(ActorId::NONE, self.me, corr, schema_id);
        encode_pod_frame(&mut self.wbuf, &env, body)?;
        Ok(corr)
    }

    /// Write everything queued.
    pub fn flush(&mut self) -> Result<()> {
        self.stream.write_all(&self.wbuf)?;
        self.wbuf.clear();
        Ok(())
    }

    /// Read the next frame (blocking).
    pub fn recv(&mut self) -> Result<Inbound> {
        loop {
            if let Some((env, body, n)) = decode_frame(&self.rbuf[self.rpos..])? {
                let body = body.to_vec();
                self.rpos += n;
                if self.rpos == self.rbuf.len() {
                    self.rbuf.clear();
                    self.rpos = 0;
                } else if self.rpos > (1 << 20) {
                    self.rbuf.drain(..self.rpos);
                    self.rpos = 0;
                }
                return Ok(classify(env, body));
            }
            let mut tmp = [0u8; 1 << 16];
            let n = self.stream.read(&mut tmp)?;
            if n == 0 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "gateway closed the connection",
                )));
            }
            self.rbuf.extend_from_slice(&tmp[..n]);
        }
    }

    /// One ask, round trip.
    pub fn ask(&mut self, to: ActorId, schema_id: u32, body: &[u8]) -> Result<(Envelope, Vec<u8>)> {
        let corr = self.queue_ask(to, schema_id, body)?;
        self.flush()?;
        loop {
            match self.recv()? {
                Inbound::Reply { env, body } if env.corr == corr => return Ok((env, body)),
                Inbound::Err { env, code, msg } if env.corr == corr => {
                    return Err(Error::Protocol(leak(format!("error {code}: {msg}"))))
                }
                _ => continue,
            }
        }
    }

    /// Ping round trip.
    pub fn ping(&mut self) -> Result<()> {
        let corr = self.queue_control(SCHEMA_PING, &())?;
        self.flush()?;
        loop {
            match self.recv()? {
                Inbound::Reply { env, .. } if env.corr == corr => return Ok(()),
                Inbound::Err { env, .. } if env.corr == corr => {
                    return Err(Error::Protocol("ping failed"))
                }
                _ => continue,
            }
        }
    }

    /// Subscribe; returns the ack.
    pub fn subscribe(&mut self, topic: &str, from_seq: u64) -> Result<SubscribeAck> {
        let corr = self.queue_control(SCHEMA_SUBSCRIBE, &Subscribe::new(topic, from_seq))?;
        self.flush()?;
        loop {
            match self.recv()? {
                Inbound::Reply { env, body } if env.corr == corr => return read_pod(&body),
                Inbound::Err { env, code, msg } if env.corr == corr => {
                    return Err(Error::Protocol(leak(format!(
                        "subscribe error {code}: {msg}"
                    ))))
                }
                _ => continue,
            }
        }
    }

    /// Publish; returns the seq.
    pub fn publish(&mut self, topic: &str, event: &str, data: &[u8]) -> Result<u64> {
        let corr = self.corr;
        self.corr += 1;
        let env = wire::ask(ActorId::NONE, self.me, corr, SCHEMA_PUBLISH);
        let h = PublishHeader::new(topic, event, "", 0);
        let mut body = bytemuck::bytes_of(&h).to_vec();
        body.extend_from_slice(data);
        encode_frame(&mut self.wbuf, &env, &body)?;
        self.flush()?;
        loop {
            match self.recv()? {
                Inbound::Reply { env, body } if env.corr == corr => {
                    return Ok(read_pod::<PublishAck>(&body)?.seq)
                }
                Inbound::Err { env, code, msg } if env.corr == corr => {
                    return Err(Error::Protocol(leak(format!(
                        "publish error {code}: {msg}"
                    ))))
                }
                _ => continue,
            }
        }
    }

    /// Queue a publish as a tell (no ack).
    pub fn queue_publish_tell(&mut self, topic: &str, event: &str, data: &[u8]) -> Result<()> {
        let env = wire::tell(ActorId::NONE, self.me, SCHEMA_PUBLISH);
        let h = PublishHeader::new(topic, event, "", 0);
        let mut body = bytemuck::bytes_of(&h).to_vec();
        body.extend_from_slice(data);
        encode_frame(&mut self.wbuf, &env, &body)
    }

    /// The raw stream (for shutdown tricks).
    #[inline]
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }
}

fn classify(env: Envelope, body: Vec<u8>) -> Inbound {
    match env.kind() {
        Ok(MessageKind::Reply) => Inbound::Reply { env, body },
        Ok(MessageKind::Err) => {
            let (h, rest) = read_pod_and_rest::<ErrorHeader>(&body)
                .unwrap_or((ErrorHeader { code: 0, _pad: 0 }, &[][..]));
            Inbound::Err {
                env,
                code: h.code,
                msg: String::from_utf8_lossy(rest).into_owned(),
            }
        }
        Ok(MessageKind::Tell) if env.schema_id == SCHEMA_EVENT && env.schema_id != SCHEMA_ERROR => {
            match read_pod_and_rest::<EventHeader>(&body) {
                Ok((head, data)) => Inbound::Event {
                    head,
                    data: data.to_vec(),
                },
                Err(_) => Inbound::Other { env, body },
            }
        }
        Ok(MessageKind::Tell) if env.schema_id == SCHEMA_LAGGED => {
            match read_pod::<Lagged>(&body) {
                Ok(l) => Inbound::Lagged(l),
                Err(_) => Inbound::Other { env, body },
            }
        }
        _ => Inbound::Other { env, body },
    }
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// A minimal blocking HTTP/1.1 response.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    /// Status code.
    pub status: u16,
    /// Lower-cased headers.
    pub headers: Vec<(String, String)>,
    /// Body.
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// A header value.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// A keep-alive HTTP/1.1 client over one connection.
pub struct HttpClient {
    stream: TcpStream,
    rbuf: Vec<u8>,
    rpos: usize,
}

impl HttpClient {
    /// Connect.
    pub fn connect(addr: impl ToSocketAddrs) -> Result<HttpClient> {
        let stream = TcpStream::connect(addr)?;
        let _ = stream.set_nodelay(true);
        Ok(HttpClient {
            stream,
            rbuf: Vec::with_capacity(1 << 16),
            rpos: 0,
        })
    }

    /// Set a read timeout.
    pub fn set_read_timeout(&mut self, t: Option<Duration>) -> Result<()> {
        self.stream.set_read_timeout(t)?;
        Ok(())
    }

    /// Send a request and read the response (`Content-Length` bodies only).
    pub fn request(
        &mut self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<HttpResponse> {
        self.send(method, target, headers, body)?;
        self.read_response()
    }

    /// Send a request without reading the response (for SSE / pipelining).
    pub fn send(
        &mut self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Result<()> {
        let mut req = format!("{method} {target} HTTP/1.1\r\nHost: holon\r\n");
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        if !body.is_empty() || method == "POST" {
            req.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        req.push_str("\r\n");
        self.stream.write_all(req.as_bytes())?;
        if !body.is_empty() {
            self.stream.write_all(body)?;
        }
        Ok(())
    }

    /// Read one response.
    pub fn read_response(&mut self) -> Result<HttpResponse> {
        self.compact();
        loop {
            if let Some(end) = self.rbuf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&self.rbuf[..end]).into_owned();
                let mut lines = head.split("\r\n");
                let status: u16 = lines
                    .next()
                    .and_then(|l| l.split(' ').nth(1))
                    .and_then(|s| s.parse().ok())
                    .ok_or(Error::Protocol("bad status line"))?;
                let mut headers = Vec::new();
                let mut len = 0usize;
                for l in lines {
                    if let Some((k, v)) = l.split_once(':') {
                        let k = k.trim().to_ascii_lowercase();
                        let v = v.trim().to_string();
                        if k == "content-length" {
                            len = v.parse().unwrap_or(0);
                        }
                        headers.push((k, v));
                    }
                }
                let start = end + 4;
                if self.rbuf.len() >= start + len {
                    let body = self.rbuf[start..start + len].to_vec();
                    self.rbuf.drain(..start + len);
                    return Ok(HttpResponse {
                        status,
                        headers,
                        body,
                    });
                }
            }
            self.fill()?;
        }
    }

    /// Read the response head of an SSE stream (status + headers). Events
    /// then come from [`HttpClient::read_sse_event`].
    pub fn read_head(&mut self) -> Result<(u16, Vec<(String, String)>)> {
        self.compact();
        loop {
            if let Some(end) = self.rbuf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&self.rbuf[..end]).into_owned();
                let mut lines = head.split("\r\n");
                let status: u16 = lines
                    .next()
                    .and_then(|l| l.split(' ').nth(1))
                    .and_then(|s| s.parse().ok())
                    .ok_or(Error::Protocol("bad status line"))?;
                let headers = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    .collect();
                self.rbuf.drain(..end + 4);
                return Ok((status, headers));
            }
            self.fill()?;
        }
    }

    /// Read one SSE event: `(id, event, data)`. Comments are skipped.
    pub fn read_sse_event(&mut self) -> Result<SseEvent> {
        loop {
            let tail = &self.rbuf[self.rpos..];
            if let Some(end) = tail.windows(2).position(|w| w == b"\n\n") {
                let block = String::from_utf8_lossy(&tail[..end]).into_owned();
                self.rpos += end + 2;
                if self.rpos == self.rbuf.len() {
                    self.rbuf.clear();
                    self.rpos = 0;
                } else if self.rpos > (1 << 16) {
                    self.rbuf.drain(..self.rpos);
                    self.rpos = 0;
                }
                let mut ev = SseEvent::default();
                let mut data_lines = Vec::new();
                let mut any = false;
                for line in block.split('\n') {
                    if let Some(v) = line.strip_prefix("id: ") {
                        ev.id = v.parse().ok();
                        any = true;
                    } else if let Some(v) = line.strip_prefix("event: ") {
                        ev.event = v.to_string();
                        any = true;
                    } else if let Some(v) = line.strip_prefix("data: ") {
                        data_lines.push(v.to_string());
                        any = true;
                    } else if line == ":b64" {
                        ev.b64 = true;
                    }
                }
                if !any {
                    // A comment / heartbeat block. Returned rather than
                    // swallowed: looping here would reset the socket read
                    // timeout on every heartbeat and a caller's deadline
                    // would never be re-checked, so an idle stream blocked
                    // forever.
                    return Ok(SseEvent {
                        heartbeat: true,
                        ..SseEvent::default()
                    });
                }
                ev.data = data_lines.join("\n");
                return Ok(ev);
            }
            self.fill()?;
        }
    }

    fn compact(&mut self) {
        if self.rpos > 0 {
            self.rbuf.drain(..self.rpos);
            self.rpos = 0;
        }
    }

    fn fill(&mut self) -> Result<()> {
        let mut tmp = [0u8; 1 << 16];
        let n = self.stream.read(&mut tmp)?;
        if n == 0 {
            return Err(Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed the connection",
            )));
        }
        self.rbuf.extend_from_slice(&tmp[..n]);
        Ok(())
    }

    /// The raw stream.
    #[inline]
    pub fn stream(&self) -> &TcpStream {
        &self.stream
    }
}

/// One SSE event as read by [`HttpClient::read_sse_event`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SseEvent {
    /// `id:` line, parsed.
    pub id: Option<u64>,
    /// `event:` line (empty when absent).
    pub event: String,
    /// Joined `data:` lines.
    pub data: String,
    /// The payload was base64-encoded (`:b64` comment preceded it).
    pub b64: bool,
    /// This block carried no `id:`/`event:`/`data:` line — a comment or a
    /// heartbeat. Every other field is empty. Callers that only want data
    /// events skip it, and in doing so get their own deadline checked.
    pub heartbeat: bool,
}

/// Size of one envelope on the wire (re-exported for clients).
pub const FRAME_HEADER: usize = ENVELOPE_SIZE;
