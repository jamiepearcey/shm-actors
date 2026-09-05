//! The TCP face: envelope frames in, envelope frames out (ADR-0017 §3).

use std::time::Instant;

use holon_core::{ActorId, Envelope, MessageKind};
use holon_topic::MessageView;

use crate::conn::{AskOrigin, Connection, InFlight, SubMode};
use crate::error::Result;
use crate::gateway::{reply_env, GatewayStats, ShardCtx};
use crate::wire::{
    self, decode_frame, encode_frame, encode_pod_frame, read_pod, read_pod_and_rest, EventHeader,
    Lagged, PublishAck, PublishHeader, Subscribe, SubscribeAck, Unsubscribe, SCHEMA_ERROR,
    SCHEMA_EVENT, SCHEMA_LAGGED, SCHEMA_PING, SCHEMA_PONG, SCHEMA_PUBLISH, SCHEMA_PUBLISH_ACK,
    SCHEMA_SUBSCRIBE, SCHEMA_SUBSCRIBE_ACK, SCHEMA_UNSUBSCRIBE,
};

/// Handle every complete frame in the read buffer.
pub fn dispatch(ctx: &mut ShardCtx<'_>, conn: &mut Connection) -> Result<()> {
    loop {
        if !conn.wants_read() && conn.inbound().is_empty() {
            return Ok(());
        }
        let (env, body, n) = match decode_frame(conn.inbound())? {
            Some((env, body, n)) => (env, body.to_vec(), n),
            None => return Ok(()),
        };
        conn.consume(n);
        conn.requests += 1;
        GatewayStats::inc(&ctx.stats.requests);
        handle_frame(ctx, conn, env, &body)?;
        // Stop parsing once the connection is saturated; the rest waits in
        // the buffer (and the socket) until asks drain.
        if conn.asks.len() >= crate::conn::MAX_PENDING_ASKS || conn.throttled() {
            return Ok(());
        }
    }
}

fn handle_frame(
    ctx: &mut ShardCtx<'_>,
    conn: &mut Connection,
    env: Envelope,
    body: &[u8],
) -> Result<()> {
    let kind = env.kind()?;
    if wire::is_control(&env) {
        return handle_control(ctx, conn, env, kind, body);
    }
    match kind {
        MessageKind::Tell => {
            GatewayStats::inc(&ctx.stats.tells);
            if let Err(e) = ctx.aref.tell_raw_to(ActorId(env.to), env.schema_id, body) {
                // A tell has no reply channel; report on the connection with
                // the tell's corr (0) so the client can at least see it.
                GatewayStats::inc(&ctx.stats.errors);
                write_error(conn, &env, wire::code::FAILED, &e.to_string());
            }
            Ok(())
        }
        MessageKind::Ask => {
            if conn.asks.len() >= crate::conn::MAX_PENDING_ASKS {
                write_error(
                    conn,
                    &env,
                    wire::code::OVERLOADED,
                    "too many asks in flight",
                );
                return Ok(());
            }
            match ctx
                .aref
                .ask_start_sized(ActorId(env.to), env.schema_id, body, ctx.cfg.max_reply)
            {
                Ok(pending) => {
                    GatewayStats::inc(&ctx.stats.asks);
                    conn.asks.push(InFlight {
                        pending,
                        origin: AskOrigin::Frame(env),
                        started: Instant::now(),
                    });
                }
                Err(e) => {
                    GatewayStats::inc(&ctx.stats.errors);
                    write_error(conn, &env, wire::code::FAILED, &e.to_string());
                }
            }
            Ok(())
        }
        MessageKind::Reply | MessageKind::Err => {
            // Clients do not answer the gateway.
            write_error(conn, &env, wire::code::BAD_REQUEST, "unexpected reply kind");
            Ok(())
        }
    }
}

fn handle_control(
    ctx: &mut ShardCtx<'_>,
    conn: &mut Connection,
    env: Envelope,
    kind: MessageKind,
    body: &[u8],
) -> Result<()> {
    match env.schema_id {
        SCHEMA_PING => {
            if kind == MessageKind::Ask {
                let e = reply_env(&env, SCHEMA_PONG, 0);
                encode_frame(conn.out(), &e, &[])?;
            }
        }
        SCHEMA_SUBSCRIBE => {
            let s: Subscribe = read_pod(body)?;
            match ctx.subscribe(conn, s.topic(), s.from_seq, SubMode::Stream) {
                Ok((topic_id, head, cursor)) => {
                    if kind == MessageKind::Ask {
                        let ack = SubscribeAck {
                            topic_id,
                            _pad: 0,
                            head,
                            cursor,
                        };
                        let e = reply_env(&env, SCHEMA_SUBSCRIBE_ACK, size_of::<SubscribeAck>());
                        encode_pod_frame(conn.out(), &e, &ack)?;
                    }
                }
                Err(e) => write_error(conn, &env, wire::code::BAD_NAME, &e.to_string()),
            }
        }
        SCHEMA_UNSUBSCRIBE => {
            let u: Unsubscribe = read_pod(body)?;
            conn.subs.retain(|s| s.topic != u.topic());
            if kind == MessageKind::Ask {
                let e = reply_env(&env, 0, 0);
                encode_frame(conn.out(), &e, &[])?;
            }
        }
        SCHEMA_PUBLISH => {
            let (h, data): (PublishHeader, &[u8]) = read_pod_and_rest(body)?;
            let policy = ctx.policy(h.window);
            match ctx
                .topics
                .publish(h.topic(), policy, h.event(), h.content_type(), data)
            {
                Ok(seq) => {
                    GatewayStats::inc(&ctx.stats.publishes);
                    if kind == MessageKind::Ask {
                        let e = reply_env(&env, SCHEMA_PUBLISH_ACK, size_of::<PublishAck>());
                        encode_pod_frame(conn.out(), &e, &PublishAck { seq })?;
                    }
                }
                Err(e) => write_error(conn, &env, wire::code::PUBLISH_FAILED, &e.to_string()),
            }
        }
        _ => write_error(
            conn,
            &env,
            wire::code::UNKNOWN_SCHEMA,
            "unknown control schema",
        ),
    }
    Ok(())
}

/// Write an actor's reply to the client with the client's addressing.
pub fn write_reply(conn: &mut Connection, req: &Envelope, renv: &Envelope, body: &[u8]) {
    let e = reply_env(req, renv.schema_id, body.len());
    let _ = encode_frame(conn.out(), &e, body);
}

/// Write an `Err` envelope for `req`.
pub fn write_error(conn: &mut Connection, req: &Envelope, code: u32, msg: &str) {
    let msg = &msg.as_bytes()[..msg.len().min(512)];
    let mut e = reply_env(req, SCHEMA_ERROR, 8 + msg.len());
    e.kind = MessageKind::Err.as_u16();
    let mut body = Vec::with_capacity(8 + msg.len());
    body.extend_from_slice(bytemuck::bytes_of(&wire::ErrorHeader { code, _pad: 0 }));
    body.extend_from_slice(msg);
    let _ = encode_frame(conn.out(), &e, &body);
}

/// Write one topic message as an `EVENT` frame.
pub fn write_event(conn: &mut Connection, topic_id: u32, m: &MessageView<'_>) {
    let h = EventHeader::new(m.seq, m.ts_ms, topic_id, m.event, m.content_type);
    let data = &m.data[..m
        .data
        .len()
        .min(wire::MAX_WIRE_BODY - size_of::<EventHeader>())];
    let e = wire::tell(ActorId::NONE, ActorId::NONE, SCHEMA_EVENT);
    let out = conn.out();
    // Envelope, header, data — three appends, one frame.
    let mut env = e;
    env.body_len = (size_of::<EventHeader>() + data.len()) as u16;
    env.flags |= holon_core::FLAG_INLINE_PAYLOAD;
    out.extend_from_slice(bytemuck::bytes_of(&env));
    out.extend_from_slice(bytemuck::bytes_of(&h));
    out.extend_from_slice(data);
}

/// Write a `LAGGED` frame.
pub fn write_lagged(conn: &mut Connection, topic_id: u32, resume_seq: u64, head: u64) {
    let e = wire::tell(ActorId::NONE, ActorId::NONE, SCHEMA_LAGGED);
    let _ = encode_pod_frame(
        conn.out(),
        &e,
        &Lagged {
            topic_id,
            _pad: 0,
            resume_seq,
            head,
        },
    );
}
