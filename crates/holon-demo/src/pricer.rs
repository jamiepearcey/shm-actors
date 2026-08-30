//! The `Pricer` actor: owns no state — it pins the `curve` cell per message.

use std::path::PathBuf;

use holon_actor::{Actor, CellRef, Cx};
use holon_core::{Envelope, Payload, Reply};

use crate::curve::{interpolate, price, CURVE_KEY};
use crate::messages::{PriceReply, PriceRequest, PRICE_REQUEST_SCHEMA};
use crate::orchestrate::{append_line, unix_nanos};

/// The pricer's service name (its [`ActorId`](holon_core::ActorId) is
/// `ActorId::named(PRICER_NAME)`; every pricer process shares it).
pub const PRICER_NAME: &str = "pricer";

/// The pricer: a [`CellRef`] to `curve` plus process-local scratch.
pub struct Pricer {
    curve: CellRef,
    /// Messages handled by this incarnation (scratch; lost on restart by design).
    handled: u64,
    /// Die (hard) while handling the n-th message.
    kill_after: Option<u64>,
    /// Where to log the kill timestamp.
    result: Option<PathBuf>,
    incarnation: u32,
}

impl Pricer {
    /// A pricer over the `curve` cell. With `kill_after = Some(n)` the n-th
    /// message is handled up to the point where the curve is pinned and the
    /// price computed, then the process `_exit(137)`s — holding its claim and
    /// its journaled pin, with no destructor run.
    pub fn new(kill_after: Option<u64>, result: Option<PathBuf>) -> Pricer {
        Pricer {
            curve: CellRef::new(CURVE_KEY),
            handled: 0,
            kill_after,
            result,
            incarnation: std::process::id(),
        }
    }

    /// Messages handled so far by this incarnation.
    pub fn handled(&self) -> u64 {
        self.handled
    }
}

impl Actor for Pricer {
    fn accepts() -> &'static [u32] {
        &[PRICE_REQUEST_SCHEMA]
    }

    fn handle(&mut self, _msg: &Envelope, body: &[u8], cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        let req = PriceRequest::from_bytes(body)?;
        let curve = cx.pin(&self.curve)?;
        let rate = interpolate(&curve, req.tenor).map_err(holon_core::Error::Handler)?;
        let px = price(rate, req.tenor, req.notional);
        self.handled += 1;
        if self.kill_after == Some(self.handled) {
            // The crash: claim held, pin held, no reply written, no destructor.
            if let Some(p) = &self.result {
                append_line(p, &format!("KILL {}", unix_nanos()));
            }
            // SAFETY: `_exit` is always sound to call; it terminates the process
            // without running any destructor — exactly the hostile exit we want.
            unsafe { libc::_exit(137) }
        }
        Reply::of(&PriceReply {
            px,
            curve_version: curve.version(),
            incarnation: self.incarnation,
            attempt: cx.attempt(),
        })
    }
}
