//! The `Risk` actor: the second actor in the pricer process, routed by `to`
//! over the same mailbox. Owns no state either — it pins the same `curve`
//! cell and answers DV01 instead of price.

use holon_actor::{Actor, CellRef, Cx};
use holon_core::{Envelope, Payload, Reply};

use crate::curve::{interpolate, price, CURVE_KEY};
use crate::messages::{RiskReply, RiskRequest, RISK_REQUEST_SCHEMA};

/// The risk actor's service name.
pub const RISK_NAME: &str = "risk";

/// The risk actor: a [`CellRef`] to `curve` plus a handled counter.
pub struct Risk {
    curve: CellRef,
    handled: u64,
    incarnation: u32,
}

impl Default for Risk {
    fn default() -> Self {
        Risk::new()
    }
}

impl Risk {
    /// A risk actor over the `curve` cell.
    pub fn new() -> Risk {
        Risk {
            curve: CellRef::new(CURVE_KEY),
            handled: 0,
            incarnation: std::process::id(),
        }
    }

    /// Messages handled so far by this incarnation.
    pub fn handled(&self) -> u64 {
        self.handled
    }
}

impl Actor for Risk {
    fn accepts() -> &'static [u32] {
        &[RISK_REQUEST_SCHEMA]
    }

    fn handle(&mut self, _msg: &Envelope, body: &[u8], cx: &mut Cx<'_>) -> holon_core::Result<Reply> {
        let req = RiskRequest::from_bytes(body)?;
        let curve = cx.pin(&self.curve)?;
        let rate = interpolate(&curve, req.tenor).map_err(holon_core::Error::Handler)?;
        let px = price(rate, req.tenor, req.notional);
        let up = price(rate + 1e-4, req.tenor, req.notional);
        let down = price(rate - 1e-4, req.tenor, req.notional);
        self.handled += 1;
        Reply::of(&RiskReply {
            dv01: (up - down) / 2.0,
            px,
            curve_version: curve.version(),
            incarnation: self.incarnation,
            attempt: cx.attempt(),
        })
    }
}
