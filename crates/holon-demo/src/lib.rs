//! `holon-demo` — the ADR-0015 demo: one message, one mailbox, one handler,
//! one pinned cell, across processes, under `kill -9`, with measured numbers.
//!
//! The binary (`src/main.rs`) exposes the roles; this library holds what the
//! roles, the bench and the integration tests share: the curve cell, the POD
//! messages, the `Pricer` and `Risk` actors (both hosted by every `pricer`
//! process, routed by `to` over the one mailbox), and the multi-process
//! orchestration.
//!
//! ```text
//! holon-demo coordinator   --uds <p> [--seg-base <n>]
//! holon-demo curve-publish --uds <p> [--bump-bp <f>]        # re-run = market move (v2, v3, …)
//! holon-demo pricer        --uds <p> [--kill-after <n>] [--result <f>] [--spin] [--bare]
//!                          [--lease-ms <n>] [--parent-watch]
//! holon-demo client        --uds <p> --n <N> [--clients <k>] [--result <f>] [--spin] [--bare] [--mix]
//! holon-demo supervisor    --uds <p> [--kill-after <n>] [--result <f>] [--pricer-result <f>] [--spin]
//! holon-demo bench         [--n <N>] [--runs <r>] [--crash-n <N>] [--kill-after <n>]
//! ```

#![deny(missing_docs)]

pub mod curve;
pub mod messages;
pub mod orchestrate;
pub mod pricer;
pub mod risk;
pub mod roles;

pub use curve::{curve_batch, curve_schema, interpolate, price, CURVE_KEY, CURVE_TENORS};
pub use messages::{
    expected_dv01, PriceReply, PriceRequest, RiskReply, RiskRequest, PRICE_REPLY_SCHEMA,
    PRICE_REQUEST_SCHEMA, RISK_REPLY_SCHEMA, RISK_REQUEST_SCHEMA,
};
pub use pricer::{Pricer, PRICER_NAME};
pub use risk::{Risk, RISK_NAME};
