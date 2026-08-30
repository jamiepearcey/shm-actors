//! The POD messages: `PriceRequest`/`PriceReply` for the `pricer` actor and
//! `RiskRequest`/`RiskReply` for the `risk` actor hosted in the same process.

use holon_core::Payload;
use shm_core::SharedPod;

/// Schema id of [`PriceRequest`].
pub const PRICE_REQUEST_SCHEMA: u32 = 1001;
/// Schema id of [`PriceReply`].
pub const PRICE_REPLY_SCHEMA: u32 = 1002;

/// Price `notional` at `tenor` years off the current curve.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PriceRequest {
    /// Tenor in years.
    pub tenor: f64,
    /// Notional to discount.
    pub notional: f64,
    /// Client sequence number (echoed nowhere; for the client's own bookkeeping).
    pub seq: u64,
}

// SAFETY: `#[repr(C)]`, three 8-byte scalars, no padding, no pointers, no Drop.
unsafe impl SharedPod for PriceRequest {}
impl Payload for PriceRequest {
    const SCHEMA_ID: u32 = PRICE_REQUEST_SCHEMA;
}

/// The discounted price and where it came from.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PriceReply {
    /// The discounted price.
    pub px: f64,
    /// The curve cell version the price was computed against.
    pub curve_version: u64,
    /// The pricer process's pid — which incarnation answered.
    pub incarnation: u32,
    /// How many times the ask was redelivered before this reply (`0` = first delivery).
    pub attempt: u32,
}

// SAFETY: `#[repr(C)]`, 8+8+4+4 bytes, no padding, no pointers, no Drop.
unsafe impl SharedPod for PriceReply {}
impl Payload for PriceReply {
    const SCHEMA_ID: u32 = PRICE_REPLY_SCHEMA;
}

/// Schema id of [`RiskRequest`].
pub const RISK_REQUEST_SCHEMA: u32 = 1003;
/// Schema id of [`RiskReply`].
pub const RISK_REPLY_SCHEMA: u32 = 1004;

/// DV01 of `notional` at `tenor` years off the current curve (the `risk` actor).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct RiskRequest {
    /// Tenor in years.
    pub tenor: f64,
    /// Notional.
    pub notional: f64,
    /// Client sequence number.
    pub seq: u64,
}

// SAFETY: `#[repr(C)]`, three 8-byte scalars, no padding, no pointers, no Drop.
unsafe impl SharedPod for RiskRequest {}
impl Payload for RiskRequest {
    const SCHEMA_ID: u32 = RISK_REQUEST_SCHEMA;
}

/// DV01 (central ±1 bp bump of the interpolated rate) and the price it was
/// bumped around.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct RiskReply {
    /// Price change per +1 bp of rate (negative for a discount).
    pub dv01: f64,
    /// The unbumped price.
    pub px: f64,
    /// The curve cell version used.
    pub curve_version: u64,
    /// The hosting process's pid.
    pub incarnation: u32,
    /// Redelivery count.
    pub attempt: u32,
}

// SAFETY: `#[repr(C)]`, 8+8+8+4+4 bytes, no padding, no pointers, no Drop.
unsafe impl SharedPod for RiskReply {}
impl Payload for RiskReply {
    const SCHEMA_ID: u32 = RISK_REPLY_SCHEMA;
}

/// The DV01 the `risk` actor reports for an unbumped price `px` at `tenor`:
/// `P = N·e^{−rt}` bumped ±1 bp and central-differenced is exactly
/// `−P·sinh(t·1e−4)`. Clients verify replies against this.
pub fn expected_dv01(px: f64, tenor: f64) -> f64 {
    -px * (tenor * 1e-4).sinh()
}

const _: () = assert!(core::mem::size_of::<PriceRequest>() == 24);
const _: () = assert!(core::mem::size_of::<RiskRequest>() == 24);
const _: () = assert!(core::mem::size_of::<RiskReply>() == 32);
const _: () = assert!(core::mem::size_of::<PriceReply>() == 24);
