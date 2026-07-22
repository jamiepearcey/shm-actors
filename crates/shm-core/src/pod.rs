//! POD discipline for types that may live in shared memory.

/// Marker trait for types that are safe to place in a cross-process shared
/// mapping.
///
/// # Safety
///
/// A `SharedPod` type must be *plain old data* in the strictest cross-process
/// sense:
///
/// - `#[repr(C)]` with a fixed, documented layout (no `repr(Rust)` reordering).
/// - No pointers or references — an address is only meaningful in the process
///   that produced it. Cross-process references are expressed as
///   [`ChunkDesc`](crate::desc::ChunkDesc)s or byte offsets, never raw pointers.
/// - No `Drop` — the memory is owned by the arena, not by any process's stack.
/// - No vtables / trait objects — nothing that embeds a code pointer.
/// - Every cross-process *control word* (anything mutated concurrently by more
///   than one process) is an `Atomic*`, accessed with explicit ordering.
///   `SharedPod` covers the immutable/single-writer payload records;
///   coordination words like [`ChunkCtrl`](crate::ctrl::ChunkCtrl) deliberately
///   contain atomics and are therefore *not* `Pod`/`SharedPod` — they are placed
///   in shared memory by hand.
///
/// Because these invariants cannot be fully checked by the compiler, the trait
/// is `unsafe`. There is intentionally **no blanket impl**: each type opts in
/// explicitly.
///
/// # Implementing for your own type
///
/// Downstream crates add their own POD records with a plain `unsafe impl` — no
/// derive macro is required:
///
/// ```
/// use shm_core::SharedPod;
///
/// #[repr(C)]
/// #[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
/// struct MyRecord {
///     a: u64,
///     b: u32,
///     _pad: u32,
/// }
///
/// // SAFETY: `#[repr(C)]`, no padding, no pointers, no Drop, `Pod`.
/// unsafe impl SharedPod for MyRecord {}
/// ```
pub unsafe trait SharedPod: bytemuck::Pod {}

macro_rules! impl_shared_pod_scalar {
    ($($t:ty),* $(,)?) => {
        $(
            // SAFETY: fixed-width scalar with a well-defined, arch-independent
            // byte representation; no pointers, no Drop, `Pod`.
            unsafe impl SharedPod for $t {}
        )*
    };
}

// Fixed-width scalars are legitimate shared POD. `usize`/`isize` are omitted:
// their width is arch-dependent and must not cross a process boundary.
impl_shared_pod_scalar!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);
