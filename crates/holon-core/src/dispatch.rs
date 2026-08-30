//! Zero-`dyn` dispatch: a dense `schema_id → handler` table built once at
//! spawn, looked up with a bounds-checked search and an indirect call — no
//! `Box<dyn Any>`, no downcast, no vtable on the hot path (design §5).
//!
//! This is the runtime shape the future `#[derive(Actor)]` macro targets: the
//! macro will emit the same table (or a `match` over the registered ids) at
//! compile time; today [`Dispatch::register`] fills it by hand.

use crate::envelope::Envelope;
use crate::error::Result;
use crate::payload::Reply;

/// A message handler: plain `fn` pointer, no captures.
///
/// `A` is the actor state, `C` the per-message context the runtime lends the
/// handler (the actor layer's `Cx`).
pub type Handler<A, C> = fn(&mut A, &Envelope, &[u8], &mut C) -> Result<Reply>;

/// A dense dispatch table for one actor type.
///
/// Registration assigns each schema id a small dense index into `table`; the
/// sorted `ids` vector maps a schema id back to that index by binary search
/// (a handful of compares for any realistic handler count, no hashing, one
/// cache line of ids). Lookup is therefore a bounds check plus an indirect
/// call.
pub struct Dispatch<A, C> {
    /// Sorted schema ids; `ids[i]`'s handler is `table[i]`.
    ids: Vec<u32>,
    /// Handlers, parallel to `ids`.
    table: Vec<Handler<A, C>>,
}

impl<A, C> Default for Dispatch<A, C> {
    fn default() -> Self {
        Dispatch::new()
    }
}

impl<A, C> Dispatch<A, C> {
    /// An empty table.
    pub fn new() -> Dispatch<A, C> {
        Dispatch {
            ids: Vec::new(),
            table: Vec::new(),
        }
    }

    /// Register `handler` for `schema_id`, replacing any previous handler.
    pub fn register(&mut self, schema_id: u32, handler: Handler<A, C>) -> &mut Self {
        match self.ids.binary_search(&schema_id) {
            Ok(i) => self.table[i] = handler,
            Err(i) => {
                self.ids.insert(i, schema_id);
                self.table.insert(i, handler);
            }
        }
        self
    }

    /// Builder form of [`register`](Self::register).
    pub fn on(mut self, schema_id: u32, handler: Handler<A, C>) -> Self {
        self.register(schema_id, handler);
        self
    }

    /// The handler for `schema_id`, if registered.
    #[inline]
    pub fn lookup(&self, schema_id: u32) -> Option<Handler<A, C>> {
        self.ids
            .binary_search(&schema_id)
            .ok()
            .map(|i| self.table[i])
    }

    /// Number of registered handlers.
    #[inline]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether no handler is registered.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// The registered schema ids, ascending.
    #[inline]
    pub fn schema_ids(&self) -> &[u32] {
        &self.ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{ActorId, MessageKind};

    struct Counter(u32);

    fn inc(a: &mut Counter, _e: &Envelope, _b: &[u8], _c: &mut ()) -> Result<Reply> {
        a.0 += 1;
        Ok(Reply::None)
    }
    fn dec(a: &mut Counter, _e: &Envelope, _b: &[u8], _c: &mut ()) -> Result<Reply> {
        a.0 -= 1;
        Ok(Reply::None)
    }

    #[test]
    fn register_lookup_replace() {
        let mut d: Dispatch<Counter, ()> = Dispatch::new();
        assert!(d.is_empty());
        d.register(30, inc).register(10, inc).register(20, dec);
        assert_eq!(d.len(), 3);
        assert_eq!(d.schema_ids(), &[10, 20, 30]);
        assert!(d.lookup(40).is_none());

        let e = Envelope::inline(MessageKind::Tell, ActorId(1), ActorId(2), 0, 10, 0);
        let mut a = Counter(5);
        d.lookup(10).unwrap()(&mut a, &e, &[], &mut ()).unwrap();
        d.lookup(30).unwrap()(&mut a, &e, &[], &mut ()).unwrap();
        d.lookup(20).unwrap()(&mut a, &e, &[], &mut ()).unwrap();
        assert_eq!(a.0, 6);

        // Re-registering replaces in place.
        d.register(20, inc);
        assert_eq!(d.len(), 3);
        d.lookup(20).unwrap()(&mut a, &e, &[], &mut ()).unwrap();
        assert_eq!(a.0, 7);

        let d2 = Dispatch::<Counter, ()>::new().on(1, inc);
        assert_eq!(d2.len(), 1);
    }
}
