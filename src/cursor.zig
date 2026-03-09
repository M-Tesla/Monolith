//! Cursor re-export.
//! The Cursor type lives in txn.zig to avoid circular imports.
pub const Cursor = @import("txn.zig").Cursor;
