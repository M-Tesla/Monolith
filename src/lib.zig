// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Monolith — embedded B-tree storage engine.
//!
//! Basic usage:
//!   const monolith = @import("monolith");
//!
//!   var env = try monolith.Environment.open("/data/db.monolith", .{}, 16, 1 << 30);
//!   defer env.close();
//!
//!   var txn = try monolith.Transaction.begin(&env, null, .{});
//!   defer txn.abort();
//!   const dbi = try txn.openDbi("users", .{ .create = true });
//!   try txn.put(dbi, key_bytes, val_bytes, .{});
//!   try txn.commit();

pub const Environment = @import("env.zig").Environment;
pub const Transaction = @import("txn.zig").Transaction;
pub const Cursor      = @import("cursor.zig").Cursor;
pub const types       = @import("types.zig");
pub const limits      = @import("limits.zig");

// Convenience re-exports (avoids writing `monolith.types.EnvFlags`)
pub const EnvFlags  = types.EnvFlags;
pub const DbFlags   = types.DbFlags;
pub const TxnFlags  = types.TxnFlags;
pub const PutFlags  = types.PutFlags;
pub const Dbi       = types.Dbi;
pub const KV        = types.KV;
pub const Error     = types.Error;
pub const Stat      = types.Stat;
pub const EnvStat   = types.EnvStat;
pub const Geometry  = types.Geometry;
pub const CheckResult = Environment.CheckResult;

// -------------------------------------------------------------------------
// Integration tests
// -------------------------------------------------------------------------
test {
    _ = @import("tests/test_basic.zig");
    _ = @import("tests/test_dupsort.zig");
    _ = @import("tests/test_integerkey.zig");
    _ = @import("tests/test_multi_dbi.zig");
    _ = @import("tests/test_splits_drop.zig");
    _ = @import("tests/test_nested.zig");
    _ = @import("tests/test_advanced_put.zig");
    _ = @import("tests/test_utilities.zig");
    _ = @import("tests/test_advanced_cursors.zig");
    _ = @import("tests/test_overflow.zig");
    _ = @import("tests/test_cursor_prev.zig");
    _ = @import("tests/test_reversekey.zig");
    _ = @import("tests/test_mvcc_gc.zig");
    _ = @import("tests/test_flags_persist.zig");
    _ = @import("tests/test_checksums.zig");
    _ = @import("tests/test_limits.zig");
    _ = @import("tests/test_canary.zig");
    _ = @import("tests/test_dupsort_positioned.zig");
    _ = @import("tests/test_replace.zig");
    _ = @import("tests/test_compact.zig");
    _ = @import("tests/test_spill.zig");
    _ = @import("tests/test_writemap.zig");
    _ = @import("tests/test_sync_flags.zig");
    _ = @import("tests/test_txn_renew.zig");
    _ = @import("tests/test_close_dbi.zig");
    _ = @import("tests/test_geometry.zig");
    _ = @import("tests/test_shrink.zig");
    _ = @import("tests/test_liforeclaim.zig");
    _ = @import("tests/test_exclusive.zig");
    _ = @import("tests/test_coalesce.zig");
    _ = @import("tests/test_sorted_flush.zig");
    _ = @import("tests/test_accede.zig");
}
