// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! IntegerKey tests — append-only ledger and audit log patterns.
//! Use cases: ledger_entries, audit_log.
//!
//! In integerkey DBIs keys are stored as big-endian u64 so that
//! lexicographic order matches numeric order.

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_integerkey.monolith";

/// Converts u64 to big-endian bytes for use as an integerkey.
fn u64ToKey(n: u64) [8]u8 {
    return std.mem.toBytes(std.mem.nativeToBig(u64, n));
}

test "integerkey: insert and read by sequence" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    // Append 5 ledger entries
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ledger_entries", .{
            .create     = true,
            .integerkey = true,
        });

        var i: u64 = 1;
        while (i <= 5) : (i += 1) {
            const key = u64ToKey(i);
            const val = std.mem.asBytes(&i);
            try txn.put(dbi, &key, val, .{ .append = true });
        }
        try txn.commit();
    }

    // Verify order and count
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("ledger_entries", .{ .integerkey = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    var expected: u64 = 1;
    var kv_opt = try cur.first();
    while (kv_opt) |kv| : (kv_opt = try cur.next()) {
        // Decode the key
        const key_val = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, kv.key[0..8]));
        try std.testing.expectEqual(expected, key_val);
        expected += 1;
    }
    try std.testing.expectEqual(@as(u64, 6), expected); // 1..5 iterated
}

test "integerkey: last() returns highest sequence" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ledger", .{ .create = true, .integerkey = true });

        var i: u64 = 1;
        while (i <= 100) : (i += 1) {
            const key = u64ToKey(i);
            try txn.put(dbi, &key, "payload", .{ .append = true });
        }
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("ledger", .{ .integerkey = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    const last = try cur.last();
    try std.testing.expect(last != null);

    const last_seq = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, last.?.key[0..8]));
    try std.testing.expectEqual(@as(u64, 100), last_seq);
}
