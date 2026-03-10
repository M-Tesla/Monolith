// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! txn.renew() — reuse a read-only transaction object to see a newer snapshot.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_txn_renew.monolith";

test "txn_renew: sees data written after the first read" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 16, 1 << 20);
    defer env.close();

    // Seed.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("r", .{ .create = true });
        try txn.put(dbi, "first", "v1", .{});
        try txn.commit();
    }

    // Open a read-only transaction — sees "first" but not "second".
    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    {
        const dbi = try rtxn.openDbi("r", .{});
        const v = try rtxn.get(dbi, "first");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("v1", v.?);
        try std.testing.expect(try rtxn.get(dbi, "second") == null);
    }
    rtxn.abort();

    // Write "second" in a new write transaction.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("r", .{});
        try txn.put(dbi, "second", "v2", .{});
        try txn.commit();
    }

    // Renew the read transaction — should now see "second".
    try rtxn.renew();
    defer rtxn.abort();
    {
        const dbi = try rtxn.openDbi("r", .{});
        const v = try rtxn.get(dbi, "second");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("v2", v.?);
    }
}

test "txn_renew: renew fails on write transaction" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 16, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    txn.abort();

    // renew on a write txn must return error.BadTxn.
    try std.testing.expectError(error.BadTxn, txn.renew());
}

test "txn_renew: multiple renewals converge to latest snapshot" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 16, 1 << 20);
    defer env.close();

    // Seed.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("r", .{ .create = true });
        try txn.put(dbi, "k", "0", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });

    var i: u32 = 1;
    while (i <= 5) : (i += 1) {
        // Write a new value.
        {
            var txn = try m.Transaction.begin(&env, null, .{});
            errdefer txn.abort();
            const dbi = try txn.openDbi("r", .{});
            var buf: [8]u8 = undefined;
            const val = std.fmt.bufPrint(&buf, "{d}", .{i}) catch unreachable;
            try txn.put(dbi, "k", val, .{});
            try txn.commit();
        }
        // Renew the read txn and verify it sees the latest value.
        rtxn.abort();
        try rtxn.renew();
        const dbi = try rtxn.openDbi("r", .{});
        const v = try rtxn.get(dbi, "k");
        try std.testing.expect(v != null);
        var buf: [8]u8 = undefined;
        const expected = std.fmt.bufPrint(&buf, "{d}", .{i}) catch unreachable;
        try std.testing.expectEqualStrings(expected, v.?);
    }
    rtxn.abort();
}
