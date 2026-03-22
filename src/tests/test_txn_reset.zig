// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! txn.reset() and reset+renew cycle.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_txn_reset.monolith";

test "txn_reset: reset+renew sees latest snapshot" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    // Seed.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("r", .{ .create = true });
        try txn.put(dbi, "k", "v1", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });

    // Verify initial state.
    {
        const dbi = try rtxn.openDbi("r", .{});
        const v = try rtxn.get(dbi, "k");
        try std.testing.expectEqualStrings("v1", v.?);
    }

    // Reset releases the reader slot.
    rtxn.reset();

    // Write a new value.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("r", .{});
        try txn.put(dbi, "k", "v2", .{});
        try txn.commit();
    }

    // Renew after reset — sees the new value.
    try rtxn.renew();
    defer rtxn.abort();
    {
        const dbi = try rtxn.openDbi("r", .{});
        const v = try rtxn.get(dbi, "k");
        try std.testing.expectEqualStrings("v2", v.?);
    }
}

test "txn_reset: double reset is safe" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    rtxn.reset();
    rtxn.reset(); // second reset must be a no-op
    // After double reset, renew must still work.
    try rtxn.renew();
    rtxn.abort();
}

test "txn_reset: reset on write txn is a no-op" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    // reset() on a write txn must silently do nothing (guard: if (!self.rdonly) return).
    txn.reset();
    // The txn must still be usable.
    const dbi = try txn.openDbi("wr", .{ .create = true });
    try txn.put(dbi, "k", "v", .{});
    try txn.commit();
}

test "txn_reset: reset+renew loop" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("loop", .{ .create = true });
        try txn.put(dbi, "key", "val", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });

    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        rtxn.reset();
        try rtxn.renew();
        const dbi = try rtxn.openDbi("loop", .{});
        const v = try rtxn.get(dbi, "key");
        try std.testing.expect(v != null);
    }
    rtxn.abort();
}
