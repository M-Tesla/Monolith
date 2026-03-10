// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Spill List — large single transaction forces dirty pages into the cold spill
//! list, then commit writes everything correctly to the database.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_spill.monolith";

test "spill: 100k keys in one transaction, commit, reopen, verify" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    const N = 100_000;

    // ── Write phase ───────────────────────────────────────────────────────
    {
        var env = try m.Environment.open(PATH, .{}, 16, 1 << 24); // 16 MB
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();

        const dbi = try txn.openDbi("main", .{ .create = true });
        var key_buf: [16]u8 = undefined;
        var val_buf: [32]u8 = undefined;

        var i: u32 = 0;
        while (i < N) : (i += 1) {
            const k = std.fmt.bufPrint(&key_buf, "key{d:0>8}", .{i}) catch unreachable;
            const v = std.fmt.bufPrint(&val_buf, "val{d:0>8}xxxxxxxxxxxxxxxxxxxx", .{i}) catch unreachable;
            try txn.put(dbi, k, v, .{});
        }

        try txn.commit();
    }

    // ── Verify phase ──────────────────────────────────────────────────────
    {
        var env = try m.Environment.open(PATH, .{}, 16, 1 << 24);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();

        const dbi = try txn.openDbi("main", .{});
        var key_buf: [16]u8 = undefined;
        var val_buf: [32]u8 = undefined;

        var i: u32 = 0;
        while (i < N) : (i += 1) {
            const k = std.fmt.bufPrint(&key_buf, "key{d:0>8}", .{i}) catch unreachable;
            const expected = std.fmt.bufPrint(&val_buf, "val{d:0>8}xxxxxxxxxxxxxxxxxxxx", .{i}) catch unreachable;
            const got = try txn.get(dbi, k);
            try std.testing.expect(got != null);
            try std.testing.expectEqualStrings(expected, got.?);
        }
    }
}

test "spill: abort rolls back large transaction completely" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // Create a small initial DB.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 1 << 24);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("main", .{ .create = true });
        try txn.put(dbi, "sentinel", "present", .{});
        try txn.commit();
    }

    // Begin a large transaction but ABORT it.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 1 << 24);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        const dbi = try txn.openDbi("main", .{});
        var key_buf: [16]u8 = undefined;
        var i: u32 = 0;
        while (i < 50_000) : (i += 1) {
            const k = std.fmt.bufPrint(&key_buf, "tmp{d:0>8}", .{i}) catch unreachable;
            try txn.put(dbi, k, "garbage", .{});
        }
        txn.abort(); // discard everything
    }

    // Verify: only the sentinel key should exist.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 1 << 24);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("main", .{});
        const v = try txn.get(dbi, "sentinel");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("present", v.?);
        var key_buf: [16]u8 = undefined;
        const k0 = std.fmt.bufPrint(&key_buf, "tmp{d:0>8}", .{0}) catch unreachable;
        try std.testing.expect(try txn.get(dbi, k0) == null);
    }
}
