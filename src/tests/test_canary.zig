// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Canary API + user-context pointer.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_canary.monolith";

test "canary: put and get within same transaction" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    try txn.putCanary(1, 2, 3, 4);
    const c = txn.getCanary();
    try std.testing.expectEqual(@as(u64, 1), c.x);
    try std.testing.expectEqual(@as(u64, 2), c.y);
    try std.testing.expectEqual(@as(u64, 3), c.z);
    try std.testing.expectEqual(@as(u64, 4), c.v);
    try txn.commit();
}

test "canary: survives commit and reopen" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    {
        var env = try m.Environment.open(PATH, .{}, 4, 4 * 1024 * 1024);
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.putCanary(0xDEAD, 0xBEEF, 0xCAFE, 0xBABE);
        try txn.commit();
        env.close();
    }

    var env2 = try m.Environment.open(PATH, .{}, 4, 4 * 1024 * 1024);
    defer env2.close();
    var txn2 = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
    defer txn2.abort();
    const c = txn2.getCanary();
    try std.testing.expectEqual(@as(u64, 0xDEAD), c.x);
    try std.testing.expectEqual(@as(u64, 0xBEEF), c.y);
    try std.testing.expectEqual(@as(u64, 0xCAFE), c.z);
    try std.testing.expectEqual(@as(u64, 0xBABE), c.v);
}

test "canary: read-only transaction cannot putCanary" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    try std.testing.expectError(error.BadTxn, txn.putCanary(1, 2, 3, 4));
}

test "user-ctx: round-trip" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var sentinel: u64 = 0xABCDEF01;
    env.setUserCtx(@ptrCast(&sentinel));

    const got = env.getUserCtx();
    try std.testing.expect(got != null);
    const val = @as(*u64, @ptrCast(@alignCast(got.?))).*;
    try std.testing.expectEqual(@as(u64, 0xABCDEF01), val);
}
