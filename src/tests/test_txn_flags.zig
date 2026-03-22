// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! TxnFlags.try_begin and TxnFlags.nosync.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_txn_flags.monolith";

test "try_begin: succeeds when no writer holds the lock" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{ .try_begin = true });
    defer txn.abort();

    const dbi = try txn.openDbi("tf", .{ .create = true });
    try txn.put(dbi, "k", "v", .{});
    try txn.commit();
}

test "try_begin: returns Busy when write lock already held" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    // Hold the write lock with txn1.
    var txn1 = try m.Transaction.begin(&env, null, .{});
    defer txn1.abort();

    // try_begin must return error.Busy.
    try std.testing.expectError(error.Busy, m.Transaction.begin(&env, null, .{ .try_begin = true }));
}

test "nosync: commit succeeds without fsync" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{ .nosync = true });
    errdefer txn.abort();
    const dbi = try txn.openDbi("ns", .{ .create = true });
    try txn.put(dbi, "key", "val", .{});
    try txn.commit();

    // Data must be readable in the next transaction.
    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const dbi2 = try rtxn.openDbi("ns", .{});
    const v = try rtxn.get(dbi2, "key");
    try std.testing.expectEqualStrings("val", v.?);
}

test "nosync: can coexist with env safe_nosync" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{ .safe_nosync = true }, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{ .nosync = true });
    errdefer txn.abort();
    const dbi = try txn.openDbi("x", .{ .create = true });
    try txn.put(dbi, "a", "b", .{});
    try txn.commit();
}
