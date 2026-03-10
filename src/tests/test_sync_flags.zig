// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Sync flags — safe_nosync and nosync skip fsync on commit.
//! env.sync() explicitly flushes regardless of the flag.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_sync_flags.monolith";

test "sync_flags: safe_nosync — data readable without explicit sync" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    {
        var env = try m.Environment.open(PATH, .{ .safe_nosync = true }, 16, 1 << 20);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ns", .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.commit(); // no fsync here
    }

    // Reopen — data must be present (OS page cache is coherent).
    {
        var env = try m.Environment.open(PATH, .{ .safe_nosync = true }, 16, 1 << 20);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("ns", .{});
        const v = try txn.get(dbi, "hello");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("world", v.?);
    }
}

test "sync_flags: nosync — data readable without explicit sync" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    {
        var env = try m.Environment.open(PATH, .{ .nosync = true }, 16, 1 << 20);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ns", .{ .create = true });
        try txn.put(dbi, "key", "val", .{});
        try txn.commit();
    }

    {
        var env = try m.Environment.open(PATH, .{ .nosync = true }, 16, 1 << 20);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("ns", .{});
        const v = try txn.get(dbi, "key");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("val", v.?);
    }
}

test "sync_flags: env.sync() flushes explicitly" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{ .safe_nosync = true }, 16, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("ns", .{ .create = true });
    try txn.put(dbi, "a", "1", .{});
    try txn.commit();

    env.sync(); // explicit flush — must not crash or error

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("ns", .{});
    const v = try rtxn.get(rdbi, "a");
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("1", v.?);
}
