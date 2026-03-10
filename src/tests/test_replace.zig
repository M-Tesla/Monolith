// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! txn.replace(), txn.putMultiple(), cursor.put(current).

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_replace.monolith";

// ─── txn.replace ─────────────────────────────────────────────────────────────

test "replace: returns old value and writes new" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        try txn.put(dbi, "key", "old_value", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("t", .{});

    var old: ?[]const u8 = null;
    try txn.replace(dbi, "key", "new_value", &old);
    try std.testing.expect(old != null);
    try std.testing.expectEqualStrings("old_value", old.?);

    const got = try txn.get(dbi, "key");
    try std.testing.expectEqualStrings("new_value", got.?);
    try txn.commit();
}

test "replace: missing key sets old to null and inserts new" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("t", .{ .create = true });

    var old: ?[]const u8 = &.{};
    try txn.replace(dbi, "new_key", "value", &old);
    try std.testing.expect(old == null);
    try std.testing.expectEqualStrings("value", (try txn.get(dbi, "new_key")).?);
    try txn.commit();
}

// ─── txn.putMultiple ─────────────────────────────────────────────────────────

test "putMultiple: inserts several dup values in one call" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    const vals = [_][]const u8{ "alpha", "beta", "gamma" };

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("d", .{ .create = true, .dupsort = true });
        try txn.putMultiple(dbi, "k", &vals, .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("d", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();
    try std.testing.expect(try cur.find("k"));
    try std.testing.expectEqual(@as(usize, 3), try cur.countDups());
}

// ─── cursor.put(current) ─────────────────────────────────────────────────────

test "cursor.put(current): overwrites value at cursor position" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        try txn.put(dbi, "a", "old_a", .{});
        try txn.put(dbi, "b", "old_b", .{});
        try txn.put(dbi, "c", "old_c", .{});
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();

        // Position on "b".
        try std.testing.expect(try cur.find("b"));
        // Overwrite with new value.
        try cur.put("b", "new_b", .{ .current = true });
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("t", .{});
    try std.testing.expectEqualStrings("old_a", (try txn.get(dbi, "a")).?);
    try std.testing.expectEqualStrings("new_b", (try txn.get(dbi, "b")).?);
    try std.testing.expectEqualStrings("old_c", (try txn.get(dbi, "c")).?);
}
