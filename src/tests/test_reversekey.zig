// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! ReverseKey and IntegerKey comparators.
//! Tests that dbis opened with reversekey/integerkey flags yield correct sort order.

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_reversekey.monolith";

// ─── ReverseKey ───────────────────────────────────────────────────────────────

test "reversekey: cursor yields descending order" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rev", .{ .create = true, .reversekey = true });
        try txn.put(dbi, "apple",  "1", .{});
        try txn.put(dbi, "banana", "2", .{});
        try txn.put(dbi, "cherry", "3", .{});
        try txn.put(dbi, "date",   "4", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("rev", .{ .reversekey = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // ReverseKey: first() → largest key alphabetically = "date"
    const expected = [_][]const u8{ "date", "cherry", "banana", "apple" };
    var kv = try cur.first();
    for (expected) |exp| {
        try std.testing.expect(kv != null);
        try std.testing.expectEqualStrings(exp, kv.?.key);
        kv = try cur.next();
    }
    try std.testing.expect(kv == null);
}

test "reversekey: last() positions on smallest key" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rev2", .{ .create = true, .reversekey = true });
        try txn.put(dbi, "aaa", "1", .{});
        try txn.put(dbi, "bbb", "2", .{});
        try txn.put(dbi, "ccc", "3", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("rev2", .{ .reversekey = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // In reverse order: first="ccc", last="aaa"
    const first_kv = try cur.first();
    try std.testing.expect(first_kv != null);
    try std.testing.expectEqualStrings("ccc", first_kv.?.key);

    const last_kv = try cur.last();
    try std.testing.expect(last_kv != null);
    try std.testing.expectEqualStrings("aaa", last_kv.?.key);
}

test "reversekey: get exact key works" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rev3", .{ .create = true, .reversekey = true });
        try txn.put(dbi, "x", "val_x", .{});
        try txn.put(dbi, "y", "val_y", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("rev3", .{ .reversekey = true });

    const v = try txn.get(dbi, "x");
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("val_x", v.?);

    const missing = try txn.get(dbi, "z");
    try std.testing.expect(missing == null);
}

test "reversekey: backward iteration (prev) works" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rev4", .{ .create = true, .reversekey = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("rev4", .{ .reversekey = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // forward: c b a
    var kv = try cur.first();
    try std.testing.expectEqualStrings("c", kv.?.key);
    kv = try cur.next();
    try std.testing.expectEqualStrings("b", kv.?.key);
    kv = try cur.next();
    try std.testing.expectEqualStrings("a", kv.?.key);

    // backward: b c
    kv = try cur.prev();
    try std.testing.expectEqualStrings("b", kv.?.key);
    kv = try cur.prev();
    try std.testing.expectEqualStrings("c", kv.?.key);
    kv = try cur.prev();
    try std.testing.expect(kv == null);
}

// ─── IntegerKey ───────────────────────────────────────────────────────────────

test "integerkey: u32 keys sorted numerically" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ikey", .{ .create = true, .integerkey = true });
        // Insert in non-sorted order
        var k: [4]u8 = undefined;
        std.mem.writeInt(u32, &k, 300, .little);
        try txn.put(dbi, &k, "three-hundred", .{});
        std.mem.writeInt(u32, &k, 1, .little);
        try txn.put(dbi, &k, "one", .{});
        std.mem.writeInt(u32, &k, 50, .little);
        try txn.put(dbi, &k, "fifty", .{});
        std.mem.writeInt(u32, &k, 10, .little);
        try txn.put(dbi, &k, "ten", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("ikey", .{ .integerkey = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Expected order: 1, 10, 50, 300
    const expected_vals = [_][]const u8{ "one", "ten", "fifty", "three-hundred" };
    const expected_ints = [_]u32{ 1, 10, 50, 300 };
    var kv = try cur.first();
    for (expected_vals, expected_ints) |ev, ei| {
        try std.testing.expect(kv != null);
        try std.testing.expectEqualStrings(ev, kv.?.val);
        const got_int = std.mem.readInt(u32, kv.?.key[0..4], .little);
        try std.testing.expectEqual(ei, got_int);
        kv = try cur.next();
    }
    try std.testing.expect(kv == null);
}

test "integerkey: get exact u32 key" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ikey2", .{ .create = true, .integerkey = true });
        var k: [4]u8 = undefined;
        std.mem.writeInt(u32, &k, 42, .little);
        try txn.put(dbi, &k, "forty-two", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("ikey2", .{ .integerkey = true });

    var k: [4]u8 = undefined;
    std.mem.writeInt(u32, &k, 42, .little);
    const v = try txn.get(dbi, &k);
    try std.testing.expect(v != null);
    try std.testing.expectEqualStrings("forty-two", v.?);

    std.mem.writeInt(u32, &k, 99, .little);
    const miss = try txn.get(dbi, &k);
    try std.testing.expect(miss == null);
}
