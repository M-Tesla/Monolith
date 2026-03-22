// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! cursor.seekRange() — exact vs approximate positioning.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_seek_range.monolith";

test "seekRange: exact match sets exact=true" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("sr", .{ .create = true });
        try txn.put(dbi, "apple", "1", .{});
        try txn.put(dbi, "banana", "2", .{});
        try txn.put(dbi, "cherry", "3", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const dbi = try rtxn.openDbi("sr", .{});
    var cur = try rtxn.cursor(dbi);
    defer cur.close();

    const res = try cur.seekRange("banana");
    try std.testing.expect(res != null);
    try std.testing.expect(res.?.exact);
    try std.testing.expectEqualStrings("banana", res.?.kv.key);
    try std.testing.expectEqualStrings("2", res.?.kv.val);
}

test "seekRange: approximate match sets exact=false" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("sr", .{ .create = true });
        try txn.put(dbi, "apple", "1", .{});
        try txn.put(dbi, "cherry", "3", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const dbi = try rtxn.openDbi("sr", .{});
    var cur = try rtxn.cursor(dbi);
    defer cur.close();

    // "banana" doesn't exist; seekRange positions on "cherry" (next key)
    const res = try cur.seekRange("banana");
    try std.testing.expect(res != null);
    try std.testing.expect(!res.?.exact);
    try std.testing.expectEqualStrings("cherry", res.?.kv.key);
}

test "seekRange: key not found returns null" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("sr", .{ .create = true });
        try txn.put(dbi, "apple", "1", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const dbi = try rtxn.openDbi("sr", .{});
    var cur = try rtxn.cursor(dbi);
    defer cur.close();

    // "zzz" is past all keys
    const res = try cur.seekRange("zzz");
    try std.testing.expect(res == null);
}

test "seekRange: SeekResult fields accessible via lib re-export" {
    // Verify the type is re-exported from lib.zig
    const T = m.SeekResult;
    _ = T;
}
