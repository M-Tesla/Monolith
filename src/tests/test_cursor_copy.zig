// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! cursor.copy() — independent cursor navigation.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_cursor_copy.monolith";

test "cursor_copy: copied cursor navigates independently" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("cc", .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const dbi = try rtxn.openDbi("cc", .{});
    var cur = try rtxn.cursor(dbi);
    defer cur.close();

    // Position original on "b".
    _ = try cur.seekRange("b");
    const kv_orig = try cur.current();
    try std.testing.expect(kv_orig != null);
    try std.testing.expectEqualStrings("b", kv_orig.?.key);

    // Copy and advance the copy to "c".
    var copy = cur.copy();
    _ = try copy.next();
    const kv_copy = try copy.current();
    try std.testing.expect(kv_copy != null);
    try std.testing.expectEqualStrings("c", kv_copy.?.key);

    // Original is still on "b".
    const kv_orig2 = try cur.current();
    try std.testing.expect(kv_orig2 != null);
    try std.testing.expectEqualStrings("b", kv_orig2.?.key);
}

test "cursor_copy: copy of invalid cursor stays invalid" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("cc2", .{ .create = true });
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const dbi = try rtxn.openDbi("cc2", .{});
    var cur = try rtxn.cursor(dbi);
    defer cur.close();

    // Empty DB → cursor is invalid.
    var copy = cur.copy();
    try std.testing.expect(try copy.current() == null);
}
