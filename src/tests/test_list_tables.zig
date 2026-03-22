// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! txn.listTables() — enumerate named DBIs.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_list_tables.monolith";

test "listTables: empty environment returns zero tables" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const alloc = std.testing.allocator;
    const names = try txn.listTables(alloc);
    defer {
        for (names) |n| alloc.free(n);
        alloc.free(names);
    }
    try std.testing.expectEqual(@as(usize, 0), names.len);
}

test "listTables: returns all created tables" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("alpha",   .{ .create = true });
        _ = try txn.openDbi("beta",    .{ .create = true });
        _ = try txn.openDbi("gamma",   .{ .create = true });
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();

    const alloc = std.testing.allocator;
    const names = try rtxn.listTables(alloc);
    defer {
        for (names) |n| alloc.free(n);
        alloc.free(names);
    }

    try std.testing.expectEqual(@as(usize, 3), names.len);
    // Order is lexicographic (B-tree order).
    try std.testing.expectEqualStrings("alpha", names[0]);
    try std.testing.expectEqualStrings("beta",  names[1]);
    try std.testing.expectEqualStrings("gamma", names[2]);
}

test "listTables: drops are not listed" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("keep",   .{ .create = true });
        _ = try txn.openDbi("remove", .{ .create = true });
        try txn.commit();
    }

    // Drop "remove".
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.dropDbi("remove", true);
        try txn.commit();
    }

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();

    const alloc = std.testing.allocator;
    const names = try rtxn.listTables(alloc);
    defer {
        for (names) |n| alloc.free(n);
        alloc.free(names);
    }

    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("keep", names[0]);
}
