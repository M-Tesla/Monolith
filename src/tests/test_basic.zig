//! Basic tests: open/close, put/get/del, simple transaction.

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_basic.monolith";

test "Environment: open e close" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    env.close();
}

test "Transaction: write e read simples" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    // Write txn — create DBI and insert a value
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();

        const dbi = try txn.openDbi("kv", .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.commit();
    }

    // Read txn — verify the value
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();

        const dbi = try txn.openDbi("kv", .{});
        const val = try txn.get(dbi, "hello");
        try std.testing.expect(val != null);
        try std.testing.expectEqualStrings("world", val.?);
    }
}

test "Transaction: get of nonexistent key returns null" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("kv", .{ .create = true });
        try txn.commit();
        _ = dbi;
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("kv", .{});
    const val = try txn.get(dbi, "missing_key");
    try std.testing.expect(val == null);
}

test "Transaction: del removes key" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("kv", .{ .create = true });
        try txn.put(dbi, "k1", "v1", .{});
        try txn.del(dbi, "k1", null);
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("kv", .{});
    const val = try txn.get(dbi, "k1");
    try std.testing.expect(val == null);
}

test "Cursor: iteração completa" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    // Insert 3 keys
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("kv", .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.commit();
    }

    // Iterate via cursor
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("kv", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    var count: usize = 0;
    if (try cur.first()) |_| {
        count += 1;
        while (try cur.next()) |_| count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
}
