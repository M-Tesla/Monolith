/// Tests for exclusive mode — no .lck file, single-process use (Fase 33).
const std = @import("std");
const m = @import("../lib.zig");

const PATH: [:0]const u8 = "test_exclusive.monolith";

fn cleanup() void {
    std.fs.cwd().deleteFile(PATH) catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
}

test "exclusive: opens without creating a .lck file" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{ .exclusive = true }, 16, 4 * 1024 * 1024);
    defer env.close();

    // The .lck file must not exist.
    const lck_stat = std.fs.cwd().statFile(PATH ++ "-lck") catch null;
    try std.testing.expect(lck_stat == null);
}

test "exclusive: read and write transactions work correctly" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{ .exclusive = true }, 16, 4 * 1024 * 1024);
    defer env.close();

    // Write some data.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("ex", .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.put(dbi, "foo",   "bar",   .{});
        try txn.commit();
    }

    // Read it back.
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("ex", .{});
        const v1 = try txn.get(dbi, "hello");
        try std.testing.expect(v1 != null);
        try std.testing.expectEqualStrings("world", v1.?);
        const v2 = try txn.get(dbi, "foo");
        try std.testing.expect(v2 != null);
        try std.testing.expectEqualStrings("bar", v2.?);
    }
}

test "exclusive: multiple transactions work without lock contention" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{ .exclusive = true }, 16, 8 * 1024 * 1024);
    defer env.close();

    // 5 sequential write transactions, each doing 20 inserts.
    var round: u32 = 0;
    while (round < 5) : (round += 1) {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        var buf: [16]u8 = undefined;
        var i: u32 = 0;
        while (i < 20) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "r{d}k{:0>4}", .{ round, i }) catch unreachable;
            try txn.put(dbi, k, "v", .{});
        }
        try txn.commit();
    }

    // Read them all back.
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("data", .{});
    var buf: [16]u8 = undefined;
    var r: u32 = 0;
    while (r < 5) : (r += 1) {
        var i: u32 = 0;
        while (i < 20) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "r{d}k{:0>4}", .{ r, i }) catch unreachable;
            const v = try txn.get(dbi, k);
            try std.testing.expect(v != null);
            try std.testing.expectEqualStrings("v", v.?);
        }
    }
}
