//! WRITEMAP mode — dirty pages written directly into the mmap instead of
//! heap-allocated buffers. Abort must revert in-place mmap writes.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_writemap.monolith";

test "writemap: basic CRUD survives commit and reopen" {
    // Clean up any leftover files from a previous crashed run.
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    {
        var env = try m.Environment.open(PATH, .{ .writemap = true }, 16, 1 << 20);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("wm", .{ .create = true });

        try txn.put(dbi, "alpha", "AAA", .{});
        try txn.put(dbi, "beta",  "BBB", .{});
        try txn.put(dbi, "gamma", "CCC", .{});
        try txn.commit();
    }

    {
        var env = try m.Environment.open(PATH, .{ .writemap = true }, 16, 1 << 20);
        defer env.close();

        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("wm", .{});

        const a = try txn.get(dbi, "alpha");
        try std.testing.expect(a != null);
        try std.testing.expectEqualStrings("AAA", a.?);

        const b = try txn.get(dbi, "beta");
        try std.testing.expect(b != null);
        try std.testing.expectEqualStrings("BBB", b.?);

        const c = try txn.get(dbi, "gamma");
        try std.testing.expect(c != null);
        try std.testing.expectEqualStrings("CCC", c.?);
    }
}

test "writemap: abort reverts writes" {
    // Clean up any leftover files from a previous crashed run.
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // Seed the DB.
    {
        var env = try m.Environment.open(PATH, .{ .writemap = true }, 16, 1 << 20);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("wm", .{ .create = true });
        try txn.put(dbi, "keep", "original", .{});
        try txn.commit();
    }

    // Overwrite in a transaction and abort.
    {
        var env = try m.Environment.open(PATH, .{ .writemap = true }, 16, 1 << 20);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        const dbi = try txn.openDbi("wm", .{});
        try txn.put(dbi, "keep", "modified", .{});
        try txn.put(dbi, "gone", "transient", .{});
        txn.abort();

        // Read within the same env — must see original state.
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const rdbi = try rtxn.openDbi("wm", .{});
        const v = try rtxn.get(rdbi, "keep");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("original", v.?);
        try std.testing.expect(try rtxn.get(rdbi, "gone") == null);
    }
}

test "writemap: large write forces mmap growth" {
    // Clean up any leftover files from a previous crashed run.
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    const N = 5_000;
    {
        var env = try m.Environment.open(PATH, .{ .writemap = true }, 16, 1 << 20);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("wm", .{ .create = true });
        var buf: [20]u8 = undefined;
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{d:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "value", .{});
        }
        try txn.commit();
    }

    {
        var env = try m.Environment.open(PATH, .{ .writemap = true }, 16, 1 << 20);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("wm", .{});
        var buf: [20]u8 = undefined;
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{d:0>6}", .{i}) catch unreachable;
            try std.testing.expect(try txn.get(dbi, k) != null);
        }
    }
}
