/// Tests for liforeclaim GC mode (Fase 32).
const std = @import("std");
const m = @import("../lib.zig");

const PATH: [:0]const u8 = "test_liforeclaim.monolith";

fn cleanup() void {
    std.fs.cwd().deleteFile(PATH) catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
}

test "liforeclaim: pages are reclaimed and data is consistent" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{ .liforeclaim = true }, 16, 8 * 1024 * 1024);
    defer env.close();

    // Insert batch A.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tbl", .{ .create = true });
        var buf: [16]u8 = undefined;
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "a{:0>5}", .{i}) catch unreachable;
            try txn.put(dbi, k, "alpha", .{});
        }
        try txn.commit();
    }

    // Delete batch A (creates GC entries).
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tbl", .{});
        var buf: [16]u8 = undefined;
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "a{:0>5}", .{i}) catch unreachable;
            txn.del(dbi, k, null) catch |err| {
                if (err == error.NotFound) continue;
                return err;
            };
        }
        try txn.commit();
    }

    // Insert batch B — triggers LIFO GC reclaim of batch A's pages.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tbl", .{});
        var buf: [16]u8 = undefined;
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "b{:0>5}", .{i}) catch unreachable;
            try txn.put(dbi, k, "beta", .{});
        }
        try txn.commit();
    }

    // Read batch B back — must be intact.
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("tbl", .{});
        var buf: [16]u8 = undefined;
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "b{:0>5}", .{i}) catch unreachable;
            const v = try txn.get(dbi, k);
            try std.testing.expect(v != null);
            try std.testing.expectEqualStrings("beta", v.?);
        }
    }
}

test "liforeclaim: multiple GC rounds produce consistent state" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{ .liforeclaim = true }, 16, 8 * 1024 * 1024);
    defer env.close();

    // Three write-delete-rewrite cycles.
    var round: u32 = 0;
    while (round < 3) : (round += 1) {
        // Write.
        {
            var txn = try m.Transaction.begin(&env, null, .{});
            errdefer txn.abort();
            const dbi = try txn.openDbi("rnd", .{ .create = true });
            var buf: [16]u8 = undefined;
            var i: u32 = 0;
            while (i < 50) : (i += 1) {
                const k = std.fmt.bufPrint(&buf, "r{d}k{:0>4}", .{ round, i }) catch unreachable;
                try txn.put(dbi, k, "data", .{});
            }
            try txn.commit();
        }
        // Delete.
        {
            var txn = try m.Transaction.begin(&env, null, .{});
            errdefer txn.abort();
            const dbi = try txn.openDbi("rnd", .{});
            var buf: [16]u8 = undefined;
            var i: u32 = 0;
            while (i < 50) : (i += 1) {
                const k = std.fmt.bufPrint(&buf, "r{d}k{:0>4}", .{ round, i }) catch unreachable;
                txn.del(dbi, k, null) catch |err| {
                    if (err == error.NotFound) continue;
                    return err;
                };
            }
            try txn.commit();
        }
    }

    // Final insert and read-back.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rnd", .{});
        try txn.put(dbi, "final", "ok", .{});
        try txn.commit();
    }
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("rnd", .{});
        const v = try txn.get(dbi, "final");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("ok", v.?);
    }
}

test "liforeclaim: LIFO mode vs FIFO produces identical results" {
    const PATH_LIFO: [:0]const u8 = "test_lifo_a.monolith";
    const PATH_FIFO: [:0]const u8 = "test_lifo_b.monolith";
    std.fs.cwd().deleteFile(PATH_LIFO) catch {};
    std.fs.cwd().deleteFile(PATH_LIFO ++ "-lck") catch {};
    std.fs.cwd().deleteFile(PATH_FIFO) catch {};
    std.fs.cwd().deleteFile(PATH_FIFO ++ "-lck") catch {};
    defer {
        std.fs.cwd().deleteFile(PATH_LIFO) catch {};
        std.fs.cwd().deleteFile(PATH_LIFO ++ "-lck") catch {};
        std.fs.cwd().deleteFile(PATH_FIFO) catch {};
        std.fs.cwd().deleteFile(PATH_FIFO ++ "-lck") catch {};
    }

    const configs = [_]struct { path: [:0]const u8, lifo: bool }{
        .{ .path = PATH_LIFO, .lifo = true },
        .{ .path = PATH_FIFO, .lifo = false },
    };
    for (configs) |cfg| {
        var env = try m.Environment.open(cfg.path, .{ .liforeclaim = cfg.lifo }, 16, 8 * 1024 * 1024);
        defer env.close();

        {
            var txn = try m.Transaction.begin(&env, null, .{});
            errdefer txn.abort();
            const dbi = try txn.openDbi("db", .{ .create = true });
            var i: u32 = 0;
            var kbuf: [8]u8 = undefined;
            while (i < 80) : (i += 1) {
                const k = std.fmt.bufPrint(&kbuf, "k{:0>6}", .{i}) catch unreachable;
                try txn.put(dbi, k, "val", .{});
            }
            try txn.commit();
        }
        {
            var txn = try m.Transaction.begin(&env, null, .{});
            errdefer txn.abort();
            const dbi = try txn.openDbi("db", .{});
            var i: u32 = 0;
            var kbuf: [8]u8 = undefined;
            while (i < 40) : (i += 1) {
                const k = std.fmt.bufPrint(&kbuf, "k{:0>6}", .{i}) catch unreachable;
                txn.del(dbi, k, null) catch {};
            }
            try txn.commit();
        }
        {
            var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
            defer txn.abort();
            const dbi = try txn.openDbi("db", .{});
            var i: u32 = 0;
            var kbuf: [8]u8 = undefined;
            while (i < 40) : (i += 1) {
                const k = std.fmt.bufPrint(&kbuf, "k{:0>6}", .{40 + i}) catch unreachable;
                const v = try txn.get(dbi, k);
                try std.testing.expect(v != null);
                try std.testing.expectEqualStrings("val", v.?);
            }
        }
    }
}
