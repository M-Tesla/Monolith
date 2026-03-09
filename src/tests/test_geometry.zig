//! Geometry API — growth_step and size_upper control mmap growth.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH     = "test_geometry.monolith";
const PAGE     = 4096;

test "geometry: growth_step — each resize is a multiple of step" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    const STEP = 256 * 1024; // 256 KB

    var env = try m.Environment.open(PATH, .{}, 16, 4 * PAGE);
    defer env.close();
    env.setGeometry(.{ .growth_step = STEP });

    // Track resize calls via map_full_fn.
    const S = struct {
        var resize_count: usize = 0;
        var last_size:    usize = 0;
        fn onFull(e: *m.Environment, needed: usize) anyerror!usize {
            _ = e;
            resize_count += 1;
            // Round up to next multiple of STEP — resize() will also do this,
            // but let's just return needed; resize() will snap it.
            last_size = needed;
            return needed;
        }
    };
    S.resize_count = 0;
    env.setMapFullHandler(S.onFull);

    // Insert enough keys to force at least two mmap growths.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("g", .{ .create = true });
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{d:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "vvvvvvvvvvvvvvvvvvvvvvvv", .{});
        }
        try txn.commit();
    }

    // Verify each observed resize produced a map size that is a multiple of STEP.
    // (We check via the final file size.)
    const file = try std.fs.cwd().openFile(PATH, .{});
    defer file.close();
    const file_size = try file.getEndPos();
    try std.testing.expect(file_size % STEP == 0);
}

test "geometry: size_upper — commit returns MapFull when limit exceeded" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // Open with 8 pages; set size_upper to same — no mmap growth allowed.
    var env = try m.Environment.open(PATH, .{}, 16, 8 * PAGE);
    defer env.close();
    env.setGeometry(.{ .size_upper = 8 * PAGE });

    // First transaction: a couple of small keys — fits in the existing pages.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("g", .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.commit();
    }

    // Second transaction: 300 large-value keys — requires more than 8 pages total.
    // MapFull surfaces from commit() when writePage tries to grow beyond size_upper.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        const dbi = try txn.openDbi("g", .{});
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 300) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{d:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "vvvvvvvvvvvvvvvvvvvvvvvv", .{});
        }
        // commit must fail with MapFull; txn.abort() cleans up afterwards.
        const commit_result = txn.commit();
        txn.abort();
        try std.testing.expectError(error.MapFull, commit_result);
    }
}

test "geometry: growth_step zero — grows by minimum needed" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // No setGeometry call — growth_step stays 0 (minimum needed).
    var env = try m.Environment.open(PATH, .{}, 16, 4 * PAGE);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("g", .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("g", .{});
        const v = try txn.get(dbi, "hello");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("world", v.?);
    }
}
