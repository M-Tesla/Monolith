//! GC Recycling Safety — freed pages are not reused while a reader
//! holds a snapshot of the transaction that freed them.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_mvcc_gc.monolith";

test "mvcc_gc: freed pages not reused while reader active" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    // ── T1: insert + delete to create freed pages ──────────────────────────
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        // Insert enough entries to allocate several leaf pages (each ~64 bytes).
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], i, .little);
            std.mem.writeInt(u32, k[4..8], 0, .little);
            try txn.put(dbi, &k, "v" ** 60, .{});
        }
        // Delete all to free the leaf pages.
        i = 0;
        while (i < 200) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], i, .little);
            std.mem.writeInt(u32, k[4..8], 0, .little);
            try txn.del(dbi, &k, null);
        }
        try txn.commit();
    }

    // Record first_unallocated after T1.
    var unalloc_after_t1: u32 = 0;
    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        unalloc_after_t1 = rtxn.meta.geometry.first_unallocated;
        rtxn.abort();
    }

    // ── R: open a read-only transaction that pins T1's snapshot ────────────
    var reader = try m.Transaction.begin(&env, null, .{ .rdonly = true });

    // ── T2: write — must NOT reclaim T1's freed pages (reader is active) ──
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data2", .{ .create = true });
        var i: u32 = 0;
        while (i < 50) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], i, .little);
            std.mem.writeInt(u32, k[4..8], 0, .little);
            try txn.put(dbi, &k, "x" ** 60, .{});
        }
        try txn.commit();
    }

    // T2 had to allocate fresh pages — first_unallocated must have grown.
    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        const after_t2 = rtxn.meta.geometry.first_unallocated;
        rtxn.abort();
        try std.testing.expect(after_t2 > unalloc_after_t1);
    }

    // ── Release the reader ─────────────────────────────────────────────────
    reader.abort();

    // ── T3: write — can now reclaim T1's freed pages ───────────────────────
    {
        var unalloc_before: u32 = 0;
        {
            var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
            unalloc_before = rtxn.meta.geometry.first_unallocated;
            rtxn.abort();
        }

        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data3", .{ .create = true });
        var i: u32 = 0;
        while (i < 50) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], i, .little);
            std.mem.writeInt(u32, k[4..8], 0, .little);
            try txn.put(dbi, &k, "x" ** 60, .{});
        }
        try txn.commit();

        // With GC recycling, first_unallocated should grow only a little
        // (at most a few pages for B-tree structure overhead), not by 50+ pages.
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        const after_t3 = rtxn.meta.geometry.first_unallocated;
        rtxn.abort();
        // Allow generous overhead (GC tree structure pages + some DBI pages).
        try std.testing.expect(after_t3 <= unalloc_before + 20);
    }
}

test "mvcc_gc: GC tree survives reopen" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);

    // Insert enough items to force page splits (>~75 items per page @ 54 bytes/item),
    // then delete all so the rebalancer frees the extra leaf pages → freed_pages → GC tree.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        var i: u32 = 0;
        while (i < 300) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .little);
            try txn.put(dbi, &k, "v" ** 40, .{});
        }
        i = 0;
        while (i < 300) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .little);
            try txn.del(dbi, &k, null);
        }
        try txn.commit();
    }
    env.close();

    // Reopen — GC tree should still be intact.
    var env2 = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env2.close();

    {
        var rtxn = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
        defer rtxn.abort();
        // GC tree must have at least one entry (the freed pages from above).
        try std.testing.expect(rtxn.meta.trees.gc.items > 0);
    }
}
