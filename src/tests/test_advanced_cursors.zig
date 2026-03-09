//! Cursor Renew, REVERSEDUP sorting, MmapFull callback.

const std = @import("std");
const monolith = @import("../lib.zig");

const Environment = monolith.Environment;
const Transaction = monolith.Transaction;

// ─────────────────────────────────────────────────────────────────────────────
// Cursor Renew
// ─────────────────────────────────────────────────────────────────────────────

test "cursor renew: reuse cursor across successive read transactions" {
    const path = "test_cursor_renew.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    // Seed some data.
    {
        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("renew_test", .{ .create = true });
        try txn.put(dbi, "apple", "1", .{});
        try txn.put(dbi, "banana", "2", .{});
        try txn.put(dbi, "cherry", "3", .{});
        try txn.commit();
    }

    // First read transaction — open cursor, iterate.
    var txn1 = try Transaction.begin(&env, null, .{ .rdonly = true });
    const dbi1 = try txn1.openDbi("renew_test", .{});
    var cur = try txn1.cursor(dbi1);
    defer cur.close();

    var kv = try cur.first();
    try std.testing.expect(kv != null);
    try std.testing.expectEqualSlices(u8, "apple", kv.?.key);
    txn1.abort();

    // Second read transaction — renew the same cursor instead of creating a new one.
    var txn2 = try Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn2.abort();
    const dbi2 = try txn2.openDbi("renew_test", .{});
    _ = dbi2; // same slot guaranteed for same name in same environment

    try cur.renew(&txn2);

    kv = try cur.first();
    try std.testing.expect(kv != null);
    try std.testing.expectEqualSlices(u8, "apple", kv.?.key);

    kv = try cur.next();
    try std.testing.expect(kv != null);
    try std.testing.expectEqualSlices(u8, "banana", kv.?.key);
}

test "cursor renew: fails when DBI is not open in new transaction" {
    const path = "test_cursor_renew_fail.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    {
        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("db_a", .{ .create = true });
        try txn.put(dbi, "k", "v", .{});
        try txn.commit();
    }

    var txn1 = try Transaction.begin(&env, null, .{ .rdonly = true });
    const dbi1 = try txn1.openDbi("db_a", .{});
    var cur = try txn1.cursor(dbi1);
    defer cur.close();
    txn1.abort();

    // New txn does NOT open "db_a" — renew must fail.
    var txn2 = try Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn2.abort();

    const err = cur.renew(&txn2);
    try std.testing.expectError(error.BadDbi, err);
}

// ─────────────────────────────────────────────────────────────────────────────
// REVERSEDUP sorting
// ─────────────────────────────────────────────────────────────────────────────

test "reversedup: values iterated in descending order" {
    const path = "test_reversedup.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("revdup", .{
        .create   = true,
        .dupsort  = true,
        .reversedup = true,
    });

    // Insert three values for the same key (in arbitrary order).
    try txn.put(dbi, "key", "bbb", .{});
    try txn.put(dbi, "key", "aaa", .{});
    try txn.put(dbi, "key", "ccc", .{});

    // Cursor should iterate in descending value order: "ccc" → "bbb" → "aaa".
    var cur = try txn.cursor(dbi);
    defer cur.close();

    _ = try cur.find("key");
    const kv0 = try cur.current();
    try std.testing.expect(kv0 != null);
    try std.testing.expectEqualSlices(u8, "ccc", kv0.?.val);

    const v1 = try cur.nextDup();
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualSlices(u8, "bbb", v1.?);

    const v2 = try cur.nextDup();
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualSlices(u8, "aaa", v2.?);

    const v3 = try cur.nextDup();
    try std.testing.expect(v3 == null); // no more dups

    try txn.commit();
}

test "reversedup: persists and reopens correctly" {
    const path = "test_reversedup_persist.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        defer env.close();
        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("revdb", .{
            .create     = true,
            .dupsort    = true,
            .reversedup = true,
        });
        try txn.put(dbi, "x", "zzz", .{});
        try txn.put(dbi, "x", "aaa", .{});
        try txn.commit();
    }

    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        defer env.close();
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("revdb", .{ .dupsort = true, .reversedup = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        _ = try cur.find("x");
        const kv = try cur.current();
        try std.testing.expect(kv != null);
        // Largest value must come first.
        try std.testing.expectEqualSlices(u8, "zzz", kv.?.val);
    }
}

test "reversedup: countDups still accurate" {
    const path = "test_reversedup_count.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("revcount", .{
        .create     = true,
        .dupsort    = true,
        .reversedup = true,
    });
    try txn.put(dbi, "k", "v1", .{});
    try txn.put(dbi, "k", "v2", .{});
    try txn.put(dbi, "k", "v3", .{});

    var cur = try txn.cursor(dbi);
    defer cur.close();
    _ = try cur.find("k");
    try std.testing.expectEqual(@as(usize, 3), try cur.countDups());

    try txn.commit();
}

// ─────────────────────────────────────────────────────────────────────────────
// MmapFull callback
// ─────────────────────────────────────────────────────────────────────────────

// Callback that doubles the required size.
fn doubleSize(_: *Environment, needed: usize) anyerror!usize {
    return needed * 2;
}

test "map_full_fn: callback invoked when mmap grows" {
    const path = "test_mapfull_cb.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    // Start with a tiny initial map (4 pages) so growth is forced quickly.
    var env = try Environment.open(path, .{}, 4, 4 * 4096);
    defer env.close();

    env.setMapFullHandler(doubleSize);

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("cb_test", .{ .create = true });

    // Insert enough data to force the mmap to grow.
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, @as(u64, i), .big);
        try txn.put(dbi, &key_buf, "value_data_here", .{});
    }
    try txn.commit();

    // Verify the data is intact.
    var rtxn = try Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("cb_test", .{});
    var key_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &key_buf, 42, .big);
    const val = try rtxn.get(rdbi, &key_buf);
    try std.testing.expect(val != null);
    try std.testing.expectEqualSlices(u8, "value_data_here", val.?);
}

// Callback that refuses to grow — always returns MapFull.
fn refuseGrowth(_: *Environment, _: usize) anyerror!usize {
    return error.MapFull;
}

test "map_full_fn: callback can abort commit by returning error" {
    const path = "test_mapfull_abort.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    // Start with exactly 4 pages (0=meta0, 1=meta1, 2=main_root, 3=gc_root).
    // Opening any new DBI allocates page 4, which overflows the 4-page mmap.
    var env = try Environment.open(path, .{}, 4, 4 * 4096);
    defer env.close();
    env.setMapFullHandler(refuseGrowth);

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "key", "val", .{});

    // Commit must fail: page 4 is at offset 16 384 which equals map.len,
    // so resize fires, the callback returns error.MapFull, commit propagates it.
    try std.testing.expectError(error.MapFull, txn.commit());
}
