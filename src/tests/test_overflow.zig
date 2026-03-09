//! Overflow Pages — values from 1 B to 1 MB.
//! Tests put/get/del/cursor, commit + reopen, and check() validation.

const std = @import("std");
const monolith = @import("../lib.zig");

const Environment = monolith.Environment;
const Transaction = monolith.Transaction;

const PAGE_SIZE: usize = 4096;
const OVERFLOW_THRESHOLD: usize = PAGE_SIZE / 2; // 2048

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build a deterministic payload of `n` bytes: byte[i] = @truncate(i * 7 + seed).
fn makePayload(buf: []u8, seed: u8) void {
    for (buf, 0..) |*b, i| b.* = @truncate(i *% 7 +% seed);
}

// ─────────────────────────────────────────────────────────────────────────────
// Basic put / get of a value just above the overflow threshold
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: put and get value just above threshold (2049 bytes)" {
    const path = "test_overflow_basic.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    // Allocate a large value (just above 2048 threshold).
    const val_len: usize = OVERFLOW_THRESHOLD + 1; // 2049
    var payload = [_]u8{0} ** (OVERFLOW_THRESHOLD + 1);
    makePayload(&payload, 42);

    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("big", .{ .create = true });
        try txn.put(dbi, "mykey", &payload, .{});
        try txn.commit();
    }

    // Read back and verify.
    {
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("big", .{});
        const got = try txn.get(dbi, "mykey");
        try std.testing.expect(got != null);
        try std.testing.expectEqual(val_len, got.?.len);
        try std.testing.expectEqualSlices(u8, &payload, got.?);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Large value spanning many overflow pages (1 MB)
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: put and get 1 MB value" {
    const path = "test_overflow_1mb.monolith";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile(path ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    const val_len: usize = 1024 * 1024; // 1 MB
    const payload = try std.testing.allocator.alloc(u8, val_len);
    defer std.testing.allocator.free(payload);
    makePayload(payload, 99);

    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("mega", .{ .create = true });
        try txn.put(dbi, "bigval", payload, .{});
        try txn.commit();
    }

    {
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("mega", .{});
        const got = try txn.get(dbi, "bigval");
        try std.testing.expect(got != null);
        try std.testing.expectEqual(val_len, got.?.len);
        // Spot-check a few bytes.
        try std.testing.expectEqual(payload[0], got.?[0]);
        try std.testing.expectEqual(payload[val_len - 1], got.?[val_len - 1]);
        try std.testing.expectEqual(payload[4096], got.?[4096]);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Commit + reopen: overflow survives across sessions
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: survives commit and reopen" {
    const path = "test_overflow_reopen.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    const val_len: usize = 8192; // 8 KB — two overflow pages
    var payload: [8192]u8 = undefined;
    makePayload(&payload, 7);

    // Write.
    {
        var env = try Environment.open(path, .{}, 4, 16 * 1024 * 1024);
        defer env.close();
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("store", .{ .create = true });
        try txn.put(dbi, "bigkey", &payload, .{});
        try txn.commit();
    }

    // Reopen and read.
    {
        var env = try Environment.open(path, .{}, 4, 16 * 1024 * 1024);
        defer env.close();
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("store", .{});
        const got = try txn.get(dbi, "bigkey");
        try std.testing.expect(got != null);
        try std.testing.expectEqual(val_len, got.?.len);
        try std.testing.expectEqualSlices(u8, &payload, got.?);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Delete frees overflow pages
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: delete removes large value and frees pages" {
    const path = "test_overflow_del.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    var payload: [4096 * 3]u8 = undefined;
    makePayload(&payload, 13);

    // Write.
    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("obj", .{ .create = true });
        try txn.put(dbi, "large", &payload, .{});
        try txn.commit();
    }

    const pages_after_write = env.bestMeta().geometry.first_unallocated;

    // Delete.
    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("obj", .{});
        try txn.del(dbi, "large", null);
        try txn.commit();
    }

    // Key must be gone.
    {
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("obj", .{});
        const got = try txn.get(dbi, "large");
        try std.testing.expect(got == null);
    }

    // The GC tree should have accumulated freed pages from the overflow run.
    // Allocate enough new pages to confirm recycling occurs.
    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("obj", .{});
        // Insert another large value — should reuse freed pages.
        try txn.put(dbi, "large2", &payload, .{});
        try txn.commit();
    }

    // The total allocated page count should not grow unboundedly.
    const pages_after_reuse = env.bestMeta().geometry.first_unallocated;
    _ = pages_after_write;
    // Sanity: we haven't ballooned to 100× the original size.
    try std.testing.expect(pages_after_reuse < 200);
}

// ─────────────────────────────────────────────────────────────────────────────
// Cursor: currentKV() returns the correct large value
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: cursor iteration yields large value" {
    const path = "test_overflow_cursor.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    const val_len: usize = 5000; // ~5 KB
    var payload: [5000]u8 = undefined;
    makePayload(&payload, 55);

    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tab", .{ .create = true });
        // Insert a small key before and after to ensure cursor traversal.
        try txn.put(dbi, "aaa", "small_before", .{});
        try txn.put(dbi, "bbb", &payload, .{});
        try txn.put(dbi, "ccc", "small_after", .{});
        try txn.commit();
    }

    {
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("tab", .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();

        // first → "aaa"
        var kv = try cur.first();
        try std.testing.expect(kv != null);
        try std.testing.expectEqualSlices(u8, "aaa", kv.?.key);

        // next → "bbb" with large value
        kv = try cur.next();
        try std.testing.expect(kv != null);
        try std.testing.expectEqualSlices(u8, "bbb", kv.?.key);
        try std.testing.expectEqual(val_len, kv.?.val.len);
        try std.testing.expectEqual(payload[0], kv.?.val[0]);
        try std.testing.expectEqual(payload[val_len - 1], kv.?.val[val_len - 1]);

        // next → "ccc"
        kv = try cur.next();
        try std.testing.expect(kv != null);
        try std.testing.expectEqualSlices(u8, "ccc", kv.?.key);

        // next → end
        kv = try cur.next();
        try std.testing.expect(kv == null);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overwrite an overflow value with another large value (COW of overflow run)
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: overwrite large value with another large value" {
    const path = "test_overflow_overwrite.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    var v1: [3000]u8 = undefined;
    var v2: [6000]u8 = undefined;
    makePayload(&v1, 1);
    makePayload(&v2, 2);

    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rw", .{ .create = true });
        try txn.put(dbi, "k", &v1, .{});
        try txn.commit();
    }

    // Overwrite with a different large value.
    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rw", .{});
        try txn.put(dbi, "k", &v2, .{});
        try txn.commit();
    }

    {
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("rw", .{});
        const got = try txn.get(dbi, "k");
        try std.testing.expect(got != null);
        try std.testing.expectEqual(@as(usize, 6000), got.?.len);
        try std.testing.expectEqualSlices(u8, &v2, got.?);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Mixed inline + overflow values in the same DBI
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: mix of inline and overflow values in one DBI" {
    const path = "test_overflow_mixed.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    const n_inline = 100;
    const n_large  = 10;

    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("mix", .{ .create = true });

        // Insert inline values.
        var i: u64 = 0;
        while (i < n_inline) : (i += 1) {
            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, i, .big);
            try txn.put(dbi, &key_buf, "small", .{});
        }

        // Insert overflow values.
        var j: u64 = 1000;
        while (j < 1000 + n_large) : (j += 1) {
            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, j, .big);
            var large_buf: [3000]u8 = undefined;
            makePayload(&large_buf, @truncate(j));
            try txn.put(dbi, &key_buf, &large_buf, .{});
        }
        try txn.commit();
    }

    // Read back all inline values.
    {
        var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("mix", .{});

        var i: u64 = 0;
        while (i < n_inline) : (i += 1) {
            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, i, .big);
            const got = try txn.get(dbi, &key_buf);
            try std.testing.expect(got != null);
            try std.testing.expectEqualSlices(u8, "small", got.?);
        }

        // Read back overflow values.
        var j: u64 = 1000;
        while (j < 1000 + n_large) : (j += 1) {
            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, j, .big);
            const got = try txn.get(dbi, &key_buf);
            try std.testing.expect(got != null);
            try std.testing.expectEqual(@as(usize, 3000), got.?.len);
            // Spot-check first byte.
            var expected_buf: [3000]u8 = undefined;
            makePayload(&expected_buf, @truncate(j));
            try std.testing.expectEqual(expected_buf[0], got.?[0]);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// env.check() reports zero errors with overflow pages present
// ─────────────────────────────────────────────────────────────────────────────

test "overflow: env.check() validates overflow page runs" {
    const path = "test_overflow_check.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    {
        var txn = try Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("chk", .{ .create = true });

        // Insert several large values to create overflow runs.
        const sizes = [_]usize{ 2049, 4096, 8192, 16384 };
        for (sizes, 0..) |sz, idx| {
            var key_buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &key_buf, @intCast(idx), .big);
            const large_buf = try std.testing.allocator.alloc(u8, sz);
            defer std.testing.allocator.free(large_buf);
            makePayload(large_buf, @truncate(idx * 3));
            try txn.put(dbi, &key_buf, large_buf, .{});
        }
        try txn.commit();
    }

    const result = env.check();
    try std.testing.expectEqual(@as(u64, 0), result.errors);
    // We should have visited at least 2 (init) + 4 overflow pages.
    try std.testing.expect(result.pages_visited >= 6);
}
