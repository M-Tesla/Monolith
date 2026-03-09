//! Engine Limits API.
//! Verifies compile-time limit constants and that exceeding them returns
//! error.BadValSize from txn.put() where enforced.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_limits.monolith";

// ─── Limit constant sanity ────────────────────────────────────────────────────

test "limits: page_size is 4096" {
    try std.testing.expectEqual(@as(usize, 4096), m.limits.page_size);
}

test "limits: max_key_size is page_size/2 - 8" {
    try std.testing.expectEqual(m.limits.page_size / 2 - 8, m.limits.max_key_size);
}

test "limits: max_val_size is page_size/2 - 8" {
    try std.testing.expectEqual(m.limits.page_size / 2 - 8, m.limits.max_val_size);
}

test "limits: max_dbs is 128" {
    try std.testing.expectEqual(@as(usize, 128), m.limits.max_dbs);
}

test "limits: max_pages fits in u32" {
    try std.testing.expect(m.limits.max_pages <= std.math.maxInt(u32));
}

// ─── Key size enforcement ─────────────────────────────────────────────────────

test "limits: key at max_key_size succeeds" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    const key = try std.testing.allocator.alloc(u8, m.limits.max_key_size);
    defer std.testing.allocator.free(key);
    @memset(key, 'k');

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, key, "v", .{});
    try txn.commit();
}

test "limits: key exceeding max_key_size returns error.BadValSize" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    const key = try std.testing.allocator.alloc(u8, m.limits.max_key_size + 1);
    defer std.testing.allocator.free(key);
    @memset(key, 'k');

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("t", .{ .create = true });
    try std.testing.expectError(error.BadValSize, txn.put(dbi, key, "v", .{}));
}

// ─── Value size — no hard limit (overflow pages handle large values) ──────────

test "limits: inline value (at max_val_size) succeeds" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    const val = try std.testing.allocator.alloc(u8, m.limits.max_val_size);
    defer std.testing.allocator.free(val);
    @memset(val, 'v');

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "key", val, .{});
    try txn.commit();
}

test "limits: large value (overflow) succeeds and reads back correctly" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    // 8 KB — well above max_val_size; stored via overflow pages.
    const val = try std.testing.allocator.alloc(u8, 8 * 1024);
    defer std.testing.allocator.free(val);
    @memset(val, 0xAB);

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("big", .{ .create = true });
        try txn.put(dbi, "bigkey", val, .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("big", .{});
    const got = try txn.get(dbi, "bigkey");
    try std.testing.expect(got != null);
    try std.testing.expectEqual(val.len, got.?.len);
    try std.testing.expectEqualSlices(u8, val, got.?);
}
