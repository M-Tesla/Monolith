// Tests for v0.17 additions:
//   txn.txnInfo()   — live space accounting
//   txn.dbiFlags()  — read flags of an open DBI
//   txn.getEx()     — get value + dup count in one call
//   cursor.compare()— compare cursor positions
//   env.setHsr()    — handle-slow-reader callback (removed in 0.16+)

const std = @import("std");
const m = @import("../lib.zig");
const testing = std.testing;

const PATH = "test_v017.monolith";

fn cleanup() void {
    const io = std.testing.io;
    std.Io.Dir.cwd().deleteFile(io, PATH) catch {};
    std.Io.Dir.cwd().deleteFile(io, PATH ++ "-lck") catch {};
}

// ─── txn.txnInfo() ───────────────────────────────────────────────────────────

test "txnInfo: read-only txn has no dirty pages" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var wtxn = try m.Transaction.begin(&env, null, .{});
    const dbi = try wtxn.openDbi("t", .{ .create = true });
    try wtxn.put(dbi, "k", "v", .{});
    try wtxn.commit();

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();

    const info = rtxn.txnInfo();
    try testing.expect(info.space_used > 0);
    try testing.expectEqual(@as(u64, 0), info.space_dirty);
    try testing.expectEqual(@as(u64, 0), info.space_retired);
}

test "txnInfo: write txn reports dirty pages" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "hello", "world", .{});

    const info = txn.txnInfo();
    try testing.expect(info.space_used > 0);
    try testing.expect(info.space_dirty > 0);
}

// ─── txn.dbiFlags() ──────────────────────────────────────────────────────────

test "dbiFlags: plain DBI returns expected flags" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("plain", .{ .create = true });
    const flags = try txn.dbiFlags(dbi);
    try testing.expect(!flags.dupsort);
    try testing.expect(!flags.reversekey);
    try testing.expect(flags.create); // create was set
}

test "dbiFlags: dupsort DBI reports dupsort=true" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("ds", .{ .create = true, .dupsort = true });
    const flags = try txn.dbiFlags(dbi);
    try testing.expect(flags.dupsort);
}

test "dbiFlags: reversekey DBI reports reversekey=true" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("rk", .{ .create = true, .reversekey = true });
    const flags = try txn.dbiFlags(dbi);
    try testing.expect(flags.reversekey);
}

// ─── txn.getEx() ─────────────────────────────────────────────────────────────

test "getEx: key not found returns null" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    const res = try txn.getEx(dbi, "missing");
    try testing.expectEqual(@as(?m.GetExResult, null), res);
}

test "getEx: non-dupsort DBI returns count=1" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "key", "val", .{});

    const res = try txn.getEx(dbi, "key");
    try testing.expect(res != null);
    try testing.expectEqualStrings("val", res.?.val);
    try testing.expectEqual(@as(usize, 1), res.?.count);
}

test "getEx: dupsort DBI returns correct count" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("ds", .{ .create = true, .dupsort = true });
    try txn.put(dbi, "k", "a", .{});
    try txn.put(dbi, "k", "b", .{});
    try txn.put(dbi, "k", "c", .{});

    const res = try txn.getEx(dbi, "k");
    try testing.expect(res != null);
    try testing.expectEqual(@as(usize, 3), res.?.count);
}

// ─── cursor.compare() ────────────────────────────────────────────────────────

test "cursor.compare: same position is .eq" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "a", "1", .{});
    try txn.put(dbi, "b", "2", .{});

    var c1 = try txn.cursor(dbi);
    defer c1.close();
    var c2 = try txn.cursor(dbi);
    defer c2.close();

    _ = try c1.first();
    _ = try c2.first();

    try testing.expectEqual(std.math.Order.eq, c1.compare(&c2));
}

test "cursor.compare: earlier position is .lt" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "a", "1", .{});
    try txn.put(dbi, "b", "2", .{});

    var c1 = try txn.cursor(dbi);
    defer c1.close();
    var c2 = try txn.cursor(dbi);
    defer c2.close();

    _ = try c1.first();
    _ = try c2.last();

    try testing.expectEqual(std.math.Order.lt, c1.compare(&c2));
    try testing.expectEqual(std.math.Order.gt, c2.compare(&c1));
}

test "cursor.compare: invalid cursor is past-end (.gt)" {
    const io = std.testing.io;
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    try txn.put(dbi, "a", "1", .{});

    var c1 = try txn.cursor(dbi);
    defer c1.close();
    var c2 = try txn.cursor(dbi);
    defer c2.close();

    _ = try c1.first();
    // c2 is still at invalid (not positioned)

    try testing.expectEqual(std.math.Order.lt, c1.compare(&c2));
    try testing.expectEqual(std.math.Order.gt, c2.compare(&c1));
}
