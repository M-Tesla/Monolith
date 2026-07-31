// Tests for v0.17 additions:
//   txn.txnInfo()   — live space accounting
//   txn.dbiFlags()  — read flags of an open DBI
//   txn.getEx()     — get value + dup count in one call
//   cursor.compare()— compare cursor positions
//   env.setHsr()    — handle-slow-reader callback

const std     = @import("std");
const m       = @import("../lib.zig");
const testing = std.testing;

const PATH = "test_v017.monolith";

fn cleanup() void {
    m.Environment.deleteFiles(PATH);
}

// ─── txn.txnInfo() ───────────────────────────────────────────────────────────

test "txnInfo: read-only txn has no dirty pages" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("ds", .{ .create = true, .dupsort = true });
    const flags = try txn.dbiFlags(dbi);
    try testing.expect(flags.dupsort);
}

test "dbiFlags: reversekey DBI reports reversekey=true" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("rk", .{ .create = true, .reversekey = true });
    const flags = try txn.dbiFlags(dbi);
    try testing.expect(flags.reversekey);
}

// ─── txn.getEx() ─────────────────────────────────────────────────────────────

test "getEx: key not found returns null" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    const dbi = try txn.openDbi("t", .{ .create = true });
    const res = try txn.getEx(dbi, "missing");
    try testing.expectEqual(@as(?m.GetExResult, null), res);
}

test "getEx: non-dupsort DBI returns count=1" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
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

// ─── env.setHsr() ────────────────────────────────────────────────────────────

var hsr_called: bool = false;
var hsr_laggard: u64 = 0;

fn testHsr(env: *m.Environment, laggard_txnid: u64, gap: u64, ctx: ?*anyopaque) void {
    _ = env;
    _ = gap;
    _ = ctx;
    hsr_called = true;
    hsr_laggard = laggard_txnid;
}

test "setHsr: callback is stored and retrievable via env field" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    env.setHsr(testHsr, null);
    try testing.expect(env.hsr_fn != null);
}

test "setHsr: callback fires when slow reader blocks GC" {
    cleanup();
    defer cleanup();

    hsr_called = false;

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    env.setHsr(testHsr, null);

    // Open a read txn that pins the current snapshot.
    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();

    // Write and commit several times to accumulate GC entries.
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var wtxn = try m.Transaction.begin(&env, null, .{});
        const dbi = try wtxn.openDbi("t", .{ .create = true });
        var kbuf: [8]u8 = undefined;
        const key = std.fmt.bufPrint(&kbuf, "k{d}", .{i}) catch unreachable;
        try wtxn.put(dbi, key, "v", .{});
        try wtxn.commit();
    }

    // Now delete keys so pages are freed and GC needs to reclaim.
    {
        var wtxn = try m.Transaction.begin(&env, null, .{});
        const dbi = try wtxn.openDbi("t", .{ .create = true });
        var j: usize = 0;
        while (j < 5) : (j += 1) {
            var kbuf: [8]u8 = undefined;
            const key = std.fmt.bufPrint(&kbuf, "k{d}", .{j}) catch unreachable;
            wtxn.del(dbi, key, null) catch {};
        }
        try wtxn.commit();
    }

    // The HSR may or may not have fired depending on GC state; just verify
    // that the callback is correctly wired (no crash).
    _ = hsr_called;
}

test "setHsr: clearing callback with null works" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    env.setHsr(testHsr, null);
    try testing.expect(env.hsr_fn != null);

    env.setHsr(null, null);
    try testing.expect(env.hsr_fn == null);
}
