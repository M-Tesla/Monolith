//! Cursor Backward Iteration — prev(), prevDup(), firstDup(), lastDup().

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_cursor_prev.monolith";

// ─── Plain B-tree backward iteration ─────────────────────────────────────────

test "cursor_prev: backward iteration over plain keys" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    // Insert keys "k00".."k09"
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("plain", .{ .create = true });
        var i: u8 = 0;
        while (i < 10) : (i += 1) {
            var key_buf: [3]u8 = undefined;
            key_buf[0] = 'k';
            key_buf[1] = '0' + i / 10;
            key_buf[2] = '0' + i % 10;
            try txn.put(dbi, &key_buf, "v", .{});
        }
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("plain", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Forward to last
    _ = try cur.last();
    const kv0 = try cur.current();
    try std.testing.expect(kv0 != null);
    try std.testing.expectEqualStrings("k09", kv0.?.key);

    // Backward: k09 → k08 → ... → k00
    var expected: i16 = 8;
    while (expected >= 0) : (expected -= 1) {
        const prev_kv = try cur.prev();
        try std.testing.expect(prev_kv != null);
        var exp_buf: [3]u8 = undefined;
        exp_buf[0] = 'k';
        exp_buf[1] = '0' + @as(u8, @intCast(expected)) / 10;
        exp_buf[2] = '0' + @as(u8, @intCast(expected)) % 10;
        try std.testing.expectEqualStrings(&exp_buf, prev_kv.?.key);
    }

    // One more prev() → null (already at beginning)
    const nil = try cur.prev();
    try std.testing.expect(nil == null);
}

test "cursor_prev: forward then backward round-trip" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rt", .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.put(dbi, "d", "4", .{});
        try txn.put(dbi, "e", "5", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("rt", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Forward: a b c d e
    const keys_fwd = [_][]const u8{ "a", "b", "c", "d", "e" };
    var kv = try cur.first();
    for (keys_fwd) |k| {
        try std.testing.expect(kv != null);
        try std.testing.expectEqualStrings(k, kv.?.key);
        kv = try cur.next();
    }
    try std.testing.expect(kv == null);

    // Backward: e d c b a
    const keys_bwd = [_][]const u8{ "e", "d", "c", "b", "a" };
    kv = try cur.last();
    for (keys_bwd) |k| {
        try std.testing.expect(kv != null);
        try std.testing.expectEqualStrings(k, kv.?.key);
        kv = try cur.prev();
    }
    try std.testing.expect(kv == null);
}

// ─── DupSort backward ─────────────────────────────────────────────────────────

test "cursor_prev: prevDup iterates backward within dup-key" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "user", "aaa", .{});
        try txn.put(dbi, "user", "bbb", .{});
        try txn.put(dbi, "user", "ccc", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("dup", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Position at first dup
    const found = try cur.find("user");
    try std.testing.expect(found);

    // Advance to last via nextDup
    _ = try cur.nextDup();
    _ = try cur.nextDup();

    // Now we are at "ccc" — go backward
    const v2 = try cur.prevDup();
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("bbb", v2.?);

    const v1 = try cur.prevDup();
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("aaa", v1.?);

    // One more → null (at first dup already)
    const nil = try cur.prevDup();
    try std.testing.expect(nil == null);
}

test "cursor_prev: firstDup and lastDup reposition correctly" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("fl", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "alpha", .{});
        try txn.put(dbi, "k", "beta", .{});
        try txn.put(dbi, "k", "gamma", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("fl", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    const found = try cur.find("k");
    try std.testing.expect(found);

    // lastDup → "gamma"
    const last = try cur.lastDup();
    try std.testing.expect(last != null);
    try std.testing.expectEqualStrings("gamma", last.?);

    // firstDup → "alpha"
    const first = try cur.firstDup();
    try std.testing.expect(first != null);
    try std.testing.expectEqualStrings("alpha", first.?);

    // lastDup again → "gamma"
    const last2 = try cur.lastDup();
    try std.testing.expect(last2 != null);
    try std.testing.expectEqualStrings("gamma", last2.?);
}

test "cursor_prev: prevDup stops at key boundary" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("bound", .{ .create = true, .dupsort = true });
        // Two distinct keys
        try txn.put(dbi, "aaa", "x", .{});
        try txn.put(dbi, "bbb", "y1", .{});
        try txn.put(dbi, "bbb", "y2", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("bound", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Position on "bbb" key
    const found = try cur.find("bbb");
    try std.testing.expect(found);

    // Advance to "y2"
    _ = try cur.nextDup();

    // prevDup → "y1"
    const pv = try cur.prevDup();
    try std.testing.expect(pv != null);
    try std.testing.expectEqualStrings("y1", pv.?);

    // prevDup → null (would cross into "aaa" key territory)
    const nil = try cur.prevDup();
    try std.testing.expect(nil == null);
}

test "cursor_prev: single entry prev() returns null" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("single", .{ .create = true });
        try txn.put(dbi, "only", "one", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("single", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    const kv = try cur.first();
    try std.testing.expect(kv != null);
    try std.testing.expectEqualStrings("only", kv.?.key);

    const nil = try cur.prev();
    try std.testing.expect(nil == null);
}
