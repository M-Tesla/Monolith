//! Page split and dropDbi tests.
//! - splits: insert enough entries to force leaf and branch page splits.
//! - dropDbi: empty or permanently remove a named DBI.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_splits_drop.monolith";

// ─── Splits ──────────────────────────────────────────────────────────────────

test "splits: 500 keys force splits and order is preserved" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const N: u32 = 500;

    // Insert 500 keys in ascending order
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("table", .{ .create = true });
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key{d:0>10}", .{i}) catch unreachable;
            var val_buf: [16]u8 = undefined;
            const val = std.fmt.bufPrint(&val_buf, "val{d:0>10}", .{i}) catch unreachable;
            try txn.put(dbi, key, val, .{});
        }
        try txn.commit();
    }

    // Verify count and order via cursor
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("table", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    var count: u32 = 0;
    var prev_key_buf: [16]u8 = undefined;
    var prev_key_len: usize  = 0;

    var kv_opt = try cur.first();
    while (kv_opt) |kv| : (kv_opt = try cur.next()) {
        // Verify lexicographic order
        if (prev_key_len > 0) {
            const cmp = std.mem.order(u8, prev_key_buf[0..prev_key_len], kv.key);
            try std.testing.expect(cmp == .lt);
        }
        @memcpy(prev_key_buf[0..kv.key.len], kv.key);
        prev_key_len = kv.key.len;
        count += 1;
    }
    try std.testing.expectEqual(N, count);
}

test "splits: 500 keys inserted in reverse order" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const N: u32 = 500;

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rev", .{ .create = true });
        var i: u32 = N;
        while (i > 0) : (i -= 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key{d:0>10}", .{i - 1}) catch unreachable;
            try txn.put(dbi, key, "x", .{});
        }
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("rev", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    var count: u32 = 0;
    var kv_opt = try cur.first();
    while (kv_opt != null) : (kv_opt = try cur.next()) count += 1;
    try std.testing.expectEqual(N, count);
}

test "splits: get after commit with splits" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const N: u32 = 300;

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("db", .{ .create = true });
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "k{d:0>12}", .{i}) catch unreachable;
            var val_buf: [16]u8 = undefined;
            const val = std.fmt.bufPrint(&val_buf, "v{d:0>12}", .{i}) catch unreachable;
            try txn.put(dbi, key, val, .{});
        }
        try txn.commit();
    }

    // Verify a subset via get()
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("db", .{});

    var key_buf: [16]u8 = undefined;
    var val_buf: [16]u8 = undefined;
    var i: u32 = 0;
    while (i < N) : (i += 37) { // sample every 37th
        const key = std.fmt.bufPrint(&key_buf, "k{d:0>12}", .{i}) catch unreachable;
        const expected = std.fmt.bufPrint(&val_buf, "v{d:0>12}", .{i}) catch unreachable;
        const got = try txn.get(dbi, key);
        try std.testing.expect(got != null);
        try std.testing.expectEqualStrings(expected, got.?);
    }
}

// ─── dropDbi ─────────────────────────────────────────────────────────────────

test "dropDbi: empty without delete keeps DBI in catalog" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("mydb", .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.commit();
    }

    // Drop without delete (delete=false)
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.dropDbi("mydb", false);
        try txn.commit();
    }

    // DBI still exists but is empty
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("mydb", .{});
        const val = try txn.get(dbi, "a");
        try std.testing.expect(val == null);
        var cur = try txn.cursor(dbi);
        defer cur.close();
        const first = try cur.first();
        try std.testing.expect(first == null);
    }
}

test "dropDbi: permanent delete removes from catalog" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("todelete", .{ .create = true });
        try txn.put(dbi, "x", "y", .{});
        try txn.commit();
    }

    // Drop with delete=true
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.dropDbi("todelete", true);
        try txn.commit();
    }

    // Reopen without .create must fail
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const result = txn.openDbi("todelete", .{});
    try std.testing.expectError(error.NotFound, result);
}

// ─── Rebalance / Coalescing ───────────────────────────────────────────────────

test "rebalance: delete all keys via cursor collapses the tree" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const N: u32 = 400;

    // Insert 400 keys (triggers splits)
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tree", .{ .create = true });
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "k{d:0>12}", .{i}) catch unreachable;
            try txn.put(dbi, key, "v", .{});
        }
        try txn.commit();
    }

    // Delete all via cursor
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tree", .{});
        var cur = try txn.cursor(dbi);
        var kv = try cur.first();
        while (kv != null) {
            try cur.del();
            kv = try cur.first(); // reposition after merge
        }
        cur.close();
        try txn.commit();
    }

    // Verify the tree is empty
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi  = try txn.openDbi("tree", .{});
    var cur    = try txn.cursor(dbi);
    defer cur.close();
    const first = try cur.first();
    try std.testing.expect(first == null);
    const val = try txn.get(dbi, "k000000000001");
    try std.testing.expect(val == null);
}

test "rebalance: delete half the keys maintains consistency" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const N: u32 = 300;

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("db", .{ .create = true });
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "k{d:0>12}", .{i}) catch unreachable;
            try txn.put(dbi, key, "val", .{});
        }
        try txn.commit();
    }

    // Delete even-indexed keys via txn.del
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("db", .{});
        var i: u32 = 0;
        while (i < N) : (i += 2) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "k{d:0>12}", .{i}) catch unreachable;
            try txn.del(dbi, key, null);
        }
        try txn.commit();
    }

    // Verify odd keys still exist and even keys are gone
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("db", .{});

    var count: u32 = 0;
    var cur = try txn.cursor(dbi);
    defer cur.close();
    var kv_opt = try cur.first();
    while (kv_opt != null) : (kv_opt = try cur.next()) count += 1;
    try std.testing.expectEqual(N / 2, count); // 150 odd keys

    // Spot-check (separate buffers to avoid aliasing)
    var buf_odd:  [16]u8 = undefined;
    var buf_even: [16]u8 = undefined;
    const k_odd  = std.fmt.bufPrint(&buf_odd,  "k{d:0>12}", .{1}) catch unreachable;
    const k_even = std.fmt.bufPrint(&buf_even, "k{d:0>12}", .{2}) catch unreachable;
    try std.testing.expect((try txn.get(dbi, k_odd)) != null);
    try std.testing.expect((try txn.get(dbi, k_even)) == null);
}

test "rebalance: root collapse after full deletion" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        // Enough entries to create 1 split (2 leaves + 1 branch)
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key{d:0>10}", .{i}) catch unreachable;
            try txn.put(dbi, key, "v", .{});
        }
        try txn.commit();
    }

    // Delete everything via txn.del
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{});
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            var key_buf: [16]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key{d:0>10}", .{i}) catch unreachable;
            try txn.del(dbi, key, null);
        }
        try txn.commit();
    }

    // Tree must be empty
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi  = try txn.openDbi("t", .{});
    var cur    = try txn.cursor(dbi);
    defer cur.close();
    try std.testing.expect((try cur.first()) == null);
}

test "dropDbi: other DBIs are not affected" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const a = try txn.openDbi("alpha", .{ .create = true });
        const b = try txn.openDbi("beta",  .{ .create = true });
        try txn.put(a, "key1", "val1", .{});
        try txn.put(b, "key2", "val2", .{});
        try txn.commit();
    }

    // Drop "alpha"
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.dropDbi("alpha", true);
        try txn.commit();
    }

    // "beta" remains intact
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const b   = try txn.openDbi("beta", .{});
    const val = try txn.get(b, "key2");
    try std.testing.expect(val != null);
    try std.testing.expectEqualStrings("val2", val.?);
}

// ─── cursor.del() keeps position ─────────────────────────────────────────────

test "cursor_del: del() positions on successor (no rebalance)" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("cd", .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.put(dbi, "d", "4", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("cd", .{});
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Position on "b", delete it — successor should be "c".
    const found = try cur.find("b");
    try std.testing.expect(found);
    try cur.del();
    const after = try cur.current();
    try std.testing.expect(after != null);
    try std.testing.expectEqualStrings("c", after.?.key);

    // Delete "c" — successor "d".
    try cur.del();
    const after2 = try cur.current();
    try std.testing.expect(after2 != null);
    try std.testing.expectEqualStrings("d", after2.?.key);

    // Delete "d" (last) — cursor becomes invalid.
    try cur.del();
    const after3 = try cur.current();
    try std.testing.expect(after3 == null);

    try txn.commit();
}

test "cursor_del: del() loop over all entries" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const N: u32 = 200;

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("loop", .{ .create = true });
        var i: u32 = 0;
        while (i < N) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], i, .little);
            std.mem.writeInt(u32, k[4..8], 0, .little);
            try txn.put(dbi, &k, "v", .{});
        }
        try txn.commit();
    }

    // Delete every entry via cursor, using cur.current() after each del().
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("loop", .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();

        var deleted: u32 = 0;
        var kv = try cur.first();
        while (kv != null) {
            try cur.del();
            deleted += 1;
            kv = try cur.current();
        }
        try std.testing.expectEqual(N, deleted);
        try txn.commit();
    }

    // Table must be empty.
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("loop", .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();
        const kv = try cur.first();
        try std.testing.expect(kv == null);
    }
}
