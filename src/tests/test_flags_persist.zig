//! DBI Flags Persistence & Reopen Validation.
//! Tests that structural flags (dupsort, reversekey, integerkey …) are
//! serialised into Tree.flags on commit and validated on the next openDbi.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_flags_persist.monolith";

// ─── Flags round-trip ─────────────────────────────────────────────────────────

test "flags_persist: dupsort flag survives commit + reopen" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);

    // Create a dupsort DBI and commit.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "v1", .{});
        try txn.put(dbi, "k", "v2", .{});
        try txn.commit();
    }
    env.close();

    // Reopen — must succeed with matching flags.
    var env2 = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env2.close();
    {
        var txn = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .dupsort = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();
        const found = try cur.find("k");
        try std.testing.expect(found);
        const v = try cur.nextDup();
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("v2", v.?);
    }
}

test "flags_persist: incompatible flags rejected" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.commit();
    }
    env.close();

    var env2 = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env2.close();

    // Opening without dupsort must return error.Incompatible.
    {
        var txn = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
        defer txn.abort();
        const result = txn.openDbi("dup", .{});
        try std.testing.expectError(error.Incompatible, result);
    }
}

test "flags_persist: reversekey flag survives reopen" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rev", .{ .create = true, .reversekey = true });
        try txn.put(dbi, "aaa", "1", .{});
        try txn.put(dbi, "bbb", "2", .{});
        try txn.put(dbi, "ccc", "3", .{});
        try txn.commit();
    }
    env.close();

    var env2 = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env2.close();
    {
        var txn = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("rev", .{ .reversekey = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();
        // With reversekey, first() must be "ccc".
        const kv = try cur.first();
        try std.testing.expect(kv != null);
        try std.testing.expectEqualStrings("ccc", kv.?.key);
    }
}

test "flags_persist: no flags — plain DBI reopens fine" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("plain", .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.commit();
    }
    env.close();

    var env2 = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env2.close();
    {
        var txn = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("plain", .{});
        const v = try txn.get(dbi, "hello");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("world", v.?);
    }
}
