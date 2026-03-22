// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! PutFlags.alldups and PutFlags.appenddup for DupSort DBIs.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_alldups_appenddup.monolith";

test "alldups: replaces all dups with a single new value" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "a", .{});
        try txn.put(dbi, "k", "b", .{});
        try txn.put(dbi, "k", "c", .{});
        try txn.commit();
    }

    // alldups: replace all three with a single "z"
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .dupsort = true });
        try txn.put(dbi, "k", "z", .{ .alldups = true });
        try txn.commit();
    }

    // Verify only "z" remains.
    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const dbi = try rtxn.openDbi("dup", .{ .dupsort = true });
        var cur = try rtxn.cursor(dbi);
        defer cur.close();
        try std.testing.expect(try cur.find("k")); // sets key_prefix_len
        const cnt = try cur.countDups();
        try std.testing.expectEqual(@as(u64, 1), cnt);
        const kv = try cur.current();
        try std.testing.expect(kv != null);
        try std.testing.expectEqualStrings("k", kv.?.key);
        try std.testing.expectEqualStrings("z", kv.?.val);
    }
}

test "alldups: works when key has a single dup" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "only", .{});
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .dupsort = true });
        try txn.put(dbi, "k", "new", .{ .alldups = true });
        try txn.commit();
    }

    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const dbi = try rtxn.openDbi("dup", .{ .dupsort = true });
        const v = try rtxn.get(dbi, "k");
        try std.testing.expectEqualStrings("new", v.?);
    }
}

test "appenddup: bulk inserts in sorted order" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        // Insert in ascending order using appenddup.
        try txn.put(dbi, "k", "d1", .{ .appenddup = true });
        try txn.put(dbi, "k", "d2", .{ .appenddup = true });
        try txn.put(dbi, "k", "d3", .{ .appenddup = true });
        try txn.commit();
    }

    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const dbi = try rtxn.openDbi("dup", .{ .dupsort = true });
        var cur = try rtxn.cursor(dbi);
        defer cur.close();
        try std.testing.expect(try cur.find("k"));
        const cnt = try cur.countDups();
        try std.testing.expectEqual(@as(u64, 3), cnt);
    }
}

test "appenddup: mixed append and normal put" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "aa", .{});
        try txn.put(dbi, "k", "bb", .{});
        // appenddup for something >= "bb"
        try txn.put(dbi, "k", "cc", .{ .appenddup = true });
        try txn.commit();
    }

    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const dbi = try rtxn.openDbi("dup", .{ .dupsort = true });
        var cur = try rtxn.cursor(dbi);
        defer cur.close();
        try std.testing.expect(try cur.find("k"));
        const cnt = try cur.countDups();
        try std.testing.expectEqual(@as(u64, 3), cnt);
    }
}
