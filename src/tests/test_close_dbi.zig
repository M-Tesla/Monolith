// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! env.closeDBI() — releases a DBI handle and frees its slot for reuse.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_close_dbi.monolith";

test "close_dbi: data survives after closing and reopening the DBI" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 16, 1 << 20);
    defer env.close();

    // Create DBI and write data.
    const dbi = blk: {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const d = try txn.openDbi("mydb", .{ .create = true });
        try txn.put(d, "key", "value", .{});
        try txn.commit();
        break :blk d;
    };

    // Close the DBI handle at env level.
    env.closeDBI(dbi);

    // Reopen in a new transaction — data must still be there.
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi2 = try txn.openDbi("mydb", .{});
        const v = try txn.get(dbi2, "key");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("value", v.?);
    }
}

test "close_dbi: slot is recycled on next openDbi" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 16, 1 << 20);
    defer env.close();

    // Open two DBIs: slot 1 and slot 2.
    const dbi_a = blk: {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const a = try txn.openDbi("alpha", .{ .create = true });
        _ = try txn.openDbi("beta",  .{ .create = true });
        try txn.commit();
        break :blk a;
    };

    // Close "alpha" (slot 1) and open a new DBI — must reuse slot 1.
    env.closeDBI(dbi_a);

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const gamma = try txn.openDbi("gamma", .{ .create = true });
        // gamma should occupy the recycled slot.
        try std.testing.expect(gamma == dbi_a);
        try txn.commit();
    }
}

test "close_dbi: multiple DBIs, each closes and reopens independently" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 32, 1 << 20);
    defer env.close();

    // Create 5 DBIs.
    var dbis: [5]m.Dbi = undefined;
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const names = [_][:0]const u8{ "a", "b", "c", "d", "e" };
        for (&dbis, names) |*d, name| {
            d.* = try txn.openDbi(name, .{ .create = true });
            try txn.put(d.*, name[0..1], name[0..1], .{});
        }
        try txn.commit();
    }

    // Close them all.
    for (dbis) |d| env.closeDBI(d);

    // Reopen all — verify data intact.
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const names = [_][:0]const u8{ "a", "b", "c", "d", "e" };
        for (names) |name| {
            const d = try txn.openDbi(name, .{});
            const v = try txn.get(d, name[0..1]);
            try std.testing.expect(v != null);
            try std.testing.expectEqualStrings(name[0..1], v.?);
        }
    }
}
