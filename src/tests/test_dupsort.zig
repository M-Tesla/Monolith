// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Dupsort tests — secondary indexes.
//! Pattern used in: accounts_by_user, keys_by_account, txn_by_user, etc.

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_dupsort.monolith";

test "dupsort: put multiple values per key" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    // Insert: user_doc → [account1, account2, account3]
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("accounts_by_user", .{
            .create   = true,
            .dupsort  = true,
        });

        const user_doc = "12345678901   "; // 14 bytes (CPF zero-padded)
        try txn.put(dbi, user_doc, "account-uuid-001", .{});
        try txn.put(dbi, user_doc, "account-uuid-002", .{});
        try txn.put(dbi, user_doc, "account-uuid-003", .{});
        try txn.commit();
    }

    // Read and iterate dups
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("accounts_by_user", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    const user_doc = "12345678901   ";
    const found = try cur.find(user_doc);
    try std.testing.expect(found);

    // First value
    const kv = try cur.current();
    try std.testing.expect(kv != null);

    // Count dups
    const count = try cur.countDups();
    try std.testing.expectEqual(@as(usize, 3), count);

    // Iterate
    var seen: usize = 1; // already positioned on the first
    while (try cur.nextDup()) |_| seen += 1;
    try std.testing.expectEqual(@as(usize, 3), seen);
}

test "dupsort: delete a specific value" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("idx", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "key", "val_a", .{});
        try txn.put(dbi, "key", "val_b", .{});
        try txn.commit();
    }

    // Delete only val_a
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("idx", .{ .dupsort = true });
        try txn.del(dbi, "key", "val_a");
        try txn.commit();
    }

    // Verify val_b still exists
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("idx", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    const found = try cur.find("key");
    try std.testing.expect(found);

    const kv = try cur.current();
    try std.testing.expect(kv != null);
    try std.testing.expectEqualStrings("val_b", kv.?.val);

    // No more dups
    const next = try cur.nextDup();
    try std.testing.expect(next == null);
}

test "dupsort: multiple independent keys" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("idx", .{ .create = true, .dupsort = true });

        // user A has 2 accounts
        try txn.put(dbi, "userA", "acc1", .{});
        try txn.put(dbi, "userA", "acc2", .{});
        // user B has 1 account
        try txn.put(dbi, "userB", "acc3", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("idx", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // userA → 2 dups
    try std.testing.expect(try cur.find("userA"));
    try std.testing.expectEqual(@as(usize, 2), try cur.countDups());

    // userB → 1 dup
    try std.testing.expect(try cur.find("userB"));
    try std.testing.expectEqual(@as(usize, 1), try cur.countDups());
}
