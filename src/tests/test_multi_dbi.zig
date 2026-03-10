// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Tests for multiple DBIs within a single environment.
//! Simulates a real schema: users + accounts + accounts_by_user.

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_multi_dbi.monolith";

test "multi_dbi: 3 distinct DBIs in the same file" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 16, 32 * 1024 * 1024);
    defer env.close();

    // Setup: create 3 DBIs in a write txn
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();

        const dbi_users    = try txn.openDbi("users",            .{ .create = true });
        const dbi_accounts = try txn.openDbi("accounts",         .{ .create = true });
        const dbi_by_user  = try txn.openDbi("accounts_by_user", .{ .create = true, .dupsort = true });

        // Insert a user
        const cpf = "12345678901   "; // 14 bytes
        const user_val = "user_struct_bytes_here";
        try txn.put(dbi_users, cpf, user_val, .{});

        // Insert an account
        const acc_id = "uuid-acc-0000001"; // 16 bytes
        const acc_val = "account_struct_bytes";
        try txn.put(dbi_accounts, acc_id, acc_val, .{});

        // Secondary index: user → account
        try txn.put(dbi_by_user, cpf, acc_id, .{});

        try txn.commit();
    }

    // Verify all 3 DBIs in a read txn
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();

    const dbi_users    = try txn.openDbi("users",            .{});
    const dbi_accounts = try txn.openDbi("accounts",         .{});
    const dbi_by_user  = try txn.openDbi("accounts_by_user", .{ .dupsort = true });

    // Verify user
    const user = try txn.get(dbi_users, "12345678901   ");
    try std.testing.expect(user != null);
    try std.testing.expectEqualStrings("user_struct_bytes_here", user.?);

    // Verify account
    const acc = try txn.get(dbi_accounts, "uuid-acc-0000001");
    try std.testing.expect(acc != null);

    // JOIN: user → accounts (via secondary index)
    var cur = try txn.cursor(dbi_by_user);
    defer cur.close();

    const found = try cur.find("12345678901   ");
    try std.testing.expect(found);

    const kv = try cur.current();
    try std.testing.expect(kv != null);
    // The value is the account_id — use for lookup in dbi_accounts
    try std.testing.expectEqualStrings("uuid-acc-0000001", kv.?.val);
}

test "multi_dbi: DBIs are isolated" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 16 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi_a = try txn.openDbi("db_a", .{ .create = true });
        const dbi_b = try txn.openDbi("db_b", .{ .create = true });

        // Same key in different DBIs
        try txn.put(dbi_a, "key", "value_from_a", .{});
        try txn.put(dbi_b, "key", "value_from_b", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi_a = try txn.openDbi("db_a", .{});
    const dbi_b = try txn.openDbi("db_b", .{});

    const va = try txn.get(dbi_a, "key");
    const vb = try txn.get(dbi_b, "key");

    try std.testing.expect(va != null);
    try std.testing.expect(vb != null);
    try std.testing.expectEqualStrings("value_from_a", va.?);
    try std.testing.expectEqualStrings("value_from_b", vb.?);
    // Values differ despite the same key
    try std.testing.expect(!std.mem.eql(u8, va.?, vb.?));
}

test "multi_dbi: atomic transaction across multiple DBIs" {
    defer std.fs.cwd().deleteFile(PATH) catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 16 * 1024 * 1024);
    defer env.close();

    // Setup
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("primary", .{ .create = true });
        _ = try txn.openDbi("secondary", .{ .create = true, .dupsort = true });
        try txn.commit();
    }

    // Simulate abort: nothing should have been inserted
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        const dbi_p = try txn.openDbi("primary",   .{});
        const dbi_s = try txn.openDbi("secondary", .{ .dupsort = true });
        try txn.put(dbi_p, "k", "v", .{});
        try txn.put(dbi_s, "k", "v", .{});
        txn.abort(); // discard all changes
    }

    // Verify that the abort took effect
    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi_p = try txn.openDbi("primary", .{});
    const val = try txn.get(dbi_p, "k");
    try std.testing.expect(val == null);
}
