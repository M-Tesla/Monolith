// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! txn.renameDbi() — rename a named DBI.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_rename_dbi.monolith";

test "renameDbi: basic rename, data is accessible under new name" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("old", .{ .create = true });
        try txn.put(dbi, "key", "val", .{});
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.renameDbi("old", "new");
        try txn.commit();
    }

    // Data is accessible under the new name.
    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const dbi = try rtxn.openDbi("new", .{});
        const v = try rtxn.get(dbi, "key");
        try std.testing.expectEqualStrings("val", v.?);
    }
}

test "renameDbi: old name no longer exists after rename" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("src", .{ .create = true });
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        try txn.renameDbi("src", "dst");
        try txn.commit();
    }

    // Opening "src" without create should fail.
    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        const result = rtxn.openDbi("src", .{});
        try std.testing.expectError(error.NotFound, result);
    }
}

test "renameDbi: NotFound when source does not exist" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    defer txn.abort();

    try std.testing.expectError(error.NotFound, txn.renameDbi("ghost", "anything"));
}

test "renameDbi: KeyExist when new name already exists" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("a", .{ .create = true });
        _ = try txn.openDbi("b", .{ .create = true });
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        defer txn.abort();
        try std.testing.expectError(error.KeyExist, txn.renameDbi("a", "b"));
    }
}

test "renameDbi: abort discards the rename" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 8, 1 << 20);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        _ = try txn.openDbi("orig", .{ .create = true });
        try txn.commit();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        try txn.renameDbi("orig", "renamed");
        txn.abort(); // discard
    }

    // "orig" must still exist.
    {
        var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer rtxn.abort();
        _ = try rtxn.openDbi("orig", .{});
    }
}
