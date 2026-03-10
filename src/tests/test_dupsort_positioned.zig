// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! getBoth / getBothRange / NODUPDATA.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_dupsort_positioned.monolith";

test "getBoth: exact (key,val) hit returns true" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("d", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "v1", .{});
        try txn.put(dbi, "k", "v2", .{});
        try txn.put(dbi, "k", "v3", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("d", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    try std.testing.expect(try cur.getBoth("k", "v2"));
    try std.testing.expect(try cur.getBoth("k", "missing") == false);
}

test "getBothRange: lower-bound on val" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("d", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "k", "aaa", .{});
        try txn.put(dbi, "k", "ccc", .{});
        try txn.put(dbi, "k", "eee", .{});
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("d", .{ .dupsort = true });
    var cur = try txn.cursor(dbi);
    defer cur.close();

    // Exact hit.
    const v1 = try cur.getBothRange("k", "ccc");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("ccc", v1.?);

    // Lower-bound (between "aaa" and "ccc" → lands on "ccc").
    const v2 = try cur.getBothRange("k", "bbb");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("ccc", v2.?);

    // Beyond last → null.
    const v3 = try cur.getBothRange("k", "zzz");
    try std.testing.expect(v3 == null);

    // Wrong key → null.
    const v4 = try cur.getBothRange("no_such_key", "aaa");
    try std.testing.expect(v4 == null);
}

test "nodupdata: rejects duplicate (key,val)" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 8 * 1024 * 1024);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("d", .{ .create = true, .dupsort = true });
    try txn.put(dbi, "k", "v", .{});
    // Second insert of same pair with NODUPDATA must fail.
    try std.testing.expectError(error.KeyExist,
        txn.put(dbi, "k", "v", .{ .nodupdata = true }));
    // Different value is fine.
    try txn.put(dbi, "k", "w", .{ .nodupdata = true });
    try txn.commit();
}
