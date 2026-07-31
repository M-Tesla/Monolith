// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Nested Transactions (Sub-Transactions)
//!
//! Scenarios covered:
//!   1. child commit: data becomes visible in parent txn and after root commit
//!   2. child abort:  data discarded, parent unchanged
//!   3. sequential children: each child sees the parent state after the previous one
//!   4. child of child (double nesting)
//!   5. child commit preserves pre-existing parent data

const std   = @import("std");
const lib   = @import("../lib.zig");
const Env   = lib.Environment;
const Txn   = lib.Transaction;
const DbFlags = lib.DbFlags;
const TxnFlags = lib.TxnFlags;

const tmp_dir = "test_nested_tmp";

fn openEnv() !Env {
    std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    try std.Io.Dir.cwd().createDirPath(std.Io.Threaded.global_single_threaded.io(), tmp_dir);
    return Env.open(tmp_dir ++ "/data.db", .{}, 16, 64 * 1024 * 1024);
}

test "nested: child commit propagates to parent" {
    var env = try openEnv();
    defer std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    defer env.close();

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();

    const dbi = try par.openDbi("t1", .{ .create = true });

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();

    const dbi_c = try child.openDbi("t1", .{});
    try child.put(dbi_c, "hello", "world", .{});
    try child.commit();

    // After child commit, data must be visible in parent.
    const v = (try par.get(dbi, "hello")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("world", v);

    try par.commit();

    // Verify after root commit.
    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("t1", .{});
    const rv = (try rtxn.get(rdbi, "hello")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("world", rv);
}

test "nested: child abort discards changes" {
    var env = try openEnv();
    defer std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    defer env.close();

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();

    const dbi = try par.openDbi("t2", .{ .create = true });
    try par.put(dbi, "existing", "value", .{});

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();

    const dbi_c = try child.openDbi("t2", .{});
    try child.put(dbi_c, "temp", "data", .{});
    child.abort();

    // "temp" must not exist in parent after child abort.
    const miss = try par.get(dbi, "temp");
    try std.testing.expect(miss == null);

    const v = (try par.get(dbi, "existing")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("value", v);

    par.abort();
}

test "nested: sequential children see cumulative state" {
    var env = try openEnv();
    defer std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    defer env.close();

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();

    const dbi = try par.openDbi("t3", .{ .create = true });

    // Child 1: insert "a"
    {
        var c1 = try Txn.begin(&env, &par, .{});
        const d = try c1.openDbi("t3", .{});
        try c1.put(d, "a", "1", .{});
        try c1.commit();
    }

    // Child 2: must see "a", inserts "b"
    {
        var c2 = try Txn.begin(&env, &par, .{});
        const d = try c2.openDbi("t3", .{});
        const va = (try c2.get(d, "a")) orelse return error.NotFound;
        try std.testing.expectEqualStrings("1", va);
        try c2.put(d, "b", "2", .{});
        try c2.commit();
    }

    // Parent sees both "a" and "b".
    const va = (try par.get(dbi, "a")) orelse return error.NotFound;
    const vb = (try par.get(dbi, "b")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("1", va);
    try std.testing.expectEqualStrings("2", vb);

    try par.commit();
}

test "nested: double nesting" {
    var env = try openEnv();
    defer std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    defer env.close();

    var root = try Txn.begin(&env, null, .{});
    errdefer root.abort();

    const dbi = try root.openDbi("t4", .{ .create = true });

    var child = try Txn.begin(&env, &root, .{});
    errdefer child.abort();
    {
        var grandchild = try Txn.begin(&env, &child, .{});
        const d = try grandchild.openDbi("t4", .{});
        try grandchild.put(d, "deep", "value", .{});
        try grandchild.commit();
    }
    // Child sees "deep" after grandchild commit.
    const dbi_c = try child.openDbi("t4", .{});
    const v = (try child.get(dbi_c, "deep")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("value", v);
    try child.commit();

    // Root sees "deep".
    const vr = (try root.get(dbi, "deep")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("value", vr);
    try root.commit();
}

test "nested: child commit preserves parent data" {
    var env = try openEnv();
    defer std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    defer env.close();

    // Pre-populate the DB.
    {
        var t = try Txn.begin(&env, null, .{});
        errdefer t.abort();
        const d = try t.openDbi("t5", .{ .create = true });
        try t.put(d, "pre", "exist", .{});
        try t.commit();
    }

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();
    const dbi = try par.openDbi("t5", .{});

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();
    const dbi_c = try child.openDbi("t5", .{});
    try child.put(dbi_c, "new", "data", .{});
    try child.commit();

    // Both keys must be visible in parent.
    const v1 = (try par.get(dbi, "pre")) orelse return error.NotFound;
    const v2 = (try par.get(dbi, "new")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("exist", v1);
    try std.testing.expectEqualStrings("data", v2);

    try par.commit();
}

test "nested: child delete propagates" {
    var env = try openEnv();
    defer std.Io.Dir.cwd().deleteTree(std.Io.Threaded.global_single_threaded.io(), tmp_dir) catch {};
    defer env.close();

    {
        var t = try Txn.begin(&env, null, .{});
        errdefer t.abort();
        const d = try t.openDbi("t6", .{ .create = true });
        try t.put(d, "gone", "bye", .{});
        try t.put(d, "stay", "here", .{});
        try t.commit();
    }

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();
    const dbi = try par.openDbi("t6", .{});

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();
    const dbi_c = try child.openDbi("t6", .{});
    try child.del(dbi_c, "gone", null);
    try child.commit();

    // "gone" removed, "stay" intact.
    try std.testing.expect((try par.get(dbi, "gone")) == null);
    const v = (try par.get(dbi, "stay")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("here", v);

    try par.commit();
}
