// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Atomic Sequences, Hot Backup, Deep Audit, B-Tree Statistics.

const std = @import("std");
const monolith = @import("../lib.zig");

const Environment = monolith.Environment;
const Transaction = monolith.Transaction;

// ─────────────────────────────────────────────────────────────────────────────
// Atomic Sequences
// ─────────────────────────────────────────────────────────────────────────────

test "sequence: increments and returns old value" {
    const path = "test_seq_basic.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("mydb", .{ .create = true });

    // Initial sequence is 0.
    try std.testing.expectEqual(@as(u64, 0), try txn.sequence(dbi, 0));

    // First increment: returns 0, counter becomes 1.
    try std.testing.expectEqual(@as(u64, 0), try txn.sequence(dbi, 1));

    // Second increment by 5: returns 1, counter becomes 6.
    try std.testing.expectEqual(@as(u64, 1), try txn.sequence(dbi, 5));

    // Read without modifying: returns 6.
    try std.testing.expectEqual(@as(u64, 6), try txn.sequence(dbi, 0));

    try txn.commit();
}

test "sequence: persists across commit and reopen" {
    const path = "test_seq_persist.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        defer env.close();

        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("counter", .{ .create = true });
        _ = try txn.sequence(dbi, 42);
        try txn.commit();
    }

    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        defer env.close();

        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("counter", .{});
        // Sequence should still be 42 after reopen.
        try std.testing.expectEqual(@as(u64, 42), try txn.sequence(dbi, 0));
    }
}

test "sequence: read-only transaction cannot increment" {
    const path = "test_seq_rdonly.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        defer env.close();
        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("seq", .{ .create = true });
        _ = try txn.sequence(dbi, 10);
        try txn.commit();
    }

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var rtxn = try Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("seq", .{});

    // Increment must fail in a read-only transaction.
    try std.testing.expectError(error.BadTxn, rtxn.sequence(rdbi, 1));

    // But reading (increment=0) is allowed.
    try std.testing.expectEqual(@as(u64, 10), try rtxn.sequence(rdbi, 0));
}

// ─────────────────────────────────────────────────────────────────────────────
// B-Tree Statistics
// ─────────────────────────────────────────────────────────────────────────────

test "dbiStat: reflects items after insertions" {
    const path = "test_dbistat.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("data", .{ .create = true });

    var i: usize = 0;
    while (i < 50) : (i += 1) {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, @as(u64, i), .big);
        try txn.put(dbi, &key_buf, "value", .{});
    }

    const stat = try txn.dbiStat(dbi);
    try std.testing.expectEqual(@as(u32, 4096), stat.page_size);
    try std.testing.expectEqual(@as(u64, 50), stat.items);
    try std.testing.expect(stat.depth >= 1);
    try std.testing.expect(stat.leaf_pages >= 1);

    try txn.commit();
}

test "envStat: reports correct page count and txnid" {
    const path = "test_envstat.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    {
        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        try txn.put(dbi, "k", "v", .{});
        try txn.commit();
    }

    var txn = try Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();

    const es = txn.envStat();
    try std.testing.expectEqual(@as(u32, 4096), es.page_size);
    try std.testing.expect(es.total_pages >= 4);   // at least the 4 init pages
    try std.testing.expect(es.last_txnid >= 2);    // at least 2 commits happened
    try std.testing.expect(es.geo_upper_pages > 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Hot Backup
// ─────────────────────────────────────────────────────────────────────────────

test "copy: backup file is a valid database" {
    const src  = "test_copy_src.monolith";
    const dest = "test_copy_dst.monolith";
    defer std.fs.cwd().deleteFile(src)  catch {};
    defer std.fs.cwd().deleteFile(src  ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(dest) catch {};
    defer std.fs.cwd().deleteFile(dest ++ "-lck") catch {};

    // Write some data.
    {
        var env = try Environment.open(src, .{}, 4, 4 * 1024 * 1024);
        defer env.close();
        var txn = try Transaction.begin(&env, null, .{});
        defer txn.abort();
        const dbi = try txn.openDbi("backup_test", .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.commit();

        // Hot backup while the environment is still open.
        try env.copy(dest);
    }

    // Open the backup and verify data integrity.
    {
        var env2 = try Environment.open(dest, .{}, 4, 4 * 1024 * 1024);
        defer env2.close();
        var txn2 = try Transaction.begin(&env2, null, .{ .rdonly = true });
        defer txn2.abort();
        const dbi2 = try txn2.openDbi("backup_test", .{});
        const val = try txn2.get(dbi2, "hello");
        try std.testing.expect(val != null);
        try std.testing.expectEqualSlices(u8, "world", val.?);
    }
}

test "copy: backup size equals source allocated pages" {
    const src  = "test_copy_size_src.monolith";
    const dest = "test_copy_size_dst.monolith";
    defer std.fs.cwd().deleteFile(src)  catch {};
    defer std.fs.cwd().deleteFile(src  ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(dest) catch {};
    defer std.fs.cwd().deleteFile(dest ++ "-lck") catch {};

    var env = try Environment.open(src, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("sz", .{ .create = true });
    try txn.put(dbi, "k", "v", .{});
    try txn.commit();

    const num_pages = env.bestMeta().geometry.first_unallocated;
    try env.copy(dest);

    const dest_file = try std.fs.cwd().openFile(dest, .{});
    defer dest_file.close();
    const dest_size = try dest_file.getEndPos();

    try std.testing.expectEqual(@as(u64, num_pages) * 4096, dest_size);
}

// ─────────────────────────────────────────────────────────────────────────────
// Deep Audit
// ─────────────────────────────────────────────────────────────────────────────

test "check: fresh database has zero errors" {
    const path = "test_check_fresh.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    const result = env.check();
    try std.testing.expectEqual(@as(u64, 0), result.errors);
    try std.testing.expect(result.pages_visited >= 2); // at least pages 2 and 3
}

test "check: database with data has zero errors" {
    const path = "test_check_data.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    // Insert enough data to cause page splits.
    var txn = try Transaction.begin(&env, null, .{});
    defer txn.abort();
    const dbi = try txn.openDbi("audit", .{ .create = true });
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        var key_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &key_buf, @as(u64, i), .big);
        try txn.put(dbi, &key_buf, "payload", .{});
    }
    try txn.commit();

    const result = env.check();
    try std.testing.expectEqual(@as(u64, 0), result.errors);
    try std.testing.expect(result.pages_visited >= 4);
}
