// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Snapshot Isolation — path-copying CoW MVCC correctness tests.
//!
//! These tests verify that concurrent readers always see a consistent,
//! immutable snapshot of the B-tree, even while writers are modifying it.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_snapshot_isolation.monolith";

fn cleanup() void {
    std.fs.cwd().deleteFile(PATH)         catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
}



test "snapshot_isolation: reader sees pre-write snapshot" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    // T1: insert initial data.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        try txn.put(dbi, "key1", "val_before", .{});
        try txn.put(dbi, "key2", "val_before", .{});
        try txn.commit();
    }

    // R: open a read-only snapshot pinning T1.
    var reader = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer reader.abort();
    const rdbi = try reader.openDbi("data", .{});

    // T2: overwrite both keys and add new ones.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        try txn.put(dbi, "key1", "val_after", .{});
        try txn.put(dbi, "key2", "val_after", .{});
        try txn.put(dbi, "key3", "val_new",   .{});
        try txn.commit();
    }

    // Reader must still see T1's snapshot — val_before, no key3.
    const v1 = try reader.get(rdbi, "key1");
    try std.testing.expect(v1 != null);
    try std.testing.expectEqualStrings("val_before", v1.?);

    const v2 = try reader.get(rdbi, "key2");
    try std.testing.expect(v2 != null);
    try std.testing.expectEqualStrings("val_before", v2.?);

    const v3 = try reader.get(rdbi, "key3");
    try std.testing.expect(v3 == null);
}


test "snapshot_isolation: new reader sees latest commit" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
    defer env.close();

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        try txn.put(dbi, "key1", "v1", .{});
        try txn.commit();
    }

    // Open and immediately close a reader (releases the old snapshot).
    {
        var r = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        r.abort();
    }

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        try txn.put(dbi, "key2", "v2", .{});
        try txn.commit();
    }

    var r2 = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer r2.abort();
    const dbi2 = try r2.openDbi("data", .{});

    // Both keys must be visible.
    try std.testing.expect(try r2.get(dbi2, "key1") != null);
    try std.testing.expect(try r2.get(dbi2, "key2") != null);
}



test "snapshot_isolation: all keys visible after multi-txn writes" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    const ROUNDS = 10;
    const PER_ROUND = 100;

    var round: u32 = 0;
    while (round < ROUNDS) : (round += 1) {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("kv", .{ .create = true });
        var i: u32 = 0;
        while (i < PER_ROUND) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], round, .little);
            std.mem.writeInt(u32, k[4..8], i, .little);
            try txn.put(dbi, &k, "x", .{});
        }
        try txn.commit();
    }

    var r = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer r.abort();
    const rdbi = try r.openDbi("kv", .{});

    round = 0;
    while (round < ROUNDS) : (round += 1) {
        var i: u32 = 0;
        while (i < PER_ROUND) : (i += 1) {
            var k: [8]u8 = undefined;
            std.mem.writeInt(u32, k[0..4], round, .little);
            std.mem.writeInt(u32, k[4..8], i, .little);
            const v = try r.get(rdbi, &k);
            try std.testing.expect(v != null);
        }
    }
}



test "snapshot_isolation: branch path-copy preserves snapshot" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 4, 64 * 1024 * 1024);
    defer env.close();

    // T1: 300 entries — forces multi-level B-tree.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("big", .{ .create = true });
        var i: u32 = 0;
        while (i < 300) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .big);
            try txn.put(dbi, &k, "old_val", .{});
        }
        try txn.commit();
    }

    // Pin the snapshot after T1.
    var reader = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer reader.abort();
    const rdbi = try reader.openDbi("big", .{});

    // T2: overwrite all 300 entries.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("big", .{ .create = true });
        var i: u32 = 0;
        while (i < 300) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .big);
            try txn.put(dbi, &k, "new_val", .{});
        }
        try txn.commit();
    }

    // Reader must still see "old_val" for every key.
    var i: u32 = 0;
    while (i < 300) : (i += 1) {
        var k: [4]u8 = undefined;
        std.mem.writeInt(u32, &k, i, .big);
        const v = try reader.get(rdbi, &k);
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("old_val", v.?);
    }
}
