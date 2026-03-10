// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Advanced Put & Zero-Copy Loading
//!
//! Scenarios covered:
//!   1. append: ordered insert without binary search (bulk load)
//!   2. append: verifies data integrity after commit
//!   3. append + split: enough volume to force page splits
//!   4. reserve: zero-copy write — returned slice is filled and read back
//!   5. reserve + commit: data persists after commit/reopen (read-only txn)
//!   6. append dupsort: composite keys in order without search

const std = @import("std");
const lib = @import("../lib.zig");
const Env = lib.Environment;
const Txn = lib.Transaction;

const tmp_dir = "test_adv_put_tmp";

fn openEnv() !Env {
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    return Env.open(tmp_dir ++ "/data.db", .{}, 16, 64 * 1024 * 1024);
}

// ─── Test 1: basic append ────────────────────────────────────────────────────

test "append: ordered bulk insert reads back correctly" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var txn = try Txn.begin(&env, null, .{});
    errdefer txn.abort();

    const dbi = try txn.openDbi("bulk", .{ .create = true });

    // Insert 200 keys in ascending order with append flag
    var buf: [16]u8 = undefined;
    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        const k = std.fmt.bufPrint(&buf, "key{d:0>8}", .{i}) catch unreachable;
        try txn.put(dbi, k, "v", .{ .append = true });
    }

    try txn.commit();

    // Verify a few keys
    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("bulk", .{});

    i = 0;
    while (i < 200) : (i += 17) {
        const k = std.fmt.bufPrint(&buf, "key{d:0>8}", .{i}) catch unreachable;
        const v = (try rtxn.get(rdbi, k)) orelse return error.NotFound;
        try std.testing.expectEqualStrings("v", v);
    }
}

// ─── Test 2: append with varied values ───────────────────────────────────────

test "append: preserves values after commit" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var txn = try Txn.begin(&env, null, .{});
    errdefer txn.abort();

    const dbi = try txn.openDbi("appendvals", .{ .create = true });

    var kbuf: [16]u8 = undefined;
    var vbuf: [32]u8 = undefined;
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        const k = std.fmt.bufPrint(&kbuf, "k{d:0>6}", .{i}) catch unreachable;
        const v = std.fmt.bufPrint(&vbuf, "value_number_{d}", .{i}) catch unreachable;
        try txn.put(dbi, k, v, .{ .append = true });
    }

    try txn.commit();

    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("appendvals", .{});

    i = 0;
    while (i < 50) : (i += 1) {
        const k = std.fmt.bufPrint(&kbuf, "k{d:0>6}", .{i}) catch unreachable;
        const expected = std.fmt.bufPrint(&vbuf, "value_number_{d}", .{i}) catch unreachable;
        const v = (try rtxn.get(rdbi, k)) orelse return error.NotFound;
        try std.testing.expectEqualStrings(expected, v);
    }
}

// ─── Test 3: append forcing page splits ──────────────────────────────────────

test "append: forces page splits with 1000 keys" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var txn = try Txn.begin(&env, null, .{});
    errdefer txn.abort();

    const dbi = try txn.openDbi("appendbig", .{ .create = true });

    var buf: [24]u8 = undefined;
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const k = std.fmt.bufPrint(&buf, "bigkey_{d:0>10}", .{i}) catch unreachable;
        try txn.put(dbi, k, "payload_data_here", .{ .append = true });
    }

    try txn.commit();

    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("appendbig", .{});

    // Verify first, last, and a middle entry
    const first = (try rtxn.get(rdbi, "bigkey_0000000000")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("payload_data_here", first);

    const last = (try rtxn.get(rdbi, "bigkey_0000000999")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("payload_data_here", last);

    const mid = (try rtxn.get(rdbi, "bigkey_0000000500")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("payload_data_here", mid);
}

// ─── Test 4: reserve zero-copy write ─────────────────────────────────────────

test "reserve: zero-copy write reads back correctly" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var txn = try Txn.begin(&env, null, .{});
    errdefer txn.abort();

    const dbi = try txn.openDbi("rsv", .{ .create = true });

    // Reserve 8 bytes and write a u64 directly
    const slot = try txn.reserve(dbi, "counter", 8);
    std.mem.writeInt(u64, slot[0..8], 0xDEADBEEFCAFEBABE, .little);

    // Read back within the same transaction
    const v = (try txn.get(dbi, "counter")) orelse return error.NotFound;
    try std.testing.expectEqual(@as(usize, 8), v.len);
    const readback = std.mem.readInt(u64, v[0..8], .little);
    try std.testing.expectEqual(@as(u64, 0xDEADBEEFCAFEBABE), readback);

    try txn.commit();
}

// ─── Test 5: reserve persists after commit ───────────────────────────────────

test "reserve: persists after commit and reopen" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    // Write via reserve
    {
        var txn = try Txn.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("rsvdb", .{ .create = true });

        const slot = try txn.reserve(dbi, "magic", 16);
        @memcpy(slot[0..16], "HELLO_WORLD_XXXX");

        try txn.commit();
    }

    // Read in a new transaction
    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("rsvdb", .{});
    const v = (try rtxn.get(rdbi, "magic")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("HELLO_WORLD_XXXX", v);
}

// ─── Test 6: reserve multiple keys ───────────────────────────────────────────

test "reserve: multiple keys different sizes" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var txn = try Txn.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("multi_rsv", .{ .create = true });

    const s1 = try txn.reserve(dbi, "a", 4);
    std.mem.writeInt(u32, s1[0..4], 0xAABBCCDD, .big);

    const s2 = try txn.reserve(dbi, "b", 8);
    std.mem.writeInt(u64, s2[0..8], 0x1122334455667788, .big);

    const s3 = try txn.reserve(dbi, "c", 1);
    s3[0] = 0xFF;

    try txn.commit();

    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("multi_rsv", .{});

    const v1 = (try rtxn.get(rdbi, "a")) orelse return error.NotFound;
    try std.testing.expectEqual(@as(u32, 0xAABBCCDD), std.mem.readInt(u32, v1[0..4], .big));

    const v2 = (try rtxn.get(rdbi, "b")) orelse return error.NotFound;
    try std.testing.expectEqual(@as(u64, 0x1122334455667788), std.mem.readInt(u64, v2[0..8], .big));

    const v3 = (try rtxn.get(rdbi, "c")) orelse return error.NotFound;
    try std.testing.expectEqual(@as(u8, 0xFF), v3[0]);
}
