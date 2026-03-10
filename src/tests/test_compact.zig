// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! env.copyCompact() — compacting backup.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH    = "test_compact_src.monolith";
const COMPACT = "test_compact_dst.monolith";

test "copyCompact: all live keys readable after compact" {
    defer std.fs.cwd().deleteFile(PATH)            catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck")  catch {};
    defer std.fs.cwd().deleteFile(COMPACT)         catch {};
    defer std.fs.cwd().deleteFile(COMPACT ++ "-lck") catch {};

    // 1. Insert 500 keys, then delete 250.
    {
        var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
        errdefer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("data", .{ .create = true });
        var i: u32 = 0;
        while (i < 500) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .little);
            try txn.put(dbi, &k, "value_payload_data", .{});
        }
        // Delete odd-numbered keys (250 of them).
        i = 1;
        while (i < 500) : (i += 2) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .little);
            try txn.del(dbi, &k, null);
        }
        try txn.commit();
        env.close();
    }

    // 2. Compact.
    {
        var env = try m.Environment.open(PATH, .{}, 4, 32 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        try txn.copyCompact(COMPACT);
    }

    // 3. Compact file must be smaller than the original.
    const src_size = blk: {
        const f = try std.fs.cwd().openFile(PATH, .{});
        defer f.close();
        break :blk try f.getEndPos();
    };
    const dst_size = blk: {
        const f = try std.fs.cwd().openFile(COMPACT, .{});
        defer f.close();
        break :blk try f.getEndPos();
    };
    try std.testing.expect(dst_size < src_size);

    // 4. Open compact file and verify all 250 even keys are present.
    var env2 = try m.Environment.open(COMPACT, .{}, 4, 32 * 1024 * 1024);
    defer env2.close();
    var txn2 = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
    defer txn2.abort();
    const dbi2 = try txn2.openDbi("data", .{});

    var i: u32 = 0;
    while (i < 500) : (i += 2) {
        var k: [4]u8 = undefined;
        std.mem.writeInt(u32, &k, i, .little);
        const v = try txn2.get(dbi2, &k);
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("value_payload_data", v.?);
    }
    // Odd keys must be absent.
    i = 1;
    while (i < 500) : (i += 2) {
        var k: [4]u8 = undefined;
        std.mem.writeInt(u32, &k, i, .little);
        try std.testing.expect((try txn2.get(dbi2, &k)) == null);
    }
}

test "copyCompact: multiple DBIs all copied correctly" {
    defer std.fs.cwd().deleteFile(PATH)            catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck")  catch {};
    defer std.fs.cwd().deleteFile(COMPACT)         catch {};
    defer std.fs.cwd().deleteFile(COMPACT ++ "-lck") catch {};

    {
        var env = try m.Environment.open(PATH, .{}, 8, 16 * 1024 * 1024);
        errdefer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi_a = try txn.openDbi("alpha", .{ .create = true });
        const dbi_b = try txn.openDbi("beta",  .{ .create = true });
        try txn.put(dbi_a, "x", "1", .{});
        try txn.put(dbi_a, "y", "2", .{});
        try txn.put(dbi_b, "p", "A", .{});
        try txn.put(dbi_b, "q", "B", .{});
        try txn.commit();
        env.close();
    }

    {
        var env = try m.Environment.open(PATH, .{}, 8, 16 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        try txn.copyCompact(COMPACT);
    }

    var env2 = try m.Environment.open(COMPACT, .{}, 8, 16 * 1024 * 1024);
    defer env2.close();
    var txn2 = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
    defer txn2.abort();

    const dbi_a = try txn2.openDbi("alpha", .{});
    try std.testing.expectEqualStrings("1", (try txn2.get(dbi_a, "x")).?);
    try std.testing.expectEqualStrings("2", (try txn2.get(dbi_a, "y")).?);

    const dbi_b = try txn2.openDbi("beta", .{});
    try std.testing.expectEqualStrings("A", (try txn2.get(dbi_b, "p")).?);
    try std.testing.expectEqualStrings("B", (try txn2.get(dbi_b, "q")).?);
}
