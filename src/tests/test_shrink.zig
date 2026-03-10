// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
/// Tests for auto-shrink after commit (Fase 31).
///
/// Auto-shrink works when the mmap was grown via growth_step (over-allocation),
/// leaving a large gap between map.len and first_unallocated * PAGE_SIZE.
/// After the threshold is configured and a commit fires, the file is truncated.
const std = @import("std");
const m = @import("../lib.zig");

const PATH: [:0]const u8 = "test_shrink.monolith";

fn cleanup() void {
    std.fs.cwd().deleteFile(PATH) catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
}

test "shrink: file shrinks when threshold enabled after over-allocation" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 16, 32 * 1024 * 1024);
    defer env.close();

    // Phase 1: set growth_step but NO shrink threshold.
    // This causes resize to snap up to 512KB multiples but never auto-shrinks.
    env.setGeometry(.{ .growth_step = 512 * 1024 });

    // Insert keys — forces a resize from 16KB to 512KB (growth_step snap).
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("main", .{ .create = true });
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "data_value_here", .{});
        }
        try txn.commit();
    }

    // Capture the over-allocated size (no shrink happened since threshold=0).
    const size_at_peak = try env.file.getEndPos();
    // Verify the resize actually snapped to 512KB.
    try std.testing.expect(size_at_peak >= 512 * 1024);

    // Phase 2: enable shrink threshold.
    // Now deletion + commit will trigger auto-shrink.
    env.setGeometry(.{ .shrink_threshold = 128 * 1024 });

    // Delete all keys.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("main", .{});
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            txn.del(dbi, k, null) catch |err| {
                if (err == error.NotFound) continue;
                return err;
            };
        }
        try txn.commit();
    }

    // File should be significantly smaller than the 512KB peak.
    const size_after_shrink = try env.file.getEndPos();
    try std.testing.expect(size_after_shrink < size_at_peak);
}

test "shrink: file does not shrink when threshold is zero (disabled)" {
    cleanup();
    defer cleanup();

    // growth_step only, NO shrink_threshold.
    var env = try m.Environment.open(PATH, .{}, 16, 32 * 1024 * 1024);
    defer env.close();
    env.setGeometry(.{ .growth_step = 512 * 1024 });

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("main", .{ .create = true });
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "data", .{});
        }
        try txn.commit();
    }

    const size_after_insert = try env.file.getEndPos();
    try std.testing.expect(size_after_insert >= 512 * 1024); // grew to 512KB

    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("main", .{});
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            txn.del(dbi, k, null) catch |err| {
                if (err == error.NotFound) continue;
                return err;
            };
        }
        try txn.commit();
    }

    // Without shrink_threshold, file must NOT shrink from the 512KB multiple.
    const size_after_delete = try env.file.getEndPos();
    try std.testing.expectEqual(size_after_insert, size_after_delete);
}

test "shrink: data is intact after shrink" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 16, 32 * 1024 * 1024);
    defer env.close();

    // Insert 200 keys, shrink disabled during inserts.
    env.setGeometry(.{ .growth_step = 512 * 1024 });
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tbl", .{ .create = true });
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "keep", .{});
        }
        try txn.commit();
    }

    // Enable shrink and delete first 150 keys.
    env.setGeometry(.{ .shrink_threshold = 128 * 1024 });
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("tbl", .{});
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 150) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            txn.del(dbi, k, null) catch |err| {
                if (err == error.NotFound) continue;
                return err;
            };
        }
        try txn.commit();
    }

    // Verify remaining 50 keys are still readable after shrink.
    {
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("tbl", .{});
        var buf: [32]u8 = undefined;
        var i: u32 = 150;
        while (i < 200) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "k{:0>6}", .{i}) catch unreachable;
            const v = try txn.get(dbi, k);
            try std.testing.expect(v != null);
            try std.testing.expectEqualStrings("keep", v.?);
        }
    }
}
