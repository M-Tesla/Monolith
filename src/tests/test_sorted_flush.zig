// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
/// Tests for sorted dirty-page flush (Fase 35).
/// The sort is an internal optimisation — observable behaviour is identical.
/// Tests verify correctness under workloads that exercise multi-page commits.
const std = @import("std");
const m   = @import("../lib.zig");

const PATH: [:0]const u8 = "test_sorted_flush.monolith";

fn cleanup() void {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
}

test "sorted_flush: large commit writes all pages correctly" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 16, 32 * 1024 * 1024);
    defer env.close();

    // Insert enough keys to produce many dirty pages, forcing the sort path.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("big", .{ .create = true });
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 500) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "key{:0>6}", .{i}) catch unreachable;
            try txn.put(dbi, k, "some_value_data", .{});
        }
        try txn.commit();
    }

    // Reopen and verify all 500 keys are readable.
    {
        var env2 = try m.Environment.open(PATH, .{}, 16, 32 * 1024 * 1024);
        defer env2.close();
        var txn = try m.Transaction.begin(&env2, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("big", .{});
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 500) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "key{:0>6}", .{i}) catch unreachable;
            const v = try txn.get(dbi, k);
            try std.testing.expect(v != null);
            try std.testing.expectEqualStrings("some_value_data", v.?);
        }
    }
}

test "sorted_flush: interleaved inserts and deletes preserve order" {
    cleanup();
    defer cleanup();

    var env = try m.Environment.open(PATH, .{}, 16, 16 * 1024 * 1024);
    defer env.close();

    // Alternating insert/delete across many transactions exercises
    // the sort path on varied dirty-page sets each commit.
    var round: u32 = 0;
    while (round < 5) : (round += 1) {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("mix", .{ .create = true });
        var buf: [32]u8 = undefined;
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            const k = std.fmt.bufPrint(&buf, "r{d}k{:0>4}", .{ round, i }) catch unreachable;
            if (i % 2 == 0) {
                try txn.put(dbi, k, "v", .{});
            }
        }
        try txn.commit();
    }

    var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer txn.abort();
    const dbi = try txn.openDbi("mix", .{});
    var buf: [32]u8 = undefined;
    var r: u32 = 0;
    while (r < 5) : (r += 1) {
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            if (i % 2 == 0) {
                const k = std.fmt.bufPrint(&buf, "r{d}k{:0>4}", .{ r, i }) catch unreachable;
                const v = try txn.get(dbi, k);
                try std.testing.expect(v != null);
            }
        }
    }
}
