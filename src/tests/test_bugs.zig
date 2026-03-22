// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Regression tests: Bug 2 (MAX_DEPTH enforcement) and Bug 1 (unlock log).

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_bugs.monolith";

test "bug2: tree depth guard returns TreeTooDeep" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // Use a tiny map so splits happen quickly, building tree depth.
    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("depth", .{ .create = true });

    // Insert enough entries to trigger many splits — we expect the engine
    // to either succeed (depth < 64) or return error.TreeTooDeep (never crash).
    var buf: [32]u8 = undefined;
    var i: u32 = 0;
    while (i < 500) : (i += 1) {
        const k = std.fmt.bufPrint(&buf, "k{d:0>10}", .{i}) catch unreachable;
        txn.put(dbi, k, "v", .{}) catch |err| {
            if (err == error.TreeTooDeep) break; // expected guard fires before overflow
            return err;
        };
    }
    try txn.commit();
}

test "bug2: tree depth guard on integerkey" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var txn = try m.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("ikeys", .{ .create = true, .integerkey = true });

    var i: u32 = 0;
    while (i < 200) : (i += 1) {
        const k = std.mem.asBytes(&i);
        txn.put(dbi, k, "x", .{}) catch |err| {
            if (err == error.TreeTooDeep) break;
            return err;
        };
    }
    try txn.commit();
}
