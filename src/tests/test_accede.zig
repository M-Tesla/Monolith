// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
/// Tests for accede flag — open DBI without specifying flags (Fase 36).
const std = @import("std");
const m   = @import("../lib.zig");

const PATH: [:0]const u8 = "test_accede.monolith";

fn cleanup() void {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
}

test "accede: reopens dupsort DBI without specifying flags" {
    cleanup();
    defer cleanup();

    // Create a dupsort DBI.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 4 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "a", "2", .{});
        try txn.commit();
    }

    // Reopen with accede=true — must NOT return error.Incompatible.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 4 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        // accede skips flag validation — accepts stored dupsort=true.
        const dbi = try txn.openDbi("dup", .{ .accede = true });
        // Data must be intact.
        const v = try txn.get(dbi, "a");
        try std.testing.expect(v != null);
    }
}

test "accede: without accede flag, mismatched flags return Incompatible" {
    cleanup();
    defer cleanup();

    // Create with dupsort.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 4 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("dup", .{ .create = true, .dupsort = true });
        try txn.put(dbi, "x", "y", .{});
        try txn.commit();
    }

    // Reopen without dupsort flag and without accede → must fail.
    {
        var env = try m.Environment.open(PATH, .{}, 16, 4 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const result = txn.openDbi("dup", .{});
        try std.testing.expectError(error.Incompatible, result);
    }
}

test "accede: works for plain (non-dupsort) DBI too" {
    cleanup();
    defer cleanup();

    {
        var env = try m.Environment.open(PATH, .{}, 16, 4 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("plain", .{ .create = true });
        try txn.put(dbi, "k", "v", .{});
        try txn.commit();
    }

    {
        var env = try m.Environment.open(PATH, .{}, 16, 4 * 1024 * 1024);
        defer env.close();
        var txn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
        defer txn.abort();
        const dbi = try txn.openDbi("plain", .{ .accede = true });
        const v = try txn.get(dbi, "k");
        try std.testing.expect(v != null);
        try std.testing.expectEqualStrings("v", v.?);
    }
}
