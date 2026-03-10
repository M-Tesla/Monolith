// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Data Page Checksums.
//! Verifies that page checksums are stamped on commit and that corrupted
//! pages are detected by env.check().

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_checksums.monolith";

test "checksums: clean database reports no errors" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);

    // Write enough data to force at least one data page beyond the root.
    {
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .little);
            try txn.put(dbi, &k, "value_data_payload", .{});
        }
        try txn.commit();
    }

    const result = env.check();
    env.close();

    try std.testing.expectEqual(@as(u64, 0), result.errors);
    try std.testing.expect(result.pages_visited > 0);
}

test "checksums: corrupted byte detected by env.check()" {
    defer std.fs.cwd().deleteFile(PATH)         catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // 1. Write data and close cleanly.
    {
        var env = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
        var txn = try m.Transaction.begin(&env, null, .{});
        errdefer txn.abort();
        const dbi = try txn.openDbi("t", .{ .create = true });
        var i: u32 = 0;
        while (i < 100) : (i += 1) {
            var k: [4]u8 = undefined;
            std.mem.writeInt(u32, &k, i, .little);
            try txn.put(dbi, &k, "value_data_payload", .{});
        }
        try txn.commit();
        env.close();
    }

    // 2. Corrupt a byte in page 2 (first data page, body at offset 20).
    //    Offset = 2 * 4096 + 32  (well inside the page body).
    {
        const file = try std.fs.cwd().openFile(PATH, .{ .mode = .read_write });
        defer file.close();
        const corrupt_offset: u64 = 2 * 4096 + 32;
        var byte: [1]u8 = undefined;
        _ = try file.pread(&byte, corrupt_offset);
        byte[0] ^= 0xFF; // flip all bits
        _ = try file.pwrite(&byte, corrupt_offset);
    }

    // 3. Reopen and check — must report at least one error.
    var env2 = try m.Environment.open(PATH, .{}, 4, 16 * 1024 * 1024);
    defer env2.close();
    const result = env2.check();
    try std.testing.expect(result.errors > 0);
}
