// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! env.getPath, env.getFlags, env.deleteFiles, env.readerList, env.readerCheck.

const std = @import("std");
const m   = @import("../lib.zig");

const PATH = "test_env_ops.monolith";

test "getPath returns the path the env was opened with" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    const p = env.getPath();
    try std.testing.expectEqualStrings(PATH, p);
}

test "getFlags returns the flags the env was opened with" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    const flags = m.EnvFlags{ .safe_nosync = true };
    var env = try m.Environment.open(PATH, flags, 4, 1 << 20);
    defer env.close();

    const got = env.getFlags();
    try std.testing.expect(got.safe_nosync);
    try std.testing.expect(!got.rdonly);
}

test "deleteFiles removes db and lck files" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    // Create both files by opening and closing.
    {
        var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
        env.close();
    }

    // Both files must exist now.
    _ = try std.fs.cwd().openFile(PATH, .{});
    _ = try std.fs.cwd().openFile(PATH ++ "-lck", .{});

    m.Environment.deleteFiles(PATH);

    // Both files should be gone.
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().openFile(PATH, .{}));
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().openFile(PATH ++ "-lck", .{}));
}

test "readerList iterates active reader slots" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    var rtxn = try m.Transaction.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();

    var count: u32 = 0;
    const Ctx = struct {
        fn cb(info: m.ReaderInfo, ctx: ?*anyopaque) void {
            _ = info;
            const c: *u32 = @ptrCast(@alignCast(ctx.?));
            c.* += 1;
        }
    };
    env.readerList(Ctx.cb, &count);
    try std.testing.expect(count >= 1);
}

test "readerCheck returns 0 when no dead slots" {
    std.fs.cwd().deleteFile(PATH)           catch {};
    std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};
    defer std.fs.cwd().deleteFile(PATH)           catch {};
    defer std.fs.cwd().deleteFile(PATH ++ "-lck") catch {};

    var env = try m.Environment.open(PATH, .{}, 4, 1 << 20);
    defer env.close();

    // No dead processes, so readerCheck should clear nothing.
    const cleared = env.readerCheck();
    try std.testing.expectEqual(@as(u32, 0), cleared);
}
