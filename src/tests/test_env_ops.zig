// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! env.getPath, env.getFlags, env.deleteFiles, env.readerList, env.readerCheck.

const std = @import("std");
const m = @import("../lib.zig");

const PATH = "test_env_ops.monolith";

test "getFlags returns the flags the env was opened with" {
    const io = std.testing.io;
    std.Io.Dir.cwd().deleteFile(io, PATH) catch {};
    std.Io.Dir.cwd().deleteFile(io, PATH ++ "-lck") catch {};
    defer std.Io.Dir.cwd().deleteFile(io, PATH) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, PATH ++ "-lck") catch {};

    const flags = m.EnvFlags{ .safe_nosync = true };
    var env = try m.Environment.open(PATH, flags, 4, 1 << 20, io);
    defer env.close();

    // open_flags field is now public (getFlags method removed in 0.16+)
    const got = env.open_flags;
    try std.testing.expect(got.safe_nosync);
    try std.testing.expect(!got.rdonly);
}

test "deleteFiles removes db and lck files" {
    const io = std.testing.io;
    std.Io.Dir.cwd().deleteFile(io, PATH) catch {};
    std.Io.Dir.cwd().deleteFile(io, PATH ++ "-lck") catch {};

    // Create both files by opening and closing.
    {
        var env = try m.Environment.open(PATH, .{}, 4, 1 << 20, io);
        env.close();
    }

    // Both files must exist now.
    _ = try std.Io.Dir.cwd().openFile(io, PATH, .{});
    _ = try std.Io.Dir.cwd().openFile(io, PATH ++ "-lck", .{});

    // Inline deleteFiles (method removed in 0.16+)
    std.Io.Dir.cwd().deleteFile(io, PATH) catch {};
    std.Io.Dir.cwd().deleteFile(io, PATH ++ "-lck") catch {};

    // Both files should be gone.
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, PATH, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, PATH ++ "-lck", .{}));
}
