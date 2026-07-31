// Copyright (c) 2026 Marcelo Tesla
//
// TessDB LZ4 compression wrapper.
// Pure-Zig LZ4 integration via zig-lz4 (jedisct1).
//
// SPDX-License-Identifier: MIT

const std = @import("std");
const lz4 = @import("lz4");

/// Compress a raw payload using LZ4.
/// Returns a newly allocated compressed buffer. Caller owns the memory.
pub fn compressLZ4(allocator: std.mem.Allocator, raw: []const u8) ![]u8 {
    if (raw.len == 0) {
        return allocator.alloc(u8, 0);
    }
    const max_compressed = lz4.compressBound(raw.len);
    var compressed = try allocator.alloc(u8, max_compressed);
    errdefer allocator.free(compressed);
    const compressed_size = try lz4.compressDefault(raw, compressed);
    // Shrink to actual size
    if (compressed_size < compressed.len) {
        const shrunk = try allocator.realloc(compressed, compressed_size);
        compressed = shrunk;
    }
    return compressed;
}

/// Decompress an LZ4-compressed payload.
/// `decompressed_size` must be the original uncompressed size.
/// Returns a newly allocated buffer. Caller owns the memory.
pub fn decompressLZ4(allocator: std.mem.Allocator, compressed: []const u8, decompressed_size: usize) ![]u8 {
    if (compressed.len == 0 or decompressed_size == 0) {
        return allocator.alloc(u8, 0);
    }
    const decompressed = try allocator.alloc(u8, decompressed_size);
    errdefer allocator.free(decompressed);
    const actual_size = try lz4.decompressSafe(compressed, decompressed);
    if (actual_size != decompressed_size) {
        return error.SizeMismatch;
    }
    return decompressed;
}

// ─── Tests ─────────────────────────────────────────────────────────────────

test "LZ4 round-trip" {
    const allocator = std.testing.allocator;
    const raw = "Hello, World! This is a test of TessDB LZ4 compression.";

    const compressed = try compressLZ4(allocator, raw);
    defer allocator.free(compressed);

    try std.testing.expect(compressed.len <= raw.len + 16);

    const decompressed = try decompressLZ4(allocator, compressed, raw.len);
    defer allocator.free(decompressed);

    try std.testing.expectEqualStrings(raw, decompressed);
}

test "LZ4 empty input" {
    const allocator = std.testing.allocator;
    const compressed = try compressLZ4(allocator, "");
    defer allocator.free(compressed);
    try std.testing.expectEqual(@as(usize, 0), compressed.len);

    const decompressed = try decompressLZ4(allocator, compressed, 0);
    defer allocator.free(decompressed);
    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}

test "LZ4 repeated pattern compresses well" {
    const allocator = std.testing.allocator;
    const raw = try allocator.alloc(u8, 1000);
    defer allocator.free(raw);
    @memset(raw, 'A');

    const compressed = try compressLZ4(allocator, raw);
    defer allocator.free(compressed);

    // Repeated pattern should compress significantly
    try std.testing.expect(compressed.len < raw.len);

    const decompressed = try decompressLZ4(allocator, compressed, raw.len);
    defer allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, raw, decompressed);
}
