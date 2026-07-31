// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Column Store Integer & String Encoding — delta, bit-pack, RLE, dictionary.
//!
//! Pipeline explored: raw → delta → zigzag → bit-pack | RLE
//! The encoder tries multiple strategies and picks the smallest payload.
//! Decode is deterministic based on the EncodingFlags.

const std = @import("std");

/// Compact flags (1 byte) indicating which transforms were applied.
pub const EncodingFlags = packed struct(u8) {
    delta: bool = false,
    bitpack: bool = false,
    rle: bool = false,
    dictionary: bool = false,
    reserved: u4 = 0,
};

// ─── Encoded payload wrapper ───────────────────────────────────────────────

pub const EncodedPayload = struct {
    bytes: []u8,
    flags: EncodingFlags,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *EncodedPayload) void {
        self.allocator.free(self.bytes);
    }
};

// ─── Zigzag (signed ↔ unsigned for bit packing) ──────────────────────────

pub fn zigzagEncodeI32(v: i32) u32 {
    return @as(u32, @bitCast((v << 1) ^ (v >> 31)));
}

pub fn zigzagDecodeI32(v: u32) i32 {
    const sign: u32 = 0 -% (v & 1);
    // Bitcast (not @intCast): extreme inputs like minInt(i32) produce
    // 0x80000000, which overflows i32 under a checked cast.
    return @bitCast((v >> 1) ^ sign);
}

// ─── Delta encoding ────────────────────────────────────────────────────────

/// out[0] = values[0], out[i] = values[i] - values[i-1].
/// `out` must have the same length as `values`.
pub fn deltaEncodeI32(values: []const i32, out: []i32) void {
    std.debug.assert(values.len == out.len);
    if (values.len == 0) return;
    out[0] = values[0];
    for (values[1..], 1..) |v, i| {
        out[i] = v - values[i - 1];
    }
}

/// In-place cumulative sum (inverse of delta). After the call out[i] = original[i].
pub fn deltaDecodeI32InPlace(buf: []i32) void {
    if (buf.len == 0) return;
    for (1..buf.len) |i| {
        buf[i] = buf[i] + buf[i - 1];
    }
}

// ─── Bit-width utilities ───────────────────────────────────────────────────

fn computeBitWidthU32(values: []const u32) u8 {
    var max: u32 = 0;
    for (values) |v| {
        if (v > max) max = v;
    }
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

fn bitPackedSize(count: usize, bit_width: u8) usize {
    if (bit_width == 0 or count == 0) return 0;
    const total_bits = count * bit_width;
    return (total_bits + 7) / 8;
}

// ─── Bit-packing (u32 → byte array) ────────────────────────────────────────

fn writeBits(dst: []u8, idx: usize, bit_width: u8, value: u32) void {
    if (bit_width == 0) return;
    const bit_offset = idx * bit_width;
    var byte_offset = bit_offset / 8;
    var shift: u3 = @intCast(bit_offset % 8);
    var bits_remaining = bit_width;
    var src_bits = value;

    while (bits_remaining > 0) {
        const bits_to_write = @min(bits_remaining, 8 - @as(usize, shift));
        const mask = (@as(u32, 1) << @intCast(bits_to_write)) - 1;
        const bits = src_bits & mask;
        dst[byte_offset] |= @as(u8, @intCast(bits)) << shift;
        src_bits >>= @intCast(bits_to_write);
        bits_remaining -= bits_to_write;
        byte_offset += 1;
        shift = 0;
    }
}

fn readBits(src: []const u8, idx: usize, bit_width: u8) u32 {
    if (bit_width == 0) return 0;
    const bit_offset = idx * bit_width;
    var byte_offset = bit_offset / 8;
    var shift: u3 = @intCast(bit_offset % 8);
    var bits_remaining = bit_width;
    var result: u32 = 0;
    var result_shift: u6 = 0;

    while (bits_remaining > 0) {
        const bits_to_read = @min(bits_remaining, 8 - @as(usize, shift));
        const mask = (@as(u32, 1) << @intCast(bits_to_read)) - 1;
        const bits = (src[byte_offset] >> shift) & @as(u8, @intCast(mask));
        result |= @as(u32, bits) << @intCast(result_shift);
        bits_remaining -= bits_to_read;
        byte_offset += 1;
        shift = 0;
        result_shift += @intCast(bits_to_read);
    }
    return result;
}

/// Pack an array of u32 values using the minimum number of bits per value.
/// Returns a newly allocated buffer. Caller owns the memory.
pub fn bitPackU32(allocator: std.mem.Allocator, values: []const u32) ![]u8 {
    const bit_width = computeBitWidthU32(values);
    const packed_len = bitPackedSize(values.len, bit_width);
    const packed_buf = try allocator.alloc(u8, packed_len);
    errdefer allocator.free(packed_buf);
    @memset(packed_buf, 0);

    for (values, 0..) |v, i| {
        writeBits(packed_buf, i, bit_width, v);
    }
    return packed_buf;
}

/// Unpack a bit-packed array back to u32 values.
/// Returns a newly allocated buffer. Caller owns the memory.
pub fn bitUnpackU32(
    allocator: std.mem.Allocator,
    packed_data: []const u8,
    count: u32,
    bit_width: u8,
) ![]u32 {
    if (bit_width == 0) {
        const zeros = try allocator.alloc(u32, count);
        @memset(zeros, 0);
        return zeros;
    }
    var values = try allocator.alloc(u32, count);
    errdefer allocator.free(values);
    for (0..count) |i| {
        values[i] = readBits(packed_data, i, bit_width);
    }
    return values;
}

// ─── RLE (Run-Length Encoding) for u32 ─────────────────────────────────────

pub fn rleEncodeU32(allocator: std.mem.Allocator, values: []const u32) ![]u8 {
    if (values.len == 0) {
        return allocator.alloc(u8, @sizeOf(u32));
    }

    // First pass: count runs
    var run_count: u32 = 1;
    var prev = values[0];
    for (values[1..]) |v| {
        if (v != prev) {
            run_count += 1;
            prev = v;
        }
    }

    const run_size = @sizeOf(u32) + @sizeOf(u32);
    var buf = try allocator.alloc(u8, @sizeOf(u32) + run_count * run_size);
    errdefer allocator.free(buf);
    @memset(buf, 0);

    std.mem.writeInt(u32, buf[0..4], run_count, .little);
    var offset: usize = 4;

    prev = values[0];
    var run_len: u32 = 1;
    for (values[1..]) |v| {
        if (v == prev) {
            run_len += 1;
        } else {
            std.mem.writeInt(u32, buf[offset..][0..4], prev, .little);
            offset += 4;
            std.mem.writeInt(u32, buf[offset..][0..4], run_len, .little);
            offset += 4;
            prev = v;
            run_len = 1;
        }
    }
    // Last run
    std.mem.writeInt(u32, buf[offset..][0..4], prev, .little);
    offset += 4;
    std.mem.writeInt(u32, buf[offset..][0..4], run_len, .little);
    offset += 4;

    return buf;
}

pub fn rleDecodeU32(
    allocator: std.mem.Allocator,
    encoded: []const u8,
    out_count: u32,
) ![]u32 {
    if (encoded.len < 4) return error.InvalidRleData;
    const run_count = std.mem.readInt(u32, encoded[0..4], .little);
    const expected_min_len = @sizeOf(u32) + run_count * (@sizeOf(u32) + @sizeOf(u32));
    if (encoded.len < expected_min_len) return error.InvalidRleData;

    var out = try allocator.alloc(u32, out_count);
    errdefer allocator.free(out);
    var offset: usize = 4;
    var out_idx: u32 = 0;

    for (0..run_count) |_| {
        const value = std.mem.readInt(u32, encoded[offset..][0..4], .little);
        offset += 4;
        const count = std.mem.readInt(u32, encoded[offset..][0..4], .little);
        offset += 4;
        if (out_idx + count > out_count) return error.RleOverflow;
        @memset(out[out_idx .. out_idx + count], value);
        out_idx += count;
    }

    if (out_idx != out_count) return error.RleCountMismatch;
    return out;
}

// ─── High-level INT32 encode/decode ────────────────────────────────────────

/// Try multiple encoding strategies and pick the smallest payload.
/// Never returns a payload larger than raw (unless OOM).
pub fn encodeInt32(allocator: std.mem.Allocator, values: []const i32) !EncodedPayload {
    if (values.len == 0) {
        return EncodedPayload{
            .bytes = try allocator.alloc(u8, 0),
            .flags = .{},
            .allocator = allocator,
        };
    }

    const raw_size = values.len * @sizeOf(i32);
    var best_size = raw_size;
    var best_flags = EncodingFlags{};
    var best_bytes: []u8 = try allocator.dupe(u8, std.mem.sliceAsBytes(values));

    // Pre-compute delta + zigzag (shared by bitpack and RLE strategies)
    const deltas = try allocator.alloc(i32, values.len);
    defer allocator.free(deltas);
    deltaEncodeI32(values, deltas);

    var zigzags = try allocator.alloc(u32, values.len);
    defer allocator.free(zigzags);
    for (deltas, 0..) |d, i| {
        zigzags[i] = zigzagEncodeI32(d);
    }

    // Strategy: delta + bitpack
    {
        const packed_buf = bitPackU32(allocator, zigzags) catch null;
        if (packed_buf) |p| {
            defer allocator.free(p);
            const header_size = @sizeOf(i32) + @sizeOf(u8) + @sizeOf(u32); // first + bit_width + count
            const total = header_size + p.len;
            if (total < best_size) {
                allocator.free(best_bytes);
                best_bytes = try allocator.alloc(u8, total);
                std.mem.writeInt(i32, best_bytes[0..4], values[0], .little);
                best_bytes[4] = computeBitWidthU32(zigzags);
                std.mem.writeInt(u32, best_bytes[5..9], @intCast(values.len), .little);
                @memcpy(best_bytes[header_size..], p);
                best_size = total;
                best_flags = .{ .delta = true, .bitpack = true };
            }
        }
    }

    // Strategy: delta + RLE
    {
        const rle = rleEncodeU32(allocator, zigzags) catch null;
        if (rle) |r| {
            defer allocator.free(r);
            const header_size = @sizeOf(i32); // first value only
            const total = header_size + r.len;
            if (total < best_size) {
                allocator.free(best_bytes);
                best_bytes = try allocator.alloc(u8, total);
                std.mem.writeInt(i32, best_bytes[0..4], values[0], .little);
                @memcpy(best_bytes[4..], r);
                best_size = total;
                best_flags = .{ .delta = true, .rle = true };
            }
        }
    }

    return EncodedPayload{
        .bytes = best_bytes,
        .flags = best_flags,
        .allocator = allocator,
    };
}

/// Decode an INT32 payload according to the encoding flags.
/// Returns a newly allocated `[]i32`. Caller owns the memory.
pub fn decodeInt32(
    allocator: std.mem.Allocator,
    encoded: []const u8,
    flags: EncodingFlags,
    count: u32,
) ![]i32 {
    if (count == 0) {
        return allocator.alloc(i32, 0);
    }

    // Raw
    if (!flags.delta and !flags.bitpack and !flags.rle) {
        if (encoded.len < count * @sizeOf(i32)) return error.InvalidEncodedData;
        const vals = std.mem.bytesAsSlice(i32, encoded[0..count * @sizeOf(i32)]);
        const result = try allocator.alloc(i32, count);
        errdefer allocator.free(result);
        @memcpy(result, vals);
        return result;
    }

    // Delta + Bitpack
    if (flags.delta and flags.bitpack) {
        if (encoded.len < @sizeOf(i32) + @sizeOf(u8) + @sizeOf(u32)) return error.InvalidEncodedData;
        const first_value = std.mem.readInt(i32, encoded[0..4], .little);
        const bit_width = encoded[4];
        const decoded_count = std.mem.readInt(u32, encoded[5..9], .little);
        if (decoded_count != count) return error.CountMismatch;

        const packed_data = encoded[9..];
        const zigzags = try bitUnpackU32(allocator, packed_data, count, bit_width);
        defer allocator.free(zigzags);

        var result = try allocator.alloc(i32, count);
        errdefer allocator.free(result);
        result[0] = first_value;
        for (1..count) |i| {
            const delta = zigzagDecodeI32(zigzags[i]);
            result[i] = result[i - 1] + delta;
        }
        return result;
    }

    // Delta + RLE
    if (flags.delta and flags.rle) {
        if (encoded.len < @sizeOf(i32)) return error.InvalidEncodedData;
        const first_value = std.mem.readInt(i32, encoded[0..4], .little);
        const rle_data = encoded[4..];

        const zigzags = try rleDecodeU32(allocator, rle_data, count);
        defer allocator.free(zigzags);

        var result = try allocator.alloc(i32, count);
        errdefer allocator.free(result);
        result[0] = first_value;
        for (1..count) |i| {
            const delta = zigzagDecodeI32(zigzags[i]);
            result[i] = result[i - 1] + delta;
        }
        return result;
    }

    // Delta only
    if (flags.delta and !flags.bitpack and !flags.rle) {
        if (encoded.len < @sizeOf(i32) + count * @sizeOf(i32)) return error.InvalidEncodedData;
        const first_value = std.mem.readInt(i32, encoded[0..4], .little);
        const deltas_slice = std.mem.bytesAsSlice(i32, encoded[4..4 + count * @sizeOf(i32)]);

        var result = try allocator.alloc(i32, count);
        errdefer allocator.free(result);
        result[0] = first_value;
        for (1..count) |i| {
            result[i] = result[i - 1] + deltas_slice[i];
        }
        return result;
    }

    return error.UnsupportedEncoding;
}

// ─── String encoding ───────────────────────────────────────────────────────

/// Encode an array of strings. Tries dictionary encoding when cardinality
/// is ≤ 50 % of row count; otherwise falls back to raw length-prefixed.
/// Returns an owned EncodedPayload. Caller must call `deinit`.
pub fn encodeString(allocator: std.mem.Allocator, values: []const []const u8) !EncodedPayload {
    if (values.len == 0) {
        return EncodedPayload{
            .bytes = try allocator.alloc(u8, 0),
            .flags = .{},
            .allocator = allocator,
        };
    }

    // Build dictionary and index mapping
    var dict_map = std.StringHashMap(u32).init(allocator);
    defer dict_map.deinit();

    const indices = try allocator.alloc(u32, values.len);
    defer allocator.free(indices);

    var dict_list = std.ArrayList([]const u8).empty;
    defer {
        for (dict_list.items) |s| allocator.free(s);
        dict_list.deinit(allocator);
    }

    for (values, 0..) |s, i| {
        if (dict_map.get(s)) |idx| {
            indices[i] = idx;
        } else {
            const idx: u32 = @intCast(dict_list.items.len);
            const owned = try allocator.dupe(u8, s);
            errdefer allocator.free(owned);
            try dict_list.append(allocator, owned);
            try dict_map.put(owned, idx);
            indices[i] = idx;
        }
    }

    const unique_count = dict_list.items.len;
    const cardinality_ratio = @as(f64, @floatFromInt(unique_count)) / @as(f64, @floatFromInt(values.len));

    // Dictionary threshold: 50 % cardinality
    if (cardinality_ratio <= 0.5) {
        // Build dictionary payload: [count][len][bytes]...
        var dict_payload_size: usize = @sizeOf(u32);
        for (dict_list.items) |s| {
            dict_payload_size += @sizeOf(u32) + s.len;
        }

        const index_bit_width = computeBitWidthU32(indices);
        const packed_indices = try bitPackU32(allocator, indices);
        defer allocator.free(packed_indices);

        const index_payload_size = @sizeOf(u8) + @sizeOf(u32) + packed_indices.len;
        const total = dict_payload_size + index_payload_size;
        var payload = try allocator.alloc(u8, total);
        errdefer allocator.free(payload);

        var offset: usize = 0;
        std.mem.writeInt(u32, payload[offset..][0..4], @intCast(unique_count), .little);
        offset += 4;
        for (dict_list.items) |s| {
            std.mem.writeInt(u32, payload[offset..][0..4], @intCast(s.len), .little);
            offset += 4;
            @memcpy(payload[offset..offset + s.len], s);
            offset += s.len;
        }
        payload[offset] = index_bit_width;
        offset += 1;
        std.mem.writeInt(u32, payload[offset..][0..4], @intCast(values.len), .little);
        offset += 4;
        @memcpy(payload[offset..], packed_indices);

        return EncodedPayload{
            .bytes = payload,
            .flags = .{ .dictionary = true },
            .allocator = allocator,
        };
    }

    // Fallback: raw length-prefixed strings
    var total: usize = 0;
    for (values) |s| {
        total += @sizeOf(u32) + s.len;
    }
    var payload = try allocator.alloc(u8, total);
    errdefer allocator.free(payload);

    var offset: usize = 0;
    for (values) |s| {
        std.mem.writeInt(u32, payload[offset..][0..4], @intCast(s.len), .little);
        offset += 4;
        @memcpy(payload[offset..offset + s.len], s);
        offset += s.len;
    }

    return EncodedPayload{
        .bytes = payload,
        .flags = .{},
        .allocator = allocator,
    };
}

/// Result of a zero-copy string block decode. `strings` are views either into
/// the original `encoded` buffer (raw) or into the `owned` contiguous buffer
/// (dictionary). Caller must call `deinit`.
pub const StringViewBlock = struct {
    strings: [][]const u8,
    /// Contiguous buffer holding dictionary data. Null for raw blocks (strings
    /// point directly into the encoded payload).
    owned: ?[]u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *StringViewBlock) void {
        self.allocator.free(self.strings);
        if (self.owned) |buf| self.allocator.free(buf);
    }
};

/// Decode a string payload into zero-copy views.
/// For dictionary blocks, allocates ONE contiguous buffer for the dictionary
/// and returns slices pointing into it.
/// For raw blocks, returns slices pointing directly into `encoded`.
pub fn decodeStringView(
    allocator: std.mem.Allocator,
    encoded: []const u8,
    flags: EncodingFlags,
    count: u32,
) !StringViewBlock {
    if (count == 0) {
        return StringViewBlock{
            .strings = try allocator.alloc([]const u8, 0),
            .owned = null,
            .allocator = allocator,
        };
    }

    // Raw (length-prefixed) — zero-copy views into encoded payload
    if (!flags.dictionary) {
        var strings = try allocator.alloc([]const u8, count);
        errdefer allocator.free(strings);
        var offset: usize = 0;
        for (0..count) |i| {
            if (offset + 4 > encoded.len) return error.InvalidEncodedData;
            const len = std.mem.readInt(u32, encoded[offset..][0..4], .little);
            offset += 4;
            if (offset + len > encoded.len) return error.InvalidEncodedData;
            strings[i] = encoded[offset..][0..len];
            offset += len;
        }
        return StringViewBlock{
            .strings = strings,
            .owned = null,
            .allocator = allocator,
        };
    }

    // Dictionary encoding
    if (encoded.len < @sizeOf(u32)) return error.InvalidEncodedData;
    const dict_count = std.mem.readInt(u32, encoded[0..4], .little);
    var offset: usize = 4;

    // First pass: calculate total dictionary bytes
    var dict_total: usize = 0;
    var scan_off = offset;
    for (0..dict_count) |_| {
        if (scan_off + 4 > encoded.len) return error.InvalidEncodedData;
        const len = std.mem.readInt(u32, encoded[scan_off..][0..4], .little);
        scan_off += 4 + len;
        dict_total += len;
    }

    // Allocate one contiguous buffer for the dictionary
    const dict_buf = try allocator.alloc(u8, dict_total);
    errdefer allocator.free(dict_buf);

    // Read dictionary into the buffer and record slice boundaries
    var dict_slices = try allocator.alloc([]const u8, dict_count);
    defer allocator.free(dict_slices);
    var buf_off: usize = 0;
    for (0..dict_count) |i| {
        const len = std.mem.readInt(u32, encoded[offset..][0..4], .little);
        offset += 4;
        @memcpy(dict_buf[buf_off..buf_off + len], encoded[offset..][0..len]);
        dict_slices[i] = dict_buf[buf_off..buf_off + len];
        offset += len;
        buf_off += len;
    }

    if (offset + 1 + @sizeOf(u32) > encoded.len) return error.InvalidEncodedData;
    const bit_width = encoded[offset];
    offset += 1;
    const index_count = std.mem.readInt(u32, encoded[offset..][0..4], .little);
    offset += 4;
    if (index_count != count) return error.CountMismatch;

    const packed_data = encoded[offset..];
    const indices = try bitUnpackU32(allocator, packed_data, count, bit_width);
    defer allocator.free(indices);

    var strings = try allocator.alloc([]const u8, count);
    errdefer allocator.free(strings);
    for (0..count) |i| {
        if (indices[i] >= dict_count) return error.InvalidDictionaryIndex;
        strings[i] = dict_slices[indices[i]];
    }

    return StringViewBlock{
        .strings = strings,
        .owned = dict_buf,
        .allocator = allocator,
    };
}

/// Decode a string payload. Returns an owned [][]const u8 where each inner
/// slice is also allocated. Caller must free the outer array AND each string.
/// This is the safe API for callers that need persistent strings.
pub fn decodeString(
    allocator: std.mem.Allocator,
    encoded: []const u8,
    flags: EncodingFlags,
    count: u32,
) ![][]const u8 {
    var view = try decodeStringView(allocator, encoded, flags, count);
    defer view.deinit();

    var result = try allocator.alloc([]const u8, count);
    errdefer allocator.free(result);
    for (0..count) |i| {
        result[i] = try allocator.dupe(u8, view.strings[i]);
    }
    return result;
}

// ─── Tests ─────────────────────────────────────────────────────────────────

test "zigzag round-trip" {
    const pairs = &[_]struct { i32, u32 }{
        .{ 0, 0 },
        .{ -1, 1 },
        .{ 1, 2 },
        .{ -2, 3 },
        .{ 2, 4 },
        .{ std.math.maxInt(i32), 0xFFFFFFFE },
        .{ std.math.minInt(i32), 0xFFFFFFFF },
    };
    for (pairs) |p| {
        try std.testing.expectEqual(p.@"1", zigzagEncodeI32(p.@"0"));
        try std.testing.expectEqual(p.@"0", zigzagDecodeI32(p.@"1"));
    }
}

test "delta encode/decode" {
    const allocator = std.testing.allocator;
    const values = &[_]i32{ 10, 12, 15, 20, 100 };
    const deltas = try allocator.alloc(i32, values.len);
    defer allocator.free(deltas);
    deltaEncodeI32(values, deltas);
    try std.testing.expectEqual(@as(i32, 10), deltas[0]);
    try std.testing.expectEqual(@as(i32, 2), deltas[1]);
    try std.testing.expectEqual(@as(i32, 3), deltas[2]);
    try std.testing.expectEqual(@as(i32, 5), deltas[3]);
    try std.testing.expectEqual(@as(i32, 80), deltas[4]);

    deltaDecodeI32InPlace(deltas);
    try std.testing.expectEqualSlices(i32, values, deltas);
}

test "bit-pack round-trip" {
    const allocator = std.testing.allocator;
    const values = &[_]u32{ 0, 1, 2, 3, 4, 5, 6, 7 };
    const packed_data = try bitPackU32(allocator, values);
    defer allocator.free(packed_data);
    // 3 bits each × 8 values = 24 bits = 3 bytes
    try std.testing.expectEqual(@as(usize, 3), packed_data.len);

    const unpacked = try bitUnpackU32(allocator, packed_data, @intCast(values.len), 3);
    defer allocator.free(unpacked);
    try std.testing.expectEqualSlices(u32, values, unpacked);
}

test "bit-pack zero bit-width" {
    const allocator = std.testing.allocator;
    const values = &[_]u32{ 0, 0, 0, 0 };
    const packed_data = try bitPackU32(allocator, values);
    defer allocator.free(packed_data);
    try std.testing.expectEqual(@as(usize, 0), packed_data.len);

    const unpacked = try bitUnpackU32(allocator, packed_data, @intCast(values.len), 0);
    defer allocator.free(unpacked);
    try std.testing.expectEqualSlices(u32, values, unpacked);
}

test "RLE round-trip" {
    const allocator = std.testing.allocator;
    const values = &[_]u32{ 1, 1, 1, 2, 2, 3, 3, 3, 3 };
    const encoded = try rleEncodeU32(allocator, values);
    defer allocator.free(encoded);

    const decoded = try rleDecodeU32(allocator, encoded, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(u32, values, decoded);
}

test "encodeInt32 sequential → delta+bitpack" {
    const allocator = std.testing.allocator;
    const values = &[_]i32{ 100, 101, 102, 103, 104, 105 };
    var encoded = try encodeInt32(allocator, values);
    defer encoded.deinit();

    // Must be smaller than raw (24 bytes)
    try std.testing.expect(encoded.bytes.len < values.len * @sizeOf(i32));

    const decoded = try decodeInt32(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(i32, values, decoded);
}

test "encodeInt32 repeated → RLE" {
    const allocator = std.testing.allocator;
    const values = &[_]i32{ 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 };
    var encoded = try encodeInt32(allocator, values);
    defer encoded.deinit();

    try std.testing.expect(encoded.bytes.len < values.len * @sizeOf(i32));

    const decoded = try decodeInt32(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(i32, values, decoded);
}

test "encodeInt32 random → raw fallback" {
    const allocator = std.testing.allocator;
    const values = &[_]i32{ 42, -999, 12345, 0, -1 };
    var encoded = try encodeInt32(allocator, values);
    defer encoded.deinit();

    const decoded = try decodeInt32(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(i32, values, decoded);
}

// ─── String encoding tests ─────────────────────────────────────────────────

test "encodeString dictionary → low cardinality" {
    const allocator = std.testing.allocator;
    const values = &[_][]const u8{ "apple", "banana", "apple", "banana", "apple", "cherry", "cherry" };
    var encoded = try encodeString(allocator, values);
    defer encoded.deinit();

    // 3 unique out of 7 → ~43 % cardinality, dictionary should win
    try std.testing.expect(encoded.flags.dictionary);

    const decoded = try decodeString(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer {
        for (decoded) |s| allocator.free(s);
        allocator.free(decoded);
    }
    try std.testing.expectEqualStrings("apple", decoded[0]);
    try std.testing.expectEqualStrings("banana", decoded[1]);
    try std.testing.expectEqualStrings("apple", decoded[2]);
    try std.testing.expectEqualStrings("banana", decoded[3]);
    try std.testing.expectEqualStrings("apple", decoded[4]);
    try std.testing.expectEqualStrings("cherry", decoded[5]);
    try std.testing.expectEqualStrings("cherry", decoded[6]);
}

test "encodeString raw → high cardinality" {
    const allocator = std.testing.allocator;
    const values = &[_][]const u8{ "a", "bb", "ccc", "dddd", "eeeee" };
    var encoded = try encodeString(allocator, values);
    defer encoded.deinit();

    // 5 unique out of 5 → 100 % cardinality, raw fallback
    try std.testing.expect(!encoded.flags.dictionary);

    const decoded = try decodeString(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer {
        for (decoded) |s| allocator.free(s);
        allocator.free(decoded);
    }
    try std.testing.expectEqualStrings("a", decoded[0]);
    try std.testing.expectEqualStrings("bb", decoded[1]);
    try std.testing.expectEqualStrings("ccc", decoded[2]);
    try std.testing.expectEqualStrings("dddd", decoded[3]);
    try std.testing.expectEqualStrings("eeeee", decoded[4]);
}

test "encodeString empty" {
    const allocator = std.testing.allocator;
    const values = &[_][]const u8{};
    var encoded = try encodeString(allocator, values);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(usize, 0), encoded.bytes.len);

    const decoded = try decodeString(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqual(@as(usize, 0), decoded.len);
}

test "encodeString repeated → dictionary" {
    const allocator = std.testing.allocator;
    const values = &[_][]const u8{ "x", "x", "x", "x", "x", "x", "x", "x", "x", "x" };
    var encoded = try encodeString(allocator, values);
    defer encoded.deinit();

    try std.testing.expect(encoded.flags.dictionary);
    // Should be way smaller than raw (10 × 5 bytes = 50 bytes of raw string data)
    try std.testing.expect(encoded.bytes.len < 30);

    const decoded = try decodeString(allocator, encoded.bytes, encoded.flags, @intCast(values.len));
    defer {
        for (decoded) |s| allocator.free(s);
        allocator.free(decoded);
    }
    for (decoded) |s| {
        try std.testing.expectEqualStrings("x", s);
    }
}

// ─── Float encoding (Gorilla / XOR delta) ──────────────────────────────────

/// Simple bit writer that appends bits to a byte buffer.
const BitWriter = struct {
    buf: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    current_byte: u8,
    bit_count: u4,

    pub fn init(allocator: std.mem.Allocator) BitWriter {
        return .{
            .buf = std.ArrayList(u8).empty,
            .allocator = allocator,
            .current_byte = 0,
            .bit_count = 0,
        };
    }

    pub fn deinit(self: *BitWriter) void {
        self.buf.deinit(self.allocator);
    }

    pub fn writeBit(self: *BitWriter, bit: u1) !void {
        self.current_byte = (self.current_byte << 1) | bit;
        self.bit_count += 1;
        if (self.bit_count == 8) {
            try self.buf.append(self.allocator, self.current_byte);
            self.current_byte = 0;
            self.bit_count = 0;
        }
    }

    pub fn writeBits(self: *BitWriter, value: u64, n_bits: u7) !void {
        var i = n_bits;
        while (i > 0) {
            i -= 1;
            const bit: u1 = @intCast((value >> @intCast(i)) & 1);
            try self.writeBit(bit);
        }
    }

    pub fn flush(self: *BitWriter) !void {
        if (self.bit_count > 0) {
            self.current_byte <<= @as(u3, @intCast(8 - @as(usize, self.bit_count)));
            try self.buf.append(self.allocator, self.current_byte);
            self.current_byte = 0;
            self.bit_count = 0;
        }
    }

    pub fn toOwnedSlice(self: *BitWriter) ![]u8 {
        try self.flush();
        return self.buf.toOwnedSlice(self.allocator);
    }
};

/// Simple bit reader that reads bits from a byte buffer.
const BitReader = struct {
    buf: []const u8,
    byte_idx: usize,
    bit_idx: u4, // 0 = MSB, 7 = LSB

    pub fn init(buf: []const u8) BitReader {
        return .{
            .buf = buf,
            .byte_idx = 0,
            .bit_idx = 0,
        };
    }

    pub fn readBit(self: *BitReader) !u1 {
        if (self.byte_idx >= self.buf.len) return error.EndOfStream;
        const bit: u1 = @intCast((self.buf[self.byte_idx] >> @intCast(7 - self.bit_idx)) & 1);
        self.bit_idx += 1;
        if (self.bit_idx == 8) {
            self.bit_idx = 0;
            self.byte_idx += 1;
        }
        return bit;
    }

    pub fn readBits(self: *BitReader, n_bits: u7) !u64 {
        var result: u64 = 0;
        for (0..n_bits) |_| {
            result = (result << 1) | @as(u64, try self.readBit());
        }
        return result;
    }
};

fn leadingZeros64(v: u64) u7 {
    if (v == 0) return 64;
    return @intCast(@clz(v));
}

fn significantBits64(v: u64) u7 {
    if (v == 0) return 0;
    const lz = leadingZeros64(v);
    const tz: u7 = @intCast(@ctz(v));
    return 64 - lz - tz;
}

/// Encode an array of f64 values using Gorilla XOR delta compression.
/// Returns an EncodedPayload. Caller must call `deinit`.
pub fn encodeFloat64(allocator: std.mem.Allocator, values: []const f64) !EncodedPayload {
    if (values.len == 0) {
        return EncodedPayload{
            .bytes = try allocator.alloc(u8, 0),
            .flags = .{},
            .allocator = allocator,
        };
    }

    // Raw size baseline
    const raw_size = values.len * @sizeOf(f64);

    var writer = BitWriter.init(allocator);
    defer writer.deinit();

    // Write first value as-is (64 bits in two 32-bit chunks)
    const first_bits = @as(u64, @bitCast(values[0]));
    try writer.writeBits(@as(u64, first_bits >> 32), 32);
    try writer.writeBits(first_bits & 0xFFFFFFFF, 32);

    if (values.len == 1) {
        // Single value: store raw little-endian, same as the raw fallback —
        // decodeFloat64 detects raw by (len == count * 8), so the bytes here
        // must be the native representation, not BitWriter output.
        return EncodedPayload{
            .bytes = try allocator.dupe(u8, std.mem.sliceAsBytes(values)),
            .flags = .{},
            .allocator = allocator,
        };
    }

    var prev_bits = first_bits;
    var prev_leading: u7 = 0;
    var prev_significant: u7 = 0;

    for (values[1..]) |val| {
        const bits = @as(u64, @bitCast(val));
        const xored = bits ^ prev_bits;

        if (xored == 0) {
            // Same value: write 0
            try writer.writeBit(0);
        } else {
            var lz = leadingZeros64(xored);
            const sig = significantBits64(xored);

            if (lz == prev_leading and sig <= prev_significant) {
                // Same leading zeros and fits in previous significant bits
                try writer.writeBit(1);
                try writer.writeBit(0);
                const shift_for_meaningful: u7 = 64 - prev_leading - prev_significant;
                const meaningful = xored >> @intCast(shift_for_meaningful);
                try writer.writeBits(meaningful, prev_significant);
            } else {
                // New leading zeros and significant bits
                if (lz == 0) {
                    // Sign-bit flip: 64 meaningful bits don't fit the 6-bit
                    // length field. Gorilla can't help — fall back to raw.
                    const raw = try allocator.dupe(u8, std.mem.sliceAsBytes(values));
                    return EncodedPayload{
                        .bytes = raw,
                        .flags = .{},
                        .allocator = allocator,
                    };
                }
                if (lz > 31) lz = 31; // 5-bit field; excess zeros are absorbed
                                      // into the meaningful field below.
                const meaningful_len: u7 = 64 - lz; // 33..63, always fits 6 bits
                try writer.writeBit(1);
                try writer.writeBit(1);
                try writer.writeBits(@as(u64, lz), 5);
                try writer.writeBits(@as(u64, meaningful_len), 6);
                const shift_for_meaningful: u7 = 64 - lz - meaningful_len;
                const meaningful = xored >> @intCast(shift_for_meaningful);
                try writer.writeBits(meaningful, meaningful_len);
                prev_leading = lz;
                prev_significant = meaningful_len;
            }
        }
        prev_bits = bits;
    }

    const bytes = try writer.toOwnedSlice();

    // Gorilla should always be smaller or equal for sequential data; for random
    // data it may expand slightly. Only use encoding if it actually helps.
    if (bytes.len >= raw_size) {
        allocator.free(bytes);
        const raw = try allocator.dupe(u8, std.mem.sliceAsBytes(values));
        return EncodedPayload{
            .bytes = raw,
            .flags = .{},
            .allocator = allocator,
        };
    }

    return EncodedPayload{
        .bytes = bytes,
        .flags = .{}, // Gorilla doesn't use EncodingFlags bits currently
        .allocator = allocator,
    };
}

/// Decode a Gorilla-encoded f64 payload.
/// Returns an owned []f64. Caller owns the memory.
pub fn decodeFloat64(allocator: std.mem.Allocator, encoded: []const u8, count: u32) ![]f64 {
    if (count == 0) {
        return allocator.alloc(f64, 0);
    }
    if (encoded.len < 8) return error.InvalidEncodedData;

    // Check if this is raw data (no encoding applied)
    if (encoded.len == count * @sizeOf(f64)) {
        const vals = std.mem.bytesAsSlice(f64, encoded[0..count * @sizeOf(f64)]);
        const result = try allocator.alloc(f64, count);
        errdefer allocator.free(result);
        @memcpy(result, vals);
        return result;
    }

    var result = try allocator.alloc(f64, count);
    errdefer allocator.free(result);

    var reader = BitReader.init(encoded);

    // Read first value (64 bits in two 32-bit chunks)
    const first_hi = try reader.readBits(32);
    const first_lo = try reader.readBits(32);
    const first_bits = (first_hi << 32) | first_lo;
    result[0] = @as(f64, @bitCast(first_bits));

    if (count == 1) return result;

    var prev_bits = first_bits;
    var prev_leading: u7 = 0;
    var prev_significant: u7 = 0;

    for (1..count) |i| {
        const flag = try reader.readBit();
        if (flag == 0) {
            // Same value
            result[i] = @as(f64, @bitCast(prev_bits));
        } else {
            const second_flag = try reader.readBit();
            var meaningful: u64 = 0;
            var meaningful_len: u7 = 0;
            var lz: u7 = 0;

            if (second_flag == 0) {
                // Same leading zeros, previous significant bits
                meaningful_len = prev_significant;
                lz = prev_leading;
                meaningful = try reader.readBits(meaningful_len);
            } else {
                // New leading zeros and significant bits
                lz = @intCast(try reader.readBits(5));
                meaningful_len = @intCast(try reader.readBits(6));
                meaningful = try reader.readBits(meaningful_len);
                prev_leading = lz;
                prev_significant = meaningful_len;
            }

            // Reconstruct xored value
            const xored = if (lz + meaningful_len == 64)
                meaningful
            else
                meaningful << @intCast(@as(u7, 64) - @as(u7, lz) - meaningful_len);
            const bits = prev_bits ^ xored;
            result[i] = @as(f64, @bitCast(bits));
            prev_bits = bits;
        }
    }

    return result;
}

// ─── Float encoding tests ──────────────────────────────────────────────────

test "encodeFloat64 sequential → compresses" {
    const allocator = std.testing.allocator;
    // Slowly-varying floats: small XOR deltas between consecutive values,
    // which is what Gorilla actually compresses (mantissa-heavy sequences
    // like 1.0, 1.1, 1.2 fall back to raw and can never beat it).
    const values = &[_]f64{ 100.0, 100.0000001, 100.0000002, 100.0000003, 100.0000004, 100.0000005 };
    var encoded = try encodeFloat64(allocator, values);
    defer encoded.deinit();

    // Sequential data should compress significantly
    try std.testing.expect(encoded.bytes.len < values.len * @sizeOf(f64));

    const decoded = try decodeFloat64(allocator, encoded.bytes, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(f64, values, decoded);
}

test "encodeFloat64 repeated → tiny" {
    const allocator = std.testing.allocator;
    const values = &[_]f64{ 3.14, 3.14, 3.14, 3.14, 3.14 };
    var encoded = try encodeFloat64(allocator, values);
    defer encoded.deinit();

    // All same values → just 64 bits for first + 4 zero bits
    try std.testing.expect(encoded.bytes.len <= 9);

    const decoded = try decodeFloat64(allocator, encoded.bytes, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(f64, values, decoded);
}

test "encodeFloat64 random → raw fallback" {
    const allocator = std.testing.allocator;
    const values = &[_]f64{ 1.5, -999.123, 0.0, 3.14159, -2.718 };
    var encoded = try encodeFloat64(allocator, values);
    defer encoded.deinit();

    // Random data may fall back to raw
    const decoded = try decodeFloat64(allocator, encoded.bytes, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(f64, values, decoded);
}

test "encodeFloat64 empty" {
    const allocator = std.testing.allocator;
    const values = &[_]f64{};
    var encoded = try encodeFloat64(allocator, values);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(usize, 0), encoded.bytes.len);

    const decoded = try decodeFloat64(allocator, encoded.bytes, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqual(@as(usize, 0), decoded.len);
}

test "encodeFloat64 single value" {
    const allocator = std.testing.allocator;
    const values = &[_]f64{42.0};
    var encoded = try encodeFloat64(allocator, values);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(usize, 8), encoded.bytes.len);

    const decoded = try decodeFloat64(allocator, encoded.bytes, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(f64, values, decoded);
}

// ─── Bool encoding (bitmap + RLE auto-selector) ────────────────────────────

/// Encode an array of bool values using either bitmap or RLE.
/// Format byte 0 = 0 (RLE) or 1 (bitmap), followed by payload.
/// Returns an EncodedPayload. Caller must call `deinit`.
pub fn encodeBool(allocator: std.mem.Allocator, values: []const bool) !EncodedPayload {
    if (values.len == 0) {
        return EncodedPayload{
            .bytes = try allocator.alloc(u8, 0),
            .flags = .{},
            .allocator = allocator,
        };
    }

    // Count runs for RLE size estimate
    var run_count: usize = 1;
    for (1..values.len) |i| {
        if (values[i] != values[i - 1]) run_count += 1;
    }

    const bitmap_size = (values.len + 7) / 8;
    const rle_size = 1 + 4 + run_count * 5; // format + num_runs + runs

    if (rle_size < bitmap_size and run_count > 1) {
        // Use RLE
        var buf = try allocator.alloc(u8, rle_size);
        errdefer allocator.free(buf);
        buf[0] = 0; // RLE marker

        var runs = try allocator.alloc(struct { count: u32, value: u8 }, run_count);
        defer allocator.free(runs);

        var run_idx: usize = 0;
        runs[0] = .{ .count = 1, .value = if (values[0]) 1 else 0 };
        for (1..values.len) |i| {
            const v: u8 = if (values[i]) 1 else 0;
            if (v == runs[run_idx].value) {
                runs[run_idx].count += 1;
            } else {
                run_idx += 1;
                runs[run_idx] = .{ .count = 1, .value = v };
            }
        }

        std.mem.writeInt(u32, buf[1..][0..4], @intCast(run_idx + 1), .little);
        var off: usize = 5;
        for (0..run_idx + 1) |i| {
            std.mem.writeInt(u32, buf[off..][0..4], runs[i].count, .little);
            buf[off + 4] = runs[i].value;
            off += 5;
        }

        // Trim to actual size
        if (off < rle_size) {
            const trimmed = try allocator.realloc(buf, off);
            buf = trimmed;
        }

        return EncodedPayload{
            .bytes = buf,
            .flags = .{},
            .allocator = allocator,
        };
    } else {
        // Use bitmap
        const buf = try allocator.alloc(u8, 1 + bitmap_size);
        errdefer allocator.free(buf);
        buf[0] = 1; // Bitmap marker
        @memset(buf[1..], 0);
        for (values, 0..) |v, i| {
            if (v) {
                buf[1 + i / 8] |= @as(u8, 1) << @intCast(i % 8);
            }
        }
        return EncodedPayload{
            .bytes = buf,
            .flags = .{},
            .allocator = allocator,
        };
    }
}

/// Decode a bool payload. Returns an owned []bool. Caller owns the memory.
pub fn decodeBool(allocator: std.mem.Allocator, encoded: []const u8, count: u32) ![]bool {
    if (count == 0) {
        return allocator.alloc(bool, 0);
    }

    // Raw fallback: if no format marker, assume raw bytes (legacy)
    if (encoded.len == count) {
        const result = try allocator.alloc(bool, count);
        errdefer allocator.free(result);
        for (encoded, 0..) |b, i| {
            result[i] = b != 0;
        }
        return result;
    }

    if (encoded.len == 0) return error.InvalidEncodedData;
    const format = encoded[0];

    var result = try allocator.alloc(bool, count);
    errdefer allocator.free(result);

    if (format == 0) {
        // RLE
        if (encoded.len < 5) return error.InvalidEncodedData;
        const num_runs = std.mem.readInt(u32, encoded[1..][0..4], .little);
        var off: usize = 5;
        var idx: usize = 0;
        for (0..num_runs) |_| {
            if (off + 5 > encoded.len) return error.InvalidEncodedData;
            const run_count = std.mem.readInt(u32, encoded[off..][0..4], .little);
            const run_value = encoded[off + 4] != 0;
            off += 5;
            for (0..run_count) |_| {
                if (idx >= count) return error.InvalidEncodedData;
                result[idx] = run_value;
                idx += 1;
            }
        }
        if (idx != count) return error.InvalidEncodedData;
    } else if (format == 1) {
        // Bitmap
        const bitmap = encoded[1..];
        for (0..count) |i| {
            const byte_idx = i / 8;
            const bit_idx = i % 8;
            if (byte_idx >= bitmap.len) return error.InvalidEncodedData;
            result[i] = (bitmap[byte_idx] >> @intCast(bit_idx)) & 1 != 0;
        }
    } else {
        return error.InvalidEncodedData;
    }

    return result;
}

// ─── Bool encoding tests ───────────────────────────────────────────────────

test "encodeBool bitmap" {
    const allocator = std.testing.allocator;
    const values = &[_]bool{ true, false, true, false, true, false, true, false };
    var encoded = try encodeBool(allocator, values);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(usize, 2), encoded.bytes.len); // 1 format + 1 bitmap

    const decoded = try decodeBool(allocator, encoded.bytes, @intCast(values.len));
    defer allocator.free(decoded);
    try std.testing.expectEqualSlices(bool, values, decoded);
}

test "encodeBool RLE" {
    const allocator = std.testing.allocator;
    const values = &[_]bool{ true, true, true, false, false, true, true };
    var encoded = try encodeBool(allocator, values);
    defer encoded.deinit();

    // With only 7 values, bitmap (1 byte) always beats RLE.
    // RLE wins with long runs: 100 values in 2 runs.
    var long_vals = try allocator.alloc(bool, 100);
    defer allocator.free(long_vals);
    @memset(long_vals[0..50], true);
    @memset(long_vals[50..100], false);

    var encoded2 = try encodeBool(allocator, long_vals);
    defer encoded2.deinit();

    const decoded2 = try decodeBool(allocator, encoded2.bytes, 100);
    defer allocator.free(decoded2);
    try std.testing.expectEqualSlices(bool, long_vals, decoded2);
}

test "encodeBool empty" {
    const allocator = std.testing.allocator;
    const values = &[_]bool{};
    var encoded = try encodeBool(allocator, values);
    defer encoded.deinit();
    try std.testing.expectEqual(@as(usize, 0), encoded.bytes.len);
}

test "encodeBool all same" {
    const allocator = std.testing.allocator;
    const values = &[_]bool{true} ** 100;
    var encoded = try encodeBool(allocator, values);
    defer encoded.deinit();

    const decoded = try decodeBool(allocator, encoded.bytes, 100);
    defer allocator.free(decoded);
    for (decoded) |v| try std.testing.expect(v);
}
