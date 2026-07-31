// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Column Store Block Format.
//!
//! A block is the fundamental unit of the Column Store.  It stores a
//! contiguous batch of encoded values for a single column.  Blocks are
//! immutable once written; changed data is rebuilt as new blocks.
//!
//! Physical layout on disk / in memory:
//!   [BlockHeader] [data payload] [BlockFooter]

const std = @import("std");

/// Logical data types supported inside a column block.
pub const ColumnDataType = enum(u8) {
    int8 = 1,
    int16 = 2,
    int32 = 3,
    int64 = 4,
    uint8 = 5,
    uint16 = 6,
    uint32 = 7,
    uint64 = 8,
    float32 = 9,
    float64 = 10,
    string = 11,
    bool = 12,
    _,
};

/// Compression algorithms supported for block payloads.
pub const CompressionType = enum(u8) {
    none = 0,
    lz4 = 1,
    _,
};

/// Header present at the start of every block.
pub const BlockHeader = extern struct {
    /// Magic: 'M' 'C' 'B' 'K' = 0x4D43424B
    magic: u32,
    /// Column this block belongs to (stable identifier).
    column_id: u32,
    /// Block sequence number within the column.
    block_id: u32,
    /// Number of rows stored in this block.
    row_count: u32,
    /// Logical data type of the column.
    data_type: u8,
    /// Encoding used for the payload (0 = raw / uncompressed).
    encoding: u8,
    /// Compression algorithm used for the payload.
    compression: CompressionType,
    /// Reserved.
    reserved: u8,
    /// Size of the data payload in bytes (does NOT include header/footer).
    payload_size: u32,

    pub const MAGIC: u32 = 0x4D43424B;

    pub fn validate(self: *const BlockHeader) bool {
        return self.magic == MAGIC;
    }
};

/// Footer present at the end of every block.
pub const BlockFooter = extern struct {
    /// Number of null values in this block.
    null_count: u32,
    /// CRC32 of the payload (header and footer excluded).
    payload_crc: u32,
    /// Original payload size before compression (0 if not compressed).
    uncompressed_size: u32,
    /// Minimum value in this block (bit-cast from i64/f64; 0 if not applicable).
    min_value: u64,
    /// Maximum value in this block (bit-cast from i64/f64; 0 if not applicable).
    max_value: u64,

    pub fn minInt(self: BlockFooter) i64 {
        return @as(i64, @bitCast(self.min_value));
    }
    pub fn maxInt(self: BlockFooter) i64 {
        return @as(i64, @bitCast(self.max_value));
    }
    pub fn minFloat(self: BlockFooter) f64 {
        return @as(f64, @bitCast(self.min_value));
    }
    pub fn maxFloat(self: BlockFooter) f64 {
        return @as(f64, @bitCast(self.max_value));
    }

    /// Returns true if this block can be skipped for an integer predicate.
    /// `op` is the comparison operator: '<', '>', '=', etc.
    pub fn canSkipInt(self: BlockFooter, op: u8, value: i64) bool {
        const mn = self.minInt();
        const mx = self.maxInt();
        // Skip means: no row in [min, max] can satisfy `col <op> value`.
        return switch (op) {
            '<' => mn >= value,
            '>' => mx <= value,
            '=' => mx < value or mn > value,
            else => false,
        };
    }

    /// Returns true if this block can be skipped for a float predicate.
    pub fn canSkipFloat(self: BlockFooter, op: u8, value: f64) bool {
        const mn = self.minFloat();
        const mx = self.maxFloat();
        // Skip means: no row in [min, max] can satisfy `col <op> value`.
        return switch (op) {
            '<' => mn >= value,
            '>' => mx <= value,
            '=' => mx < value or mn > value,
            else => false,
        };
    }
};

/// In-memory representation of a column block.
pub const ColumnBlock = struct {
    header: BlockHeader,
    /// Owned slice containing the raw payload bytes.
    /// Caller must free when done.
    payload: []const u8,
    footer: BlockFooter,

    /// Serialize the block into a newly allocated buffer.
    /// Returns a single contiguous buffer: header + payload + footer.
    pub fn serialize(self: ColumnBlock, allocator: std.mem.Allocator) ![]u8 {
        const header_size = @sizeOf(BlockHeader);
        const footer_size = @sizeOf(BlockFooter);
        const total = header_size + self.payload.len + footer_size;
        var buf = try allocator.alloc(u8, total);
        @memcpy(buf[0..header_size], std.mem.asBytes(&self.header));
        @memcpy(buf[header_size..header_size + self.payload.len], self.payload);
        @memcpy(buf[header_size + self.payload.len..total], std.mem.asBytes(&self.footer));
        return buf;
    }

    /// Deserialize a block from a contiguous byte buffer.
    /// The returned ColumnBlock owns a copy of the payload.
    pub fn deserialize(allocator: std.mem.Allocator, buf: []const u8) !ColumnBlock {
        const header_size = @sizeOf(BlockHeader);
        const footer_size = @sizeOf(BlockFooter);
        if (buf.len < header_size + footer_size) return error.BufferTooSmall;

        const header = std.mem.bytesToValue(BlockHeader, buf[0..header_size]);
        if (!header.validate()) return error.InvalidBlockMagic;

        const payload_size = header.payload_size;
        if (buf.len < header_size + payload_size + footer_size) return error.BufferTooSmall;

        const payload = try allocator.dupe(u8, buf[header_size..header_size + payload_size]);
        errdefer allocator.free(payload);

        const footer = std.mem.bytesToValue(BlockFooter, buf[header_size + payload_size..header_size + payload_size + footer_size]);

        // Verify payload CRC
        const actual_crc = std.hash.Crc32.hash(payload);
        if (actual_crc != footer.payload_crc) return error.CrcMismatch;

        return ColumnBlock{
            .header = header,
            .payload = payload,
            .footer = footer,
        };
    }

    /// Free the owned payload.
    pub fn deinit(self: *ColumnBlock, allocator: std.mem.Allocator) void {
        allocator.free(self.payload);
        self.payload = &.{ };
    }

    /// Total serialized size of the block.
    pub fn serializedSize(self: ColumnBlock) usize {
        return @sizeOf(BlockHeader) + self.payload.len + @sizeOf(BlockFooter);
    }
};

// ─── Helper builders for raw (uncompressed) blocks ─────────────────────────

/// Build a raw INT block (int32 values).
pub fn buildIntBlock(
    allocator: std.mem.Allocator,
    column_id: u32,
    block_id: u32,
    values: []const i32,
) !ColumnBlock {
    const payload_size = values.len * @sizeOf(i32);
    const payload = try allocator.alloc(u8, payload_size);
    errdefer allocator.free(payload);
    const vals = std.mem.bytesAsSlice(i32, payload);
    @memcpy(vals, values);

    var min_val: i32 = if (values.len > 0) values[0] else 0;
    var max_val: i32 = if (values.len > 0) values[0] else 0;
    for (values) |v| {
        if (v < min_val) min_val = v;
        if (v > max_val) max_val = v;
    }

    return ColumnBlock{
        .header = .{
            .magic = BlockHeader.MAGIC,
            .column_id = column_id,
            .block_id = block_id,
            .row_count = @intCast(values.len),
            .data_type = @intFromEnum(ColumnDataType.int32),
            .encoding = 0,
            .compression = .none,
            .reserved = 0,
            .payload_size = @intCast(payload_size),
        },
        .payload = payload,
        .footer = .{
            .null_count = 0,
            .payload_crc = std.hash.Crc32.hash(payload),
            .uncompressed_size = 0,
            .min_value = @as(u64, @bitCast(@as(i64, min_val))),
            .max_value = @as(u64, @bitCast(@as(i64, max_val))),
        },
    };
}

/// Build a raw FLOAT block (float64 values).
pub fn buildFloatBlock(
    allocator: std.mem.Allocator,
    column_id: u32,
    block_id: u32,
    values: []const f64,
) !ColumnBlock {
    const payload_size = values.len * @sizeOf(f64);
    const payload = try allocator.alloc(u8, payload_size);
    errdefer allocator.free(payload);
    const vals = std.mem.bytesAsSlice(f64, payload);
    @memcpy(vals, values);

    var min_val: f64 = if (values.len > 0) values[0] else 0;
    var max_val: f64 = if (values.len > 0) values[0] else 0;
    for (values) |v| {
        if (v < min_val) min_val = v;
        if (v > max_val) max_val = v;
    }

    return ColumnBlock{
        .header = .{
            .magic = BlockHeader.MAGIC,
            .column_id = column_id,
            .block_id = block_id,
            .row_count = @intCast(values.len),
            .data_type = @intFromEnum(ColumnDataType.float64),
            .encoding = 0,
            .compression = .none,
            .reserved = 0,
            .payload_size = @intCast(payload_size),
        },
        .payload = payload,
        .footer = .{
            .null_count = 0,
            .payload_crc = std.hash.Crc32.hash(payload),
            .uncompressed_size = 0,
            .min_value = @as(u64, @bitCast(min_val)),
            .max_value = @as(u64, @bitCast(max_val)),
        },
    };
}

/// Build a raw STRING block (length-prefixed UTF-8 strings).
/// Each string is stored as: [u32 length][length bytes of UTF-8].
pub fn buildStringBlock(
    allocator: std.mem.Allocator,
    column_id: u32,
    block_id: u32,
    values: []const []const u8,
) !ColumnBlock {
    var total: usize = 0;
    for (values) |s| total += @sizeOf(u32) + s.len;

    var payload = try allocator.alloc(u8, total);
    errdefer allocator.free(payload);
    var offset: usize = 0;
    for (values) |s| {
        std.mem.writeInt(u32, payload[offset..][0..4], @intCast(s.len), .little);
        offset += 4;
        @memcpy(payload[offset..offset + s.len], s);
        offset += s.len;
    }

    return ColumnBlock{
        .header = .{
            .magic = BlockHeader.MAGIC,
            .column_id = column_id,
            .block_id = block_id,
            .row_count = @intCast(values.len),
            .data_type = @intFromEnum(ColumnDataType.string),
            .encoding = 0,
            .compression = .none,
            .reserved = 0,
            .payload_size = @intCast(total),
        },
        .payload = payload,
        .footer = .{
            .null_count = 0,
            .payload_crc = std.hash.Crc32.hash(payload),
            .uncompressed_size = 0,
            .min_value = 0,
            .max_value = 0,
        },
    };
}

// ─── Tests ─────────────────────────────────────────────────────────────────

test "BlockHeader size and magic" {
    const h = BlockHeader{
        .magic = BlockHeader.MAGIC,
        .column_id = 1,
        .block_id = 0,
        .row_count = 100,
        .data_type = @intFromEnum(ColumnDataType.int32),
        .encoding = 0,
        .compression = .none,
        .reserved = 0,
        .payload_size = 400,
    };
    try std.testing.expect(h.validate());
}

test "Int block round-trip" {
    const allocator = std.testing.allocator;
    const values = &[_]i32{ 10, 20, 30, 40, 50 };
    var block = try buildIntBlock(allocator, 1, 0, values);
    defer block.deinit(allocator);

    const buf = try block.serialize(allocator);
    defer allocator.free(buf);

    var block2 = try ColumnBlock.deserialize(allocator, buf);
    defer block2.deinit(allocator);

    try std.testing.expectEqual(block.header.column_id, block2.header.column_id);
    try std.testing.expectEqual(block.header.row_count, block2.header.row_count);
    try std.testing.expectEqual(block.footer.payload_crc, block2.footer.payload_crc);

    const vals2 = std.mem.bytesAsSlice(i32, block2.payload);
    const vals2_aligned: []const i32 = @alignCast(@ptrCast(vals2));
    try std.testing.expectEqualSlices(i32, values, vals2_aligned);
}

test "Float block round-trip" {
    const allocator = std.testing.allocator;
    const values = &[_]f64{ 1.1, 2.2, 3.3 };
    var block = try buildFloatBlock(allocator, 2, 0, values);
    defer block.deinit(allocator);

    const buf = try block.serialize(allocator);
    defer allocator.free(buf);

    var block2 = try ColumnBlock.deserialize(allocator, buf);
    defer block2.deinit(allocator);

    const vals2 = std.mem.bytesAsSlice(f64, block2.payload);
    const vals2_aligned: []const f64 = @alignCast(@ptrCast(vals2));
    try std.testing.expectEqualSlices(f64, values, vals2_aligned);
}

test "String block round-trip" {
    const allocator = std.testing.allocator;
    const values = &[_][]const u8{ "hello", "world", "monolith" };
    var block = try buildStringBlock(allocator, 3, 0, values);
    defer block.deinit(allocator);

    const buf = try block.serialize(allocator);
    defer allocator.free(buf);

    var block2 = try ColumnBlock.deserialize(allocator, buf);
    defer block2.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 3), block2.header.row_count);
}


test "Int block statistics" {
    const allocator = std.testing.allocator;
    const values = &[_]i32{ 100, -50, 30, 200, 0 };
    var b = try buildIntBlock(allocator, 1, 0, values);
    defer b.deinit(allocator);

    const min_val = @as(i64, @bitCast(b.footer.min_value));
    const max_val = @as(i64, @bitCast(b.footer.max_value));
    try std.testing.expectEqual(@as(i64, -50), min_val);
    try std.testing.expectEqual(@as(i64, 200), max_val);
    try std.testing.expectEqual(@as(u32, 0), b.footer.null_count);
}

test "Float block statistics" {
    const allocator = std.testing.allocator;
    const values = &[_]f64{ 3.14, -1.5, 0.0, 99.9 };
    var b = try buildFloatBlock(allocator, 2, 0, values);
    defer b.deinit(allocator);

    const min_val = @as(f64, @bitCast(b.footer.min_value));
    const max_val = @as(f64, @bitCast(b.footer.max_value));
    try std.testing.expectEqual(@as(f64, -1.5), min_val);
    try std.testing.expectEqual(@as(f64, 99.9), max_val);
}


test "BlockFooter canSkipInt" {
    const footer = BlockFooter{
        .null_count = 0,
        .payload_crc = 0,
        .uncompressed_size = 0,
        .min_value = @as(u64, @bitCast(@as(i64, 10))),
        .max_value = @as(u64, @bitCast(@as(i64, 100))),
    };
    try std.testing.expect(footer.canSkipInt('<', 5) == true);
    try std.testing.expect(footer.canSkipInt('<', 200) == false);
    try std.testing.expect(footer.canSkipInt('<', 10) == true);
    try std.testing.expect(footer.canSkipInt('<', 9) == true);
    try std.testing.expect(footer.canSkipInt('>', 101) == true);
    try std.testing.expect(footer.canSkipInt('>', 50) == false);
    try std.testing.expect(footer.canSkipInt('=', 50) == false);
    try std.testing.expect(footer.canSkipInt('=', 200) == true);
    try std.testing.expect(footer.canSkipInt('=', 5) == true);
}

test "BlockFooter canSkipFloat" {
    const footer = BlockFooter{
        .null_count = 0,
        .payload_crc = 0,
        .uncompressed_size = 0,
        .min_value = @as(u64, @bitCast(@as(f64, 1.5))),
        .max_value = @as(u64, @bitCast(@as(f64, 9.5))),
    };
    try std.testing.expect(footer.canSkipFloat('<', 1.0) == true);
    try std.testing.expect(footer.canSkipFloat('>', 10.0) == true);
    try std.testing.expect(footer.canSkipFloat('>', 5.0) == false);
    try std.testing.expect(footer.canSkipFloat('=', 2.0) == false);
}
