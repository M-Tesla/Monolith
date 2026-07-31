// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Column Store Scanner — zero-copy sequential block iterator.

const std = @import("std");
const block = @import("block.zig");
const store = @import("store.zig");
const encoding = @import("encoding.zig");
const compress = @import("compress.zig");

fn readBlockHeader(bytes: []const u8) block.BlockHeader {
    var h: block.BlockHeader = undefined;
    @memcpy(std.mem.asBytes(&h), bytes[0..@sizeOf(block.BlockHeader)]);
    return h;
}

/// Iterator over INT32/INT64 column blocks, yielding one i32 at a time.
pub const IntScanner = struct {
    col_store: *const store.ColumnStore,
    block_ids: []const u32,
    current_block_idx: usize,
    current_vals: []const i32,
    current_row_idx: usize,
    allocator: std.mem.Allocator,
    current_decoded: ?[]i32,
    current_decompressed: ?[]u8,

    pub fn init(allocator: std.mem.Allocator, col_store: *const store.ColumnStore, block_ids: []const u32) IntScanner {
        return .{
            .col_store = col_store,
            .block_ids = block_ids,
            .current_block_idx = 0,
            .current_vals = &.{},
            .current_row_idx = 0,
            .allocator = allocator,
            .current_decoded = null,
            .current_decompressed = null,
        };
    }

    pub fn deinit(self: *IntScanner) void {
        if (self.current_decoded) |buf| self.allocator.free(buf);
        if (self.current_decompressed) |buf| self.allocator.free(buf);
    }

    pub fn next(self: *IntScanner) !?i32 {
        while (true) {
            if (self.current_row_idx < self.current_vals.len) {
                const val = self.current_vals[self.current_row_idx];
                self.current_row_idx += 1;
                return val;
            }
            // Load next block
            if (self.current_block_idx >= self.block_ids.len) return null;
            const bid = self.block_ids[self.current_block_idx];
            const slot = self.col_store.blocks.get(bid) orelse return error.BlockNotFound;
            const header = readBlockHeader(slot.bytes);
            const payload = slot.bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

            // Free previous block buffers
            if (self.current_decoded) |buf| {
                self.allocator.free(buf);
                self.current_decoded = null;
            }
            if (self.current_decompressed) |buf| {
                self.allocator.free(buf);
                self.current_decompressed = null;
            }

            var decoded_payload: []const u8 = payload;
            if (header.compression == .lz4) {
                const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
                const footer = std.mem.bytesToValue(block.BlockFooter, slot.bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
                self.current_decompressed = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
                decoded_payload = self.current_decompressed.?;
            }

            const flags: encoding.EncodingFlags = @bitCast(header.encoding);
            if (!flags.delta and !flags.bitpack and !flags.rle) {
                // Fast path: raw data
                const vals = std.mem.bytesAsSlice(i32, decoded_payload[0..header.row_count * @sizeOf(i32)]);
                self.current_vals = @alignCast(@ptrCast(vals));
            } else {
                self.current_decoded = try encoding.decodeInt32(self.allocator, decoded_payload, flags, header.row_count);
                self.current_vals = self.current_decoded.?;
            }
            self.current_row_idx = 0;
            self.current_block_idx += 1;
        }
    }
};

/// Iterator over FLOAT64 column blocks, yielding one f64 at a time.
pub const FloatScanner = struct {
    col_store: *const store.ColumnStore,
    block_ids: []const u32,
    current_block_idx: usize,
    current_vals: []const f64,
    current_row_idx: usize,
    allocator: std.mem.Allocator,
    current_decoded: ?[]f64,
    current_decompressed: ?[]u8,

    pub fn init(allocator: std.mem.Allocator, col_store: *const store.ColumnStore, block_ids: []const u32) FloatScanner {
        return .{
            .col_store = col_store,
            .block_ids = block_ids,
            .current_block_idx = 0,
            .current_vals = &.{},
            .current_row_idx = 0,
            .allocator = allocator,
            .current_decoded = null,
            .current_decompressed = null,
        };
    }

    pub fn deinit(self: *FloatScanner) void {
        if (self.current_decoded) |buf| self.allocator.free(buf);
        if (self.current_decompressed) |buf| self.allocator.free(buf);
    }

    pub fn next(self: *FloatScanner) !?f64 {
        while (true) {
            if (self.current_row_idx < self.current_vals.len) {
                const val = self.current_vals[self.current_row_idx];
                self.current_row_idx += 1;
                return val;
            }
            if (self.current_block_idx >= self.block_ids.len) return null;
            const bid = self.block_ids[self.current_block_idx];
            const slot = self.col_store.blocks.get(bid) orelse return error.BlockNotFound;
            const header = readBlockHeader(slot.bytes);
            const payload = slot.bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

            if (self.current_decoded) |buf| {
                self.allocator.free(buf);
                self.current_decoded = null;
            }
            if (self.current_decompressed) |buf| {
                self.allocator.free(buf);
                self.current_decompressed = null;
            }

            var decoded_payload: []const u8 = payload;
            if (header.compression == .lz4) {
                const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
                const footer = std.mem.bytesToValue(block.BlockFooter, slot.bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
                self.current_decompressed = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
                decoded_payload = self.current_decompressed.?;
            }

            if (decoded_payload.len == header.row_count * @sizeOf(f64)) {
                // Raw data
                const vals = std.mem.bytesAsSlice(f64, decoded_payload[0..header.row_count * @sizeOf(f64)]);
                self.current_vals = @alignCast(@ptrCast(vals));
            } else {
                self.current_decoded = try encoding.decodeFloat64(self.allocator, decoded_payload, header.row_count);
                self.current_vals = self.current_decoded.?;
            }
            self.current_row_idx = 0;
            self.current_block_idx += 1;
        }
    }
};

/// Iterator over BOOL column blocks, yielding one bool at a time.
pub const BoolScanner = struct {
    col_store: *const store.ColumnStore,
    block_ids: []const u32,
    current_block_idx: usize,
    current_vals: []const bool,
    current_row_idx: usize,
    allocator: std.mem.Allocator,
    current_decoded: ?[]bool,
    current_decompressed: ?[]u8,

    pub fn init(allocator: std.mem.Allocator, col_store: *const store.ColumnStore, block_ids: []const u32) BoolScanner {
        return .{
            .col_store = col_store,
            .block_ids = block_ids,
            .current_block_idx = 0,
            .current_vals = &.{},
            .current_row_idx = 0,
            .allocator = allocator,
            .current_decoded = null,
            .current_decompressed = null,
        };
    }

    pub fn deinit(self: *BoolScanner) void {
        if (self.current_decoded) |buf| self.allocator.free(buf);
        if (self.current_decompressed) |buf| self.allocator.free(buf);
    }

    pub fn next(self: *BoolScanner) !?bool {
        while (true) {
            if (self.current_row_idx < self.current_vals.len) {
                const val = self.current_vals[self.current_row_idx];
                self.current_row_idx += 1;
                return val;
            }
            if (self.current_block_idx >= self.block_ids.len) return null;
            const bid = self.block_ids[self.current_block_idx];
            const slot = self.col_store.blocks.get(bid) orelse return error.BlockNotFound;
            const header = readBlockHeader(slot.bytes);
            const payload = slot.bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

            if (self.current_decoded) |buf| {
                self.allocator.free(buf);
                self.current_decoded = null;
            }
            if (self.current_decompressed) |buf| {
                self.allocator.free(buf);
                self.current_decompressed = null;
            }

            var decoded_payload: []const u8 = payload;
            if (header.compression == .lz4) {
                const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
                const footer = std.mem.bytesToValue(block.BlockFooter, slot.bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
                self.current_decompressed = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
                decoded_payload = self.current_decompressed.?;
            }

            if (decoded_payload.len == header.row_count) {
                // Raw data
                const result = try self.allocator.alloc(bool, header.row_count);
                errdefer self.allocator.free(result);
                for (decoded_payload, 0..) |b, i| {
                    result[i] = b != 0;
                }
                self.current_decoded = result;
                self.current_vals = result;
            } else {
                self.current_decoded = try encoding.decodeBool(self.allocator, decoded_payload, header.row_count);
                self.current_vals = self.current_decoded.?;
            }
            self.current_row_idx = 0;
            self.current_block_idx += 1;
        }
    }
};

/// Iterator over STRING column blocks, yielding one string at a time.
/// The returned slice is valid only until the next call to `next`.
pub const StringScanner = struct {
    col_store: *const store.ColumnStore,
    block_ids: []const u32,
    current_block_idx: usize,
    current_strings: ?[][]const u8,
    current_string_idx: usize,
    allocator: std.mem.Allocator,
    current_owned: ?[]u8,
    current_decompressed: ?[]u8,

    pub fn init(allocator: std.mem.Allocator, col_store: *const store.ColumnStore, block_ids: []const u32) StringScanner {
        return .{
            .col_store = col_store,
            .block_ids = block_ids,
            .current_block_idx = 0,
            .current_strings = null,
            .current_string_idx = 0,
            .allocator = allocator,
            .current_owned = null,
            .current_decompressed = null,
        };
    }

    pub fn deinit(self: *StringScanner) void {
        self.freeCurrentBlock();
    }

    fn freeCurrentBlock(self: *StringScanner) void {
        if (self.current_strings) |strings| {
            self.allocator.free(strings);
            self.current_strings = null;
        }
        if (self.current_owned) |buf| {
            self.allocator.free(buf);
            self.current_owned = null;
        }
        if (self.current_decompressed) |buf| {
            self.allocator.free(buf);
            self.current_decompressed = null;
        }
    }

    pub fn next(self: *StringScanner) !?[]const u8 {
        while (true) {
            const cur_strings: []const []const u8 = self.current_strings orelse &.{};
            if (self.current_string_idx < cur_strings.len) {
                const str = self.current_strings.?[self.current_string_idx];
                self.current_string_idx += 1;
                return str;
            }
            if (self.current_block_idx >= self.block_ids.len) return null;
            const bid = self.block_ids[self.current_block_idx];
            const slot = self.col_store.blocks.get(bid) orelse return error.BlockNotFound;

            self.freeCurrentBlock();

            const header = readBlockHeader(slot.bytes);
            const payload = slot.bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

            var decoded_payload: []const u8 = payload;
            if (header.compression == .lz4) {
                const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
                const footer = std.mem.bytesToValue(block.BlockFooter, slot.bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
                self.current_decompressed = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
                decoded_payload = self.current_decompressed.?;
            }

            const flags: encoding.EncodingFlags = @bitCast(header.encoding);
            var view_block = try encoding.decodeStringView(self.allocator, decoded_payload, flags, header.row_count);
            // Transfer ownership of the view block internals to the scanner
            self.current_strings = view_block.strings;
            self.current_owned = view_block.owned;
            view_block.strings = &.{};
            view_block.owned = null;
            self.current_string_idx = 0;
            self.current_block_idx += 1;
        }
    }
};

// ─── Tests ─────────────────────────────────────────────────────────────────

test "IntScanner single block" {
    const allocator = std.testing.allocator;
    var col_store = store.ColumnStore.init(allocator);
    defer col_store.deinit();

    try col_store.createTable("t", &[_]store.ColumnDef{.{ .name = "c", .data_type = .int32 }});
    try col_store.insertIntColumn("t", "c", &[_]i32{ 10, 20, 30 });
    try col_store.flushAll();

    const table = col_store.getTable("t").?;
    const col = table.getColumn("c").?;

    var scanner = IntScanner.init(allocator, &col_store, col.block_ids.items);
    defer scanner.deinit();
    try std.testing.expectEqual(@as(i32, 10), (try scanner.next()).?);
    try std.testing.expectEqual(@as(i32, 20), (try scanner.next()).?);
    try std.testing.expectEqual(@as(i32, 30), (try scanner.next()).?);
    try std.testing.expect(try scanner.next() == null);
}

test "IntScanner multiple blocks" {
    const allocator = std.testing.allocator;
    var col_store = store.ColumnStore.init(allocator);
    defer col_store.deinit();

    try col_store.createTable("t", &[_]store.ColumnDef{.{ .name = "c", .data_type = .int32 }});
    try col_store.insertIntColumn("t", "c", &[_]i32{ 1, 2 });
    try col_store.insertIntColumn("t", "c", &[_]i32{ 3, 4 });
    try col_store.flushAll();

    const table = col_store.getTable("t").?;
    const col = table.getColumn("c").?;

    var scanner = IntScanner.init(allocator, &col_store, col.block_ids.items);
    defer scanner.deinit();
    try std.testing.expectEqual(@as(i32, 1), (try scanner.next()).?);
    try std.testing.expectEqual(@as(i32, 2), (try scanner.next()).?);
    try std.testing.expectEqual(@as(i32, 3), (try scanner.next()).?);
    try std.testing.expectEqual(@as(i32, 4), (try scanner.next()).?);
    try std.testing.expect(try scanner.next() == null);
}

test "FloatScanner" {
    const allocator = std.testing.allocator;
    var col_store = store.ColumnStore.init(allocator);
    defer col_store.deinit();

    try col_store.createTable("t", &[_]store.ColumnDef{.{ .name = "c", .data_type = .float64 }});
    try col_store.insertFloatColumn("t", "c", &[_]f64{ 1.1, 2.2 });
    try col_store.flushAll();

    const table = col_store.getTable("t").?;
    const col = table.getColumn("c").?;

    var scanner = FloatScanner.init(allocator, &col_store, col.block_ids.items);
    defer scanner.deinit();
    try std.testing.expectEqual(@as(f64, 1.1), (try scanner.next()).?);
    try std.testing.expectEqual(@as(f64, 2.2), (try scanner.next()).?);
    try std.testing.expect(try scanner.next() == null);
}

test "StringScanner" {
    const allocator = std.testing.allocator;
    var col_store = store.ColumnStore.init(allocator);
    defer col_store.deinit();

    try col_store.createTable("t", &[_]store.ColumnDef{.{ .name = "c", .data_type = .string }});
    try col_store.insertStringColumn("t", "c", &[_][]const u8{ "a", "bb" });
    try col_store.flushAll();

    const table = col_store.getTable("t").?;
    const col = table.getColumn("c").?;

    var scanner = StringScanner.init(allocator, &col_store, col.block_ids.items);
    defer scanner.deinit();
    try std.testing.expectEqualStrings("a", (try scanner.next()).?);
    try std.testing.expectEqualStrings("bb", (try scanner.next()).?);
    try std.testing.expect(try scanner.next() == null);
}
