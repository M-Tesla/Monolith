// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Column Store — in-memory analytics cache.
//!
//! Combines Catalog (schema metadata) with Block storage (encoded values).
//! Blocks are kept in memory as serialized byte buffers and never touch disk.

const std = @import("std");
const block = @import("block.zig");
const catalog = @import("catalog.zig");

const encoding = @import("encoding.zig");
const compress = @import("compress.zig");

/// Number of rows accumulated before a block is automatically flushed.
/// Larger blocks amortize per-block decode overhead during scan.
pub const BLOCK_CAPACITY: usize = 1024;

/// Definition of a column at table-creation time.
pub const ColumnDef = struct {
    name: []const u8,
    data_type: block.ColumnDataType,
};

/// In-memory storage for a single block (serialized bytes).
pub const BlockSlot = struct {
    block_id: u32,
    bytes: []const u8,
};

/// Result of a zero-copy string column scan. Strings are views into one or more
/// contiguous buffers. Caller must call `deinit`.
pub const StringColumnData = struct {
    strings: [][]const u8,
    buffers: [][]u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *StringColumnData) void {
        for (self.buffers) |buf| self.allocator.free(buf);
        self.allocator.free(self.buffers);
        self.allocator.free(self.strings);
    }
};

/// Accumulated values waiting to be flushed into a block.
const PendingValues = union(enum) {
    int32: std.ArrayList(i32),
    float64: std.ArrayList(f64),
    string: std.ArrayList([]const u8),
    bool: std.ArrayList(bool),

    pub fn deinit(self: *PendingValues, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .int32 => |*list| list.deinit(allocator),
            .float64 => |*list| list.deinit(allocator),
            .string => |*list| {
                for (list.items) |s| allocator.free(s);
                list.deinit(allocator);
            },
            .bool => |*list| list.deinit(allocator),
        }
    }
};

/// High-level columnar storage engine.
pub const ColumnStore = struct {
    allocator: std.mem.Allocator,
    catalog: catalog.ColumnCatalog,
    /// Map block_id → serialized block bytes.
    blocks: std.AutoHashMap(u32, BlockSlot),
    /// Accumulated values per column that haven't been flushed to blocks yet.
    pending: std.StringHashMap(PendingValues),
    /// Total estimated memory used by all tables (blocks + pending + metadata).
    total_estimated_bytes: usize,
    /// RAM limit before LRU eviction kicks in. 0 = unlimited.
    memory_limit_bytes: usize,

    pub fn init(allocator: std.mem.Allocator) ColumnStore {
        return .{
            .allocator = allocator,
            .catalog = catalog.ColumnCatalog.init(allocator),
            .blocks = std.AutoHashMap(u32, BlockSlot).init(allocator),
            .pending = std.StringHashMap(PendingValues).init(allocator),
            .total_estimated_bytes = 0,
            .memory_limit_bytes = 64 * 1024 * 1024, // 64 MB default
        };
    }

    pub fn deinit(self: *ColumnStore) void {
        self.flushAll() catch {};

        var it = self.blocks.valueIterator();
        while (it.next()) |slot| {
            self.allocator.free(slot.bytes);
        }
        self.blocks.deinit();
        var pit = self.pending.iterator();
        while (pit.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.pending.deinit();
        self.catalog.deinit();
    }

    // ─── Schema ────────────────────────────────────────────────────────────

    pub fn createTable(self: *ColumnStore, name: []const u8, columns: []const ColumnDef) !void {
        const table = try self.catalog.createTable(name);
        for (columns) |col| {
            try table.addColumn(self.allocator, col.name, col.data_type);
        }
    }

    pub fn dropTable(self: *ColumnStore, name: []const u8) !void {
        // Free blocks owned by this table and update memory tracking
        if (self.catalog.getTable(name)) |table| {
            self.total_estimated_bytes -|= table.estimated_bytes;
            for (table.columns.items) |col| {
                for (col.block_ids.items) |bid| {
                    if (self.blocks.fetchRemove(bid)) |kv| {
                        self.allocator.free(kv.value.bytes);
                    }
                }
            }
        }
        try self.catalog.dropTable(name);
    }

    pub fn getTable(self: *ColumnStore, name: []const u8) ?*catalog.ColumnTable {
        const table = self.catalog.getTable(name);
        if (table) |t| {
            t.last_accessed_ms = @intCast(@divTrunc(std.Io.Timestamp.now(std.Io.Threaded.global_single_threaded.io(), .awake).nanoseconds, std.time.ns_per_ms));
        }
        return table;
    }

    pub fn memoryUsed(self: *const ColumnStore) usize {
        return self.total_estimated_bytes;
    }

    /// Evict the least-recently-used table(s) until memory is under the limit.
    /// `exclude_name` is the table currently being accessed and should never be evicted.
    pub fn evictLRU(self: *ColumnStore, exclude_name: []const u8) void {
        if (self.memory_limit_bytes == 0) return;
        while (self.total_estimated_bytes > self.memory_limit_bytes) {
            var it = self.catalog.tables.iterator();
            var lru_table: ?*catalog.ColumnTable = null;
            var lru_time: i64 = std.math.maxInt(i64);
            while (it.next()) |entry| {
                const t = entry.value_ptr.*;
                if (std.mem.eql(u8, t.name, exclude_name)) continue;
                if (t.last_accessed_ms < lru_time) {
                    lru_time = t.last_accessed_ms;
                    lru_table = t;
                }
            }
            const victim = lru_table orelse break; // nothing else to evict
            self.dropTable(victim.name) catch {};
        }
    }

    fn addTableMemory(self: *ColumnStore, table: *catalog.ColumnTable, delta: usize) void {
        table.estimated_bytes +|= delta;
        self.total_estimated_bytes +|= delta;
    }

    fn subTableMemory(self: *ColumnStore, table: *catalog.ColumnTable, delta: usize) void {
        table.estimated_bytes -|= delta;
        self.total_estimated_bytes -|= delta;
    }

    // ─── Pending helpers ───────────────────────────────────────────────────

    fn makePendingKey(self: *ColumnStore, table: []const u8, col: []const u8) ![]u8 {
        const key = try self.allocator.alloc(u8, table.len + 1 + col.len);
        errdefer self.allocator.free(key);
        @memcpy(key[0..table.len], table);
        key[table.len] = 0;
        @memcpy(key[table.len + 1..], col);
        return key;
    }

    fn flushPendingInt(self: *ColumnStore, table_name: []const u8, col_name: []const u8, list: *std.ArrayList(i32)) !void {
        if (list.items.len == 0) return;
        const values = list.items;
        var cb = try block.buildIntBlock(self.allocator, 0, 0, values);
        defer cb.deinit(self.allocator);
        // Re-route through the encoded insert path (bypass pending to avoid recursion)
        try self.insertIntBlock(table_name, col_name, &cb);
        list.clearRetainingCapacity();
    }

    fn flushPendingFloat(self: *ColumnStore, table_name: []const u8, col_name: []const u8, list: *std.ArrayList(f64)) !void {
        if (list.items.len == 0) return;
        const values = list.items;

        var min_val: f64 = values[0];
        var max_val: f64 = values[0];
        for (values) |v| {
            if (v < min_val) min_val = v;
            if (v > max_val) max_val = v;
        }

        // Encode floats (Gorilla XOR delta)
        var encoded = try encoding.encodeFloat64(self.allocator, values);
        defer encoded.deinit();

        // Optionally compress with LZ4
        var payload = encoded.bytes;
        var compression: block.CompressionType = .none;
        const uncompressed_size: u32 = @intCast(encoded.bytes.len);
        var was_compressed = false;

        const compressed = compress.compressLZ4(self.allocator, encoded.bytes) catch null;
        if (compressed) |comp| {
            if (comp.len < encoded.bytes.len) {
                payload = comp;
                compression = .lz4;
                was_compressed = true;
            } else {
                self.allocator.free(comp);
            }
        }

        const owned_payload = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(owned_payload);
        if (was_compressed) {
            self.allocator.free(payload);
        }

        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const block_id = table.allocateBlockId();
        var cb = block.ColumnBlock{
            .header = .{
                .magic = block.BlockHeader.MAGIC,
                .column_id = 0,
                .block_id = block_id,
                .row_count = @intCast(values.len),
                .data_type = @intFromEnum(block.ColumnDataType.float64),
                .encoding = @bitCast(encoded.flags),
                .compression = compression,
                .reserved = 0,
                .payload_size = @intCast(owned_payload.len),
            },
            .payload = owned_payload,
            .footer = .{
                .null_count = 0,
                .payload_crc = std.hash.Crc32.hash(owned_payload),
                .uncompressed_size = uncompressed_size,
                .min_value = @as(u64, @bitCast(min_val)),
                .max_value = @as(u64, @bitCast(max_val)),
            },
        };
        defer cb.deinit(self.allocator);

        const bytes = try cb.serialize(self.allocator);
        try self.blocks.put(block_id, .{ .block_id = block_id, .bytes = bytes });
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        try col.addBlock(self.allocator, block_id);
        list.clearRetainingCapacity();
    }

    fn flushPendingString(self: *ColumnStore, table_name: []const u8, col_name: []const u8, list: *std.ArrayList([]const u8)) !void {
        if (list.items.len == 0) return;
        const values = list.items;
        var cb = try block.buildStringBlock(self.allocator, 0, 0, values);
        defer cb.deinit(self.allocator);
        try self.insertStringBlock(table_name, col_name, &cb);
        for (list.items) |s| self.allocator.free(s);
        list.clearRetainingCapacity();
    }

    fn flushPendingBool(self: *ColumnStore, table_name: []const u8, col_name: []const u8, list: *std.ArrayList(bool)) !void {
        if (list.items.len == 0) return;
        const values = list.items;

        var min_val: u8 = if (values[0]) 1 else 0;
        var max_val: u8 = if (values[0]) 1 else 0;
        for (values) |v| {
            const b: u8 = if (v) 1 else 0;
            if (b < min_val) min_val = b;
            if (b > max_val) max_val = b;
        }

        var encoded = try encoding.encodeBool(self.allocator, values);
        defer encoded.deinit();

        var payload = encoded.bytes;
        var compression: block.CompressionType = .none;
        const uncompressed_size: u32 = @intCast(encoded.bytes.len);
        var was_compressed = false;

        const compressed = compress.compressLZ4(self.allocator, encoded.bytes) catch null;
        if (compressed) |comp| {
            if (comp.len < encoded.bytes.len) {
                payload = comp;
                compression = .lz4;
                was_compressed = true;
            } else {
                self.allocator.free(comp);
            }
        }

        const owned_payload = try self.allocator.dupe(u8, payload);
        errdefer self.allocator.free(owned_payload);
        if (was_compressed) {
            self.allocator.free(payload);
        }

        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const block_id = table.allocateBlockId();
        var cb = block.ColumnBlock{
            .header = .{
                .magic = block.BlockHeader.MAGIC,
                .column_id = 0,
                .block_id = block_id,
                .row_count = @intCast(values.len),
                .data_type = @intFromEnum(block.ColumnDataType.bool),
                .encoding = @bitCast(encoded.flags),
                .compression = compression,
                .reserved = 0,
                .payload_size = @intCast(owned_payload.len),
            },
            .payload = owned_payload,
            .footer = .{
                .null_count = 0,
                .payload_crc = std.hash.Crc32.hash(owned_payload),
                .uncompressed_size = uncompressed_size,
                .min_value = min_val,
                .max_value = max_val,
            },
        };
        defer cb.deinit(self.allocator);

        const bytes = try cb.serialize(self.allocator);
        try self.blocks.put(block_id, .{ .block_id = block_id, .bytes = bytes });
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        try col.addBlock(self.allocator, block_id);
        list.clearRetainingCapacity();
    }

    /// Flush all pending buffers into blocks.
    pub fn flushAll(self: *ColumnStore) !void {
        var it = self.pending.iterator();
        while (it.next()) |entry| {
            const key = entry.key_ptr.*;
            // Parse key back into table_name and col_name
            var sep: usize = 0;
            while (sep < key.len and key[sep] != 0) sep += 1;
            const table_name = key[0..sep];
            const col_name = key[sep + 1..];
            switch (entry.value_ptr.*) {
                .int32 => |*list| try self.flushPendingInt(table_name, col_name, list),
                .float64 => |*list| try self.flushPendingFloat(table_name, col_name, list),
                .string => |*list| try self.flushPendingString(table_name, col_name, list),
                .bool => |*list| try self.flushPendingBool(table_name, col_name, list),
            }
        }
    }

    // ─── Low-level block insert (bypasses pending, used by flush) ──────────

    fn insertIntBlock(self: *ColumnStore, table_name: []const u8, col_name: []const u8, cb: *block.ColumnBlock) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        const block_id = table.allocateBlockId();
        cb.header.block_id = block_id;
        const bytes = try cb.serialize(self.allocator);
        try self.blocks.put(block_id, .{ .block_id = block_id, .bytes = bytes });
        try col.addBlock(self.allocator, block_id);
        table.estimated_bytes += bytes.len;
        self.total_estimated_bytes += bytes.len;
    }

    fn insertFloatBlock(self: *ColumnStore, table_name: []const u8, col_name: []const u8, cb: *block.ColumnBlock) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        const block_id = table.allocateBlockId();
        cb.header.block_id = block_id;
        const bytes = try cb.serialize(self.allocator);
        try self.blocks.put(block_id, .{ .block_id = block_id, .bytes = bytes });
        try col.addBlock(self.allocator, block_id);
        table.estimated_bytes += bytes.len;
        self.total_estimated_bytes += bytes.len;
    }

    fn insertStringBlock(self: *ColumnStore, table_name: []const u8, col_name: []const u8, cb: *block.ColumnBlock) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        const block_id = table.allocateBlockId();
        cb.header.block_id = block_id;
        const bytes = try cb.serialize(self.allocator);
        try self.blocks.put(block_id, .{ .block_id = block_id, .bytes = bytes });
        try col.addBlock(self.allocator, block_id);
        table.estimated_bytes += bytes.len;
        self.total_estimated_bytes += bytes.len;
    }

    // ─── INSERT (batch per column) ─────────────────────────────────────────

    pub fn insertIntColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, values: []const i32) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .int32 and col.data_type != .int64) return error.TypeMismatch;

        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);

        if (self.pending.getPtr(key)) |pv| {
            switch (pv.*) {
                .int32 => |*list| try list.appendSlice(self.allocator, values),
                else => return error.TypeMismatch,
            }
        } else {
            var list = std.ArrayList(i32).empty;
            try list.appendSlice(self.allocator, values);
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.pending.put(owned_key, .{ .int32 = list });
        }

        // Auto-flush when capacity is reached
        if (self.pending.getPtr(key)) |pv| {
            if (pv.int32.items.len >= BLOCK_CAPACITY) {
                try self.flushPendingInt(table_name, col_name, &pv.int32);
            }
        }
        self.evictLRU(table_name);
    }

    pub fn insertFloatColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, values: []const f64) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .float64) return error.TypeMismatch;

        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);

        if (self.pending.getPtr(key)) |pv| {
            switch (pv.*) {
                .float64 => |*list| try list.appendSlice(self.allocator, values),
                else => return error.TypeMismatch,
            }
        } else {
            var list = std.ArrayList(f64).empty;
            try list.appendSlice(self.allocator, values);
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.pending.put(owned_key, .{ .float64 = list });
        }

        if (self.pending.getPtr(key)) |pv| {
            if (pv.float64.items.len >= BLOCK_CAPACITY) {
                try self.flushPendingFloat(table_name, col_name, &pv.float64);
            }
        }
        self.evictLRU(table_name);
    }

    pub fn insertStringColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, values: []const []const u8) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .string) return error.TypeMismatch;

        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);

        if (self.pending.getPtr(key)) |pv| {
            switch (pv.*) {
                .string => |*list| {
                    for (values) |s| {
                        const owned = try self.allocator.dupe(u8, s);
                        errdefer self.allocator.free(owned);
                        try list.append(self.allocator, owned);
                    }
                },
                else => return error.TypeMismatch,
            }
        } else {
            var list = std.ArrayList([]const u8).empty;
            for (values) |s| {
                const owned = try self.allocator.dupe(u8, s);
                errdefer self.allocator.free(owned);
                try list.append(self.allocator, owned);
            }
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.pending.put(owned_key, .{ .string = list });
        }

        if (self.pending.getPtr(key)) |pv| {
            if (pv.string.items.len >= BLOCK_CAPACITY) {
                try self.flushPendingString(table_name, col_name, &pv.string);
            }
        }
        self.evictLRU(table_name);
    }

    pub fn insertBoolColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, values: []const bool) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .bool) return error.TypeMismatch;

        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);

        if (self.pending.getPtr(key)) |pv| {
            switch (pv.*) {
                .bool => |*list| try list.appendSlice(self.allocator, values),
                else => return error.TypeMismatch,
            }
        } else {
            var list = std.ArrayList(bool).empty;
            try list.appendSlice(self.allocator, values);
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.pending.put(owned_key, .{ .bool = list });
        }

        if (self.pending.getPtr(key)) |pv| {
            if (pv.bool.items.len >= BLOCK_CAPACITY) {
                try self.flushPendingBool(table_name, col_name, &pv.bool);
            }
        }
        self.evictLRU(table_name);
    }

    // ─── SELECT (scan all blocks of a column) ──────────────────────────────

    // ─── Fast direct scan (zero-copy, no deserialize) ──────────────────────

    fn readBlockHeader(bytes: []const u8) block.BlockHeader {
        var h: block.BlockHeader = undefined;
        @memcpy(std.mem.asBytes(&h), bytes[0..@sizeOf(block.BlockHeader)]);
        return h;
    }

    pub fn scanIntColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, out: *std.ArrayList(i32)) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .int32 and col.data_type != .int64) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            const vals = try self.decodeIntBlock(slot.bytes);
            defer self.allocator.free(vals);
            try out.appendSlice(self.allocator, vals);
        }

        // Append pending values
        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);
        if (self.pending.get(key)) |pv| {
            switch (pv) {
                .int32 => |list| try out.appendSlice(self.allocator, list.items),
                else => {},
            }
        }
    }

    pub fn scanFloatColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, out: *std.ArrayList(f64)) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .float64) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            const vals = try self.decodeFloatBlock(slot.bytes);
            defer self.allocator.free(vals);
            try out.appendSlice(self.allocator, vals);
        }

        // Append pending values
        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);
        if (self.pending.get(key)) |pv| {
            switch (pv) {
                .float64 => |list| try out.appendSlice(self.allocator, list.items),
                else => {},
            }
        }
    }

    pub fn scanStringColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, out: *std.ArrayList([]const u8)) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .string) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            const strings = try self.decodeStringBlock(slot.bytes);
            // Transfer ownership of each string to the caller; free only the array container
            defer self.allocator.free(strings);
            for (strings) |s| {
                try out.append(self.allocator, s);
            }
        }

        // Append pending values
        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);
        if (self.pending.get(key)) |pv| {
            switch (pv) {
                .string => |list| {
                    for (list.items) |s| {
                        try out.append(self.allocator, try self.allocator.dupe(u8, s));
                    }
                },
                else => {},
            }
        }
    }

    pub fn scanBoolColumn(self: *ColumnStore, table_name: []const u8, col_name: []const u8, out: *std.ArrayList(bool)) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .bool) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            const vals = try self.decodeBoolBlock(slot.bytes);
            defer self.allocator.free(vals);
            try out.appendSlice(self.allocator, vals);
        }

        // Append pending values
        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);
        if (self.pending.get(key)) |pv| {
            switch (pv) {
                .bool => |list| try out.appendSlice(self.allocator, list.items),
                else => {},
            }
        }
    }

    /// Zero-copy string column scan. Returns a StringColumnData where strings
    /// point into contiguous buffers (one per block). Caller must call `deinit`.
    pub fn scanStringColumnData(self: *ColumnStore, table_name: []const u8, col_name: []const u8) !StringColumnData {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .string) return error.TypeMismatch;

        var strings_list = std.ArrayList([]const u8).empty;
        defer strings_list.deinit(self.allocator);
        var buffers_list = std.ArrayList([]u8).empty;
        defer buffers_list.deinit(self.allocator);

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            var view_block = try self.decodeStringBlockView(slot.bytes);
            defer view_block.deinit();

            // Calculate total bytes for this block's strings
            var block_total: usize = 0;
            for (view_block.strings) |s| block_total += s.len;

            if (block_total > 0) {
                const block_buf = try self.allocator.alloc(u8, block_total);
                errdefer self.allocator.free(block_buf);
                try buffers_list.append(self.allocator, block_buf);

                var buf_off: usize = 0;
                for (view_block.strings) |s| {
                    @memcpy(block_buf[buf_off..buf_off + s.len], s);
                    try strings_list.append(self.allocator, block_buf[buf_off..buf_off + s.len]);
                    buf_off += s.len;
                }
            }
        }

        // Include pending strings in a final contiguous buffer
        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);
        if (self.pending.get(key)) |pv| {
            switch (pv) {
                .string => |list| {
                    var pending_total: usize = 0;
                    for (list.items) |s| pending_total += s.len;
                    if (pending_total > 0) {
                        const pending_buf = try self.allocator.alloc(u8, pending_total);
                        errdefer self.allocator.free(pending_buf);
                        try buffers_list.append(self.allocator, pending_buf);
                        var buf_off: usize = 0;
                        for (list.items) |s| {
                            @memcpy(pending_buf[buf_off..buf_off + s.len], s);
                            try strings_list.append(self.allocator, pending_buf[buf_off..buf_off + s.len]);
                            buf_off += s.len;
                        }
                    }
                },
                else => {},
            }
        }

        // Transfer ownership to result
        const strings = try strings_list.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(strings);
        const buffers = try buffers_list.toOwnedSlice(self.allocator);

        return StringColumnData{
            .strings = strings,
            .buffers = buffers,
            .allocator = self.allocator,
        };
    }

    pub fn scanStringColumnDataBlocks(self: *ColumnStore, block_ids: []const u32) !StringColumnData {
        var strings_list = std.ArrayList([]const u8).empty;
        defer strings_list.deinit(self.allocator);
        var buffers_list = std.ArrayList([]u8).empty;
        defer buffers_list.deinit(self.allocator);

        for (block_ids) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            var view_block = try self.decodeStringBlockView(slot.bytes);
            defer view_block.deinit();

            var block_total: usize = 0;
            for (view_block.strings) |s| block_total += s.len;

            if (block_total > 0) {
                const block_buf = try self.allocator.alloc(u8, block_total);
                errdefer self.allocator.free(block_buf);
                try buffers_list.append(self.allocator, block_buf);

                var buf_off: usize = 0;
                for (view_block.strings) |s| {
                    @memcpy(block_buf[buf_off..buf_off + s.len], s);
                    try strings_list.append(self.allocator, block_buf[buf_off..buf_off + s.len]);
                    buf_off += s.len;
                }
            }
        }

        const strings = try strings_list.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(strings);
        const buffers = try buffers_list.toOwnedSlice(self.allocator);

        return StringColumnData{
            .strings = strings,
            .buffers = buffers,
            .allocator = self.allocator,
        };
    }

    // ─── Streaming callback scans (no allocations) ───────────────────────────

    pub fn scanIntColumnCb(self: *ColumnStore, table_name: []const u8, col_name: []const u8, ctx: ?*anyopaque, cb: *const fn (ctx: ?*anyopaque, val: i32) void) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .int32 and col.data_type != .int64) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            const vals = try self.decodeIntBlock(slot.bytes);
            defer self.allocator.free(vals);
            for (vals) |v| cb(ctx, v);
        }
    }

    pub fn scanFloatColumnCb(self: *ColumnStore, table_name: []const u8, col_name: []const u8, ctx: ?*anyopaque, cb: *const fn (ctx: ?*anyopaque, val: f64) void) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .float64) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            const header = readBlockHeader(slot.bytes);
            const payload = slot.bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];
            const vals = std.mem.bytesAsSlice(f64, payload);
            const aligned: []const f64 = @alignCast(@ptrCast(vals));
            for (aligned) |v| cb(ctx, v);
        }
    }

    pub fn scanStringColumnCb(self: *ColumnStore, table_name: []const u8, col_name: []const u8, ctx: ?*anyopaque, cb: *const fn (ctx: ?*anyopaque, val: []const u8) void) !void {
        const table = self.catalog.getTable(table_name) orelse return error.TableNotFound;
        const col = table.getColumn(col_name) orelse return error.ColumnNotFound;
        if (col.data_type != .string) return error.TypeMismatch;

        for (col.block_ids.items) |bid| {
            const slot = self.blocks.get(bid) orelse return error.BlockNotFound;
            var view_block = try self.decodeStringBlockView(slot.bytes);
            defer view_block.deinit();
            for (view_block.strings) |s| {
                cb(ctx, s);
            }
        }

        // Yield pending values
        const key = try self.makePendingKey(table_name, col_name);
        defer self.allocator.free(key);
        if (self.pending.get(key)) |pv| {
            switch (pv) {
                .string => |list| {
                    for (list.items) |s| cb(ctx, s);
                },
                else => {},
            }
        }
    }

    // ─── Block decode helper (int) ─────────────────────────────────────────

    /// Decode a serialized int block: decompress if needed, then decode encoding.
    /// Returns an owned []i32. Caller must free with self.allocator.
    fn decodeIntBlock(self: *ColumnStore, slot_bytes: []const u8) ![]i32 {
        const header = readBlockHeader(slot_bytes);
        const payload = slot_bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

        var decoded_payload: []const u8 = payload;
        var decompressed_buf: ?[]u8 = null;
        defer if (decompressed_buf) |buf| self.allocator.free(buf);

        if (header.compression == .lz4) {
            const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
            const footer = std.mem.bytesToValue(block.BlockFooter, slot_bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
            decompressed_buf = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
            decoded_payload = decompressed_buf.?;
        }

        const flags: encoding.EncodingFlags = @bitCast(header.encoding);
        if (!flags.delta and !flags.bitpack and !flags.rle) {
            // Fast path: raw data — zero copy via slice cast
            const vals = std.mem.bytesAsSlice(i32, decoded_payload[0..header.row_count * @sizeOf(i32)]);
            const result = try self.allocator.alloc(i32, header.row_count);
            errdefer self.allocator.free(result);
            @memcpy(result, vals);
            return result;
        }

        return try encoding.decodeInt32(self.allocator, decoded_payload, flags, header.row_count);
    }

    // ─── Block decode helper (float) ───────────────────────────────────────

    fn decodeFloatBlock(self: *ColumnStore, slot_bytes: []const u8) ![]f64 {
        const header = readBlockHeader(slot_bytes);
        const payload = slot_bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

        var decoded_payload: []const u8 = payload;
        var decompressed_buf: ?[]u8 = null;
        defer if (decompressed_buf) |buf| self.allocator.free(buf);

        if (header.compression == .lz4) {
            const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
            const footer = std.mem.bytesToValue(block.BlockFooter, slot_bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
            decompressed_buf = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
            decoded_payload = decompressed_buf.?;
        }

        // Raw check: if payload size matches raw float array, it's uncompressed
        if (decoded_payload.len == header.row_count * @sizeOf(f64)) {
            const vals = std.mem.bytesAsSlice(f64, decoded_payload[0..header.row_count * @sizeOf(f64)]);
            const result = try self.allocator.alloc(f64, header.row_count);
            errdefer self.allocator.free(result);
            @memcpy(result, vals);
            return result;
        }

        return try encoding.decodeFloat64(self.allocator, decoded_payload, header.row_count);
    }

    // ─── Block decode helper (bool) ────────────────────────────────────────

    fn decodeBoolBlock(self: *ColumnStore, slot_bytes: []const u8) ![]bool {
        const header = readBlockHeader(slot_bytes);
        const payload = slot_bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

        var decoded_payload: []const u8 = payload;
        var decompressed_buf: ?[]u8 = null;
        defer if (decompressed_buf) |buf| self.allocator.free(buf);

        if (header.compression == .lz4) {
            const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
            const footer = std.mem.bytesToValue(block.BlockFooter, slot_bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
            decompressed_buf = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
            decoded_payload = decompressed_buf.?;
        }

        // Raw check: if payload size matches raw bool array, it's uncompressed
        if (decoded_payload.len == header.row_count) {
            const result = try self.allocator.alloc(bool, header.row_count);
            errdefer self.allocator.free(result);
            for (decoded_payload, 0..) |b, i| {
                result[i] = b != 0;
            }
            return result;
        }

        return try encoding.decodeBool(self.allocator, decoded_payload, header.row_count);
    }

    // ─── Block decode helper (string) ──────────────────────────────────────

    /// Decode a serialized string block: decompress if needed, then decode encoding.
    /// Returns an owned [][]const u8 where each inner slice is also allocated.
    /// Caller must free the outer array AND each string with self.allocator.
    fn decodeStringBlock(self: *ColumnStore, slot_bytes: []const u8) ![][]const u8 {
        const header = readBlockHeader(slot_bytes);
        const payload = slot_bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

        var decoded_payload: []const u8 = payload;
        var decompressed_buf: ?[]u8 = null;
        defer if (decompressed_buf) |buf| self.allocator.free(buf);

        if (header.compression == .lz4) {
            const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
            const footer = std.mem.bytesToValue(block.BlockFooter, slot_bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
            decompressed_buf = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
            decoded_payload = decompressed_buf.?;
        }

        const flags: encoding.EncodingFlags = @bitCast(header.encoding);
        return try encoding.decodeString(self.allocator, decoded_payload, flags, header.row_count);
    }

    /// Zero-copy decode of a serialized string block: decompress if needed, then
    /// decode into views. Returns a StringViewBlock. Caller must call `deinit`.
    fn decodeStringBlockView(self: *ColumnStore, slot_bytes: []const u8) !encoding.StringViewBlock {
        const header = readBlockHeader(slot_bytes);
        const payload = slot_bytes[@sizeOf(block.BlockHeader)..][0..header.payload_size];

        var decoded_payload: []const u8 = payload;
        var decompressed_buf: ?[]u8 = null;
        defer if (decompressed_buf) |buf| self.allocator.free(buf);

        if (header.compression == .lz4) {
            const footer_start = @sizeOf(block.BlockHeader) + header.payload_size;
            const footer = std.mem.bytesToValue(block.BlockFooter, slot_bytes[footer_start..footer_start + @sizeOf(block.BlockFooter)]);
            decompressed_buf = try compress.decompressLZ4(self.allocator, payload, footer.uncompressed_size);
            decoded_payload = decompressed_buf.?;
        }

        const flags: encoding.EncodingFlags = @bitCast(header.encoding);
        return try encoding.decodeStringView(self.allocator, decoded_payload, flags, header.row_count);
    }

};

// ─── Tests ─────────────────────────────────────────────────────────────────

test "ColumnStore INSERT and SELECT int" {
    const allocator = std.testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.createTable("events", &[_]ColumnDef{
        .{ .name = "id", .data_type = .int32 },
    });

    const values = &[_]i32{ 10, 20, 30, 40 };
    try store.insertIntColumn("events", "id", values);

    var out = std.ArrayList(i32).empty;
    defer out.deinit(allocator);
    try store.scanIntColumn("events", "id", &out);

    try std.testing.expectEqualSlices(i32, values, out.items);
}

test "ColumnStore INSERT and SELECT float" {
    const allocator = std.testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.createTable("metrics", &[_]ColumnDef{
        .{ .name = "cpu", .data_type = .float64 },
    });

    const values = &[_]f64{ 1.5, 2.5, 3.5 };
    try store.insertFloatColumn("metrics", "cpu", values);

    var out = std.ArrayList(f64).empty;
    defer out.deinit(allocator);
    try store.scanFloatColumn("metrics", "cpu", &out);

    try std.testing.expectEqualSlices(f64, values, out.items);
}

test "ColumnStore INSERT and SELECT string" {
    const allocator = std.testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.createTable("logs", &[_]ColumnDef{
        .{ .name = "msg", .data_type = .string },
    });

    const values = &[_][]const u8{ "hello", "world" };
    try store.insertStringColumn("logs", "msg", values);

    var out = std.ArrayList([]const u8).empty;
    defer {
        for (out.items) |s| allocator.free(s);
        out.deinit(allocator);
    }
    try store.scanStringColumn("logs", "msg", &out);

    try std.testing.expectEqualStrings("hello", out.items[0]);
    try std.testing.expectEqualStrings("world", out.items[1]);
}

test "ColumnStore INSERT and SELECT string dictionary encoded" {
    const allocator = std.testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.createTable("events", &[_]ColumnDef{
        .{ .name = "region", .data_type = .string },
    });

    // Low cardinality → dictionary encoding
    const values = &[_][]const u8{ "us-east", "us-west", "us-east", "us-west", "us-east", "eu-central", "eu-central" };
    try store.insertStringColumn("events", "region", values);

    var out = std.ArrayList([]const u8).empty;
    defer {
        for (out.items) |s| allocator.free(s);
        out.deinit(allocator);
    }
    try store.scanStringColumn("events", "region", &out);

    try std.testing.expectEqualStrings("us-east", out.items[0]);
    try std.testing.expectEqualStrings("us-west", out.items[1]);
    try std.testing.expectEqualStrings("us-east", out.items[2]);
    try std.testing.expectEqualStrings("us-west", out.items[3]);
    try std.testing.expectEqualStrings("us-east", out.items[4]);
    try std.testing.expectEqualStrings("eu-central", out.items[5]);
    try std.testing.expectEqualStrings("eu-central", out.items[6]);
}

test "ColumnStore multiple blocks" {
    const allocator = std.testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.createTable("events", &[_]ColumnDef{
        .{ .name = "id", .data_type = .int32 },
    });

    try store.insertIntColumn("events", "id", &[_]i32{ 1, 2 });
    try store.insertIntColumn("events", "id", &[_]i32{ 3, 4 });

    var out = std.ArrayList(i32).empty;
    defer out.deinit(allocator);
    try store.scanIntColumn("events", "id", &out);

    try std.testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4 }, out.items);
    // total_row_count is caller-managed; block count verifies correctness
}

test "ColumnStore dropTable frees blocks" {
    const allocator = std.testing.allocator;
    var store = ColumnStore.init(allocator);
    defer store.deinit();

    try store.createTable("tmp", &[_]ColumnDef{
        .{ .name = "v", .data_type = .int32 },
    });
    try store.insertIntColumn("tmp", "v", &[_]i32{ 1, 2, 3 });

    try store.dropTable("tmp");
    try std.testing.expect(store.getTable("tmp") == null);
    try std.testing.expectEqual(@as(usize, 0), store.blocks.count());
}
