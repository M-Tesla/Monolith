// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Column Store Catalog — schema metadata for columnar tables.
//!
//! The catalog tracks tables, columns and their block lists.  It is persisted
//! in Region 2 (column_store) and loaded into memory on open.

const std = @import("std");
const block = @import("block.zig");

/// Maximum length for table / column names.
pub const MAX_NAME_LEN: usize = 128;

/// Maximum number of columns per table.
pub const MAX_COLUMNS: usize = 64;

/// Maximum number of blocks per column.
pub const MAX_BLOCKS: usize = 1_000_000;

/// Maximum number of tables in the catalog.
pub const MAX_TABLES: usize = 256;

/// Magic bytes for serialized catalog.
pub const CATALOG_MAGIC: u32 = 0x4D434154; // "MCAT"

/// Current serialization version.
pub const CATALOG_VERSION: u16 = 1;

/// Descriptor for a single column inside a table.
pub const ColumnDescriptor = struct {
    name: []const u8,
    data_type: block.ColumnDataType,
    block_ids: std.ArrayList(u32),

    pub fn init(allocator: std.mem.Allocator, name: []const u8, dt: block.ColumnDataType) ColumnDescriptor {
        return .{
            .name = allocator.dupe(u8, name) catch unreachable,
            .data_type = dt,
            .block_ids = .empty,
        };
    }

    pub fn deinit(self: *ColumnDescriptor, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        self.block_ids.deinit(allocator);
    }

    pub fn addBlock(self: *ColumnDescriptor, allocator: std.mem.Allocator, block_id: u32) !void {
        if (self.block_ids.items.len >= MAX_BLOCKS) return error.TooManyBlocks;
        try self.block_ids.append(allocator, block_id);
    }
};

/// A columnar table: a named collection of columns.
pub const ColumnTable = struct {
    name: []const u8,
    columns: std.ArrayList(ColumnDescriptor),
    total_row_count: u64,
    next_block_id: u32,
    next_rowid: u64,
    estimated_bytes: usize,
    last_accessed_ms: i64,

    pub fn init(allocator: std.mem.Allocator, name: []const u8) ColumnTable {
        return .{
            .name = allocator.dupe(u8, name) catch unreachable,
            .columns = .empty,
            .total_row_count = 0,
            .next_block_id = 0,
            .next_rowid = 1,
            .estimated_bytes = 0,
            .last_accessed_ms = 0,
        };
    }

    pub fn deinit(self: *ColumnTable, allocator: std.mem.Allocator) void {
        for (self.columns.items) |*col| {
            col.deinit(allocator);
        }
        self.columns.deinit(allocator);
        allocator.free(self.name);
    }

    pub fn addColumn(self: *ColumnTable, allocator: std.mem.Allocator, col_name: []const u8, dt: block.ColumnDataType) !void {
        if (self.columns.items.len >= MAX_COLUMNS) return error.TooManyColumns;
        for (self.columns.items) |c| {
            if (std.mem.eql(u8, c.name, col_name)) return error.DuplicateColumn;
        }
        try self.columns.append(allocator, ColumnDescriptor.init(allocator, col_name, dt));
    }

    pub fn getColumn(self: *ColumnTable, col_name: []const u8) ?*ColumnDescriptor {
        for (self.columns.items) |*c| {
            if (std.mem.eql(u8, c.name, col_name)) return c;
        }
        return null;
    }

    pub fn allocateBlockId(self: *ColumnTable) u32 {
        const id = self.next_block_id;
        self.next_block_id += 1;
        return id;
    }
};

/// In-memory catalog of all columnar tables.
pub const ColumnCatalog = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMap(*ColumnTable),

    pub fn init(allocator: std.mem.Allocator) ColumnCatalog {
        return .{
            .allocator = allocator,
            .tables = std.StringHashMap(*ColumnTable).init(allocator),
        };
    }

    pub fn deinit(self: *ColumnCatalog) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.tables.deinit();
    }

    pub fn createTable(self: *ColumnCatalog, name: []const u8) !*ColumnTable {
        if (self.tables.count() >= MAX_TABLES) return error.TooManyTables;
        if (self.tables.contains(name)) return error.TableAlreadyExists;

        const table = try self.allocator.create(ColumnTable);
        table.* = ColumnTable.init(self.allocator, name);
        try self.tables.put(table.name, table);
        return table;
    }

    pub fn dropTable(self: *ColumnCatalog, name: []const u8) !void {
        const entry = self.tables.getEntry(name) orelse return error.TableNotFound;
        const table = entry.value_ptr.*;
        _ = self.tables.remove(name);
        table.deinit(self.allocator);
        self.allocator.destroy(table);
    }

    pub fn getTable(self: *ColumnCatalog, name: []const u8) ?*ColumnTable {
        return self.tables.get(name);
    }

    // ─── Serialization ─────────────────────────────────────────────────────

    /// Serialize the entire catalog into a newly allocated buffer.
    /// Caller owns the returned slice.
    pub fn serialize(self: *const ColumnCatalog) ![]u8 {
        var list: std.ArrayList(u8) = .empty;
        errdefer list.deinit(self.allocator);

        // Header
        try writeInt(&list, self.allocator, u32, CATALOG_MAGIC);
        try writeInt(&list, self.allocator, u16, CATALOG_VERSION);
        try writeInt(&list, self.allocator, u16, @intCast(self.tables.count()));

        // Tables
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const table = entry.value_ptr.*;
            try writeInt(&list, self.allocator, u16, @intCast(table.name.len));
            try list.appendSlice(self.allocator, table.name);
            try writeInt(&list, self.allocator, u64, table.total_row_count);
            try writeInt(&list, self.allocator, u32, table.next_block_id);
            try writeInt(&list, self.allocator, u16, @intCast(table.columns.items.len));

            for (table.columns.items) |col| {
                try writeInt(&list, self.allocator, u16, @intCast(col.name.len));
                try list.appendSlice(self.allocator, col.name);
                try writeInt(&list, self.allocator, u8, @intFromEnum(col.data_type));
                try writeInt(&list, self.allocator, u16, @intCast(col.block_ids.items.len));
                for (col.block_ids.items) |bid| {
                    try writeInt(&list, self.allocator, u32, bid);
                }
            }
        }

        return list.toOwnedSlice(self.allocator);
    }

    /// Deserialize a catalog from a byte buffer.
    /// Replaces all current state.
    pub fn deserialize(self: *ColumnCatalog, bytes: []const u8) !void {
        // Clear existing state
        var old_it = self.tables.iterator();
        while (old_it.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.tables.clearRetainingCapacity();

        var off: usize = 0;
        const magic = readInt(u32, bytes, &off);
        if (magic != CATALOG_MAGIC) return error.InvalidCatalogMagic;

        const version = readInt(u16, bytes, &off);
        if (version != CATALOG_VERSION) return error.UnsupportedCatalogVersion;

        const table_count = readInt(u16, bytes, &off);

        for (0..table_count) |_| {
            const name_len = readInt(u16, bytes, &off);
            const name = bytes[off..][0..name_len];
            off += name_len;

            const total_row_count = readInt(u64, bytes, &off);
            const next_block_id = readInt(u32, bytes, &off);
            const col_count = readInt(u16, bytes, &off);

            const table = try self.allocator.create(ColumnTable);
            table.* = ColumnTable.init(self.allocator, name);
            table.total_row_count = total_row_count;
            table.next_block_id = next_block_id;

            for (0..col_count) |_| {
                const col_name_len = readInt(u16, bytes, &off);
                const col_name = bytes[off..][0..col_name_len];
                off += col_name_len;
                const dt_raw = readInt(u8, bytes, &off);
                const dt: block.ColumnDataType = @enumFromInt(dt_raw);

                var col = ColumnDescriptor.init(self.allocator, col_name, dt);

                const block_count = readInt(u16, bytes, &off);
                for (0..block_count) |_| {
                    const bid = readInt(u32, bytes, &off);
                    try col.block_ids.append(self.allocator, bid);
                }
                try table.columns.append(self.allocator, col);
            }

            try self.tables.put(table.name, table);
        }
    }
};

/// Append a little-endian integer to an ArrayList(u8).
fn writeInt(list: *std.ArrayList(u8), gpa: std.mem.Allocator, comptime T: type, value: T) !void {
    const bytes = std.mem.toBytes(std.mem.nativeToLittle(T, value));
    try list.appendSlice(gpa, &bytes);
}

/// Read a little-endian integer from a byte buffer at offset, advancing it.
fn readInt(comptime T: type, bytes: []const u8, off: *usize) T {
    const size = @sizeOf(T);
    const val = std.mem.readInt(T, bytes[off.*..][0..size], .little);
    off.* += size;
    return val;
}

// ─── Tests ─────────────────────────────────────────────────────────────────

test "Catalog create table and column" {
    const allocator = std.testing.allocator;
    var catalog = ColumnCatalog.init(allocator);
    defer catalog.deinit();

    const table = try catalog.createTable("events");
    try std.testing.expectEqual(@as(usize, 1), catalog.tables.count());

    try table.addColumn(allocator, "timestamp", .int64);
    try table.addColumn(allocator, "level", .string);
    try std.testing.expectEqual(@as(usize, 2), table.columns.items.len);

    const col = table.getColumn("level").?;
    try std.testing.expectEqual(block.ColumnDataType.string, col.data_type);
}

test "Catalog round-trip serialization" {
    const allocator = std.testing.allocator;
    var catalog = ColumnCatalog.init(allocator);
    defer catalog.deinit();

    // Build catalog
    const table = try catalog.createTable("metrics");
    try table.addColumn(allocator, "cpu", .float64);
    try table.addColumn(allocator, "host", .string);

    const cpu_col = table.getColumn("cpu").?;
    try cpu_col.addBlock(allocator, 0);
    try cpu_col.addBlock(allocator, 1);
    try cpu_col.addBlock(allocator, 2);

    // Serialize
    const bytes = try catalog.serialize();
    defer allocator.free(bytes);

    // Deserialize into fresh catalog
    var catalog2 = ColumnCatalog.init(allocator);
    defer catalog2.deinit();
    try catalog2.deserialize(bytes);

    try std.testing.expectEqual(@as(usize, 1), catalog2.tables.count());
    const table2 = catalog2.getTable("metrics").?;
    try std.testing.expectEqual(@as(usize, 2), table2.columns.items.len);

    const cpu2 = table2.getColumn("cpu").?;
    try std.testing.expectEqual(block.ColumnDataType.float64, cpu2.data_type);
    try std.testing.expectEqual(@as(usize, 3), cpu2.block_ids.items.len);
    try std.testing.expectEqual(@as(u32, 0), cpu2.block_ids.items[0]);
    try std.testing.expectEqual(@as(u32, 2), cpu2.block_ids.items[2]);
}

test "Catalog drop table" {
    const allocator = std.testing.allocator;
    var catalog = ColumnCatalog.init(allocator);
    defer catalog.deinit();

    _ = try catalog.createTable("a");
    _ = try catalog.createTable("b");
    try std.testing.expectEqual(@as(usize, 2), catalog.tables.count());

    try catalog.dropTable("a");
    try std.testing.expectEqual(@as(usize, 1), catalog.tables.count());
    try std.testing.expect(catalog.getTable("a") == null);
    try std.testing.expect(catalog.getTable("b") != null);
}

test "Catalog duplicate table error" {
    const allocator = std.testing.allocator;
    var catalog = ColumnCatalog.init(allocator);
    defer catalog.deinit();

    _ = try catalog.createTable("x");
    const result = catalog.createTable("x");
    try std.testing.expectError(error.TableAlreadyExists, result);
}

test "Catalog allocate block id" {
    const allocator = std.testing.allocator;
    var catalog = ColumnCatalog.init(allocator);
    defer catalog.deinit();

    const table = try catalog.createTable("logs");
    try std.testing.expectEqual(@as(u32, 0), table.allocateBlockId());
    try std.testing.expectEqual(@as(u32, 1), table.allocateBlockId());
    try std.testing.expectEqual(@as(u32, 2), table.next_block_id);
}
