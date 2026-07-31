// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Region Directory — multi-region storage layout for Monolith v4+.
//!
//! Monolith files are divided into regions:
//!   Region 1: Row Store (B+tree, existing)
//!   Region 2: Column Store (compressed blocks)
//!   Region 3: Lake Catalog (snapshots, partitions, stats)
//!
//! The region directory lives at page 4 (immediately after meta0, meta1,
//! main_root, gc_root).  On v3 files (reserve16 == 0) the directory does not
//! exist and the entire file is treated as a single Row Store region.

const std = @import("std");
const consts = @import("core/consts.zig");
const page = @import("page/page.zig");

/// Magic for the region-directory page header.
pub const REGION_DIR_MAGIC: u64 = 0x4D4F4E4F52454744; // "MONOREGD"

/// Maximum number of regions supported.
pub const MAX_REGIONS: usize = 8;

/// Types of storage regions inside a .monolith file.
pub const RegionType = enum(u8) {
    row_store = 1,
    column_store = 2,
    lake_catalog = 3,
    _,
};

/// Descriptor for a single region.
pub const RegionDescriptor = extern struct {
    /// Byte offset of the region from the start of the file.
    offset: u64,
    /// Size of the region in bytes (0 = not allocated yet).
    size: u64,
    /// Region type (see RegionType).
    type: u8,
    /// Format version of this region (starts at 1).
    version: u8,
    /// Reserved / padding.
    reserved: [6]u8,
};

/// Region directory stored at page 4.
pub const RegionDirectory = extern struct {
    /// Must equal REGION_DIR_MAGIC.
    magic: u64,
    /// Number of active regions.
    region_count: u32,
    /// Reserved.
    reserved: u32,
    /// Fixed-size array of region descriptors.
    regions: [MAX_REGIONS]RegionDescriptor,

    /// Validate magic and bounds.
    pub fn validate(self: *const RegionDirectory) bool {
        if (self.magic != REGION_DIR_MAGIC) return false;
        if (self.region_count > MAX_REGIONS) return false;
        return true;
    }

    /// Find a region by type, or null if not present.
    pub fn find(self: *const RegionDirectory, rtype: RegionType) ?*const RegionDescriptor {
        for (0..self.region_count) |i| {
            if (self.regions[i].type == @intFromEnum(rtype)) {
                return &self.regions[i];
            }
        }
        return null;
    }

    /// Find a region by type, returning a mutable pointer.
    pub fn findMut(self: *RegionDirectory, rtype: RegionType) ?*RegionDescriptor {
        for (0..self.region_count) |i| {
            if (self.regions[i].type == @intFromEnum(rtype)) {
                return &self.regions[i];
            }
        }
        return null;
    }

    /// Add a new region descriptor.
    pub fn add(self: *RegionDirectory, desc: RegionDescriptor) !void {
        if (self.region_count >= MAX_REGIONS) return error.TooManyRegions;
        self.regions[self.region_count] = desc;
        self.region_count += 1;
    }

    /// Initialise an empty directory with only the Row Store region.
    pub fn initRowOnly(self: *RegionDirectory, file_size: u64) void {
        self.magic = REGION_DIR_MAGIC;
        self.region_count = 1;
        self.reserved = 0;
        self.regions = std.mem.zeroes([MAX_REGIONS]RegionDescriptor);
        self.regions[0] = .{
            .offset = 0,
            .size = file_size,
            .type = @intFromEnum(RegionType.row_store),
            .version = 1,
            .reserved = .{0} ** 6,
        };
    }

    /// Initialise a directory with Row + Column + Lake regions.
    /// Column and Lake start with size 0; they are grown on first use.
    pub fn initHybrid(self: *RegionDirectory, row_size: u64) void {
        self.magic = REGION_DIR_MAGIC;
        self.region_count = 3;
        self.reserved = 0;
        self.regions = std.mem.zeroes([MAX_REGIONS]RegionDescriptor);
        // Row Store at offset 0
        self.regions[0] = .{
            .offset = 0,
            .size = row_size,
            .type = @intFromEnum(RegionType.row_store),
            .version = 1,
            .reserved = .{0} ** 6,
        };
        // Column Store starts empty, immediately after row store
        self.regions[1] = .{
            .offset = row_size,
            .size = 0,
            .type = @intFromEnum(RegionType.column_store),
            .version = 1,
            .reserved = .{0} ** 6,
        };
        // Lake Catalog starts empty, immediately after column store
        self.regions[2] = .{
            .offset = row_size,
            .size = 0,
            .type = @intFromEnum(RegionType.lake_catalog),
            .version = 1,
            .reserved = .{0} ** 6,
        };
    }
};

test "RegionDirectory round-trip" {
    var rd: RegionDirectory = undefined;
    rd.initHybrid(4096 * 10);
    try std.testing.expect(rd.validate());
    try std.testing.expectEqual(@as(u32, 3), rd.region_count);

    const row = rd.find(.row_store).?;
    try std.testing.expectEqual(@as(u64, 0), row.offset);
    try std.testing.expectEqual(@as(u64, 4096 * 10), row.size);

    const col = rd.find(.column_store).?;
    try std.testing.expectEqual(@as(u64, 4096 * 10), col.offset);
    try std.testing.expectEqual(@as(u64, 0), col.size);

    const lake = rd.find(.lake_catalog).?;
    try std.testing.expectEqual(@as(u64, 4096 * 10), lake.offset);
    try std.testing.expectEqual(@as(u64, 0), lake.size);
}
