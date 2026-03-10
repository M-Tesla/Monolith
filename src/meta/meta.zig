// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Meta pages and database control header.

const std = @import("std");
const types = @import("../core/types.zig");
const consts = @import("../core/consts.zig");
const page = @import("../page/page.zig");

/// Meta page payload — stored immediately after the 20-byte PageHeader.
/// Two meta pages (slots 0 and 1) are maintained in a ping-pong fashion
/// to guarantee atomic commits.
pub const Meta = extern struct {
    /// Magic number and format version
    magic_and_version: u64 align(4),

    /// Transaction ID written first (commit begin)
    txnid_a: u64 align(4),

    /// Reserved 16-bit field
    reserve16: u16,

    /// Validator identifier
    validator_id: u8,

    /// Extra page-header bytes (signed, normally 0)
    extra_pagehdr: i8,

    /// Database geometry (page counts, growth limits)
    geometry: types.Geo,

    /// B-tree roots: GC tree and main catalog tree
    trees: extern struct {
        gc: types.Tree,
        main: types.Tree,
    } align(4),

    /// Integrity canary (four u64 counters)
    canary: types.Canary align(4),

    /// Commit signature / checksum
    sign: u64 align(4),

    /// Transaction ID written last (commit end) — must equal txnid_a for a clean commit
    txnid_b: u64 align(4),

    /// Retired page counters (two slots)
    pages_retired: [2]u32,

    /// Boot identifier (128-bit random, stable across process restarts)
    bootid: types.Bin128 align(4),

    /// Data-file identifier (128-bit random, unique per database file)
    dxbid: types.Bin128 align(4),

    /// Returns true if the magic matches the expected constant.
    pub fn validate(self: *const Meta) bool {
        return self.magic_and_version == consts.MAGIC;
    }
};

test "Meta size fits in minimum page" {
    try std.testing.expect(@sizeOf(Meta) < consts.MIN_PAGESIZE);
}
