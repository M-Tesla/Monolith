// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Basic primitive types for libmonolith-zig

const std = @import("std");

/// Transaction ID (u64 per monolith-internals.h)
pub const txnid_t = u64;

/// Page Number (u32 per monolith-internals.h)
pub const pgno_t = u32;

/// Index within a page (u16 per monolith-internals.h)
pub const indx_t = u16;

/// B-Tree statistics structure (tree_t)
pub const Tree = extern struct {
    flags: u16,
    height: u16,
    dupfix_size: u32,
    root: pgno_t,
    branch_pages: pgno_t,
    leaf_pages: pgno_t,
    large_pages: pgno_t,
    sequence: u64,
    items: u64,
    mod_txnid: txnid_t,
};

/// Canary structure for integrity protection (monolith_canary)
pub const Canary = extern struct {
    x: u64,
    y: u64,
    z: u64,
    v: u64,
};

/// Structure for 128-bit ID (bin128_t)
pub const Bin128 = extern struct {
    lo: u64,
    hi: u64,
};

/// Database geometry structure (geo_t)
pub const Geo = extern struct {
    grow_pv: u16,
    shrink_pv: u16,
    lower: pgno_t,
    upper: pgno_t,
    current: pgno_t, // Union: now / end_pgno
    first_unallocated: pgno_t,
};

/// Database Open Modes
pub const EnvFlags = packed struct {
    nosubdir: bool = false,
    readonly: bool = false,
    writemap: bool = false,
    metasync: bool = true,
    sync: bool = true,
    mapasync: bool = false,
    tls: bool = true,
    lock: bool = true,
    exclusive: bool = false,
    _pad: u55 = 0,
};

/// Database Flags (DBI)
pub const DbFlags = packed struct {
    reversekey: bool = false, // 0x02
    dupsort: bool = false,    // 0x04
    integerkey: bool = false, // 0x08
    dupfixed: bool = false,   // 0x10
    integerdup: bool = false, // 0x20
    reversedup: bool = false, // 0x40
    create: bool = false,     // 0x40000 (monolith_CREATE) - Not really a DB flag persisted, but used in open
    _pad: u9 = 0,
    
    pub fn toU16(self: DbFlags) u16 {
        var val: u16 = 0;
        if (self.reversekey) val |= 0x02;
        if (self.dupsort) val |= 0x04;
        if (self.integerkey) val |= 0x08;
        if (self.dupfixed) val |= 0x10;
        if (self.integerdup) val |= 0x20;
        if (self.reversedup) val |= 0x40;
        return val;
    }

    pub fn fromU16(val: u16) DbFlags {
        return .{
            .reversekey = (val & 0x02) != 0,
            .dupsort = (val & 0x04) != 0,
            .integerkey = (val & 0x08) != 0,
            .dupfixed = (val & 0x10) != 0,
            .integerdup = (val & 0x20) != 0,
            .reversedup = (val & 0x40) != 0,
        };
    }
};
