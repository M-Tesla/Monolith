// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Fundamental constants for libmonolith-zig

/// Magic signature for data files (0x59659DBDEF4C11 per monolith-internals.h)
pub const MAGIC: u64 = 0x59659DBDEF4C11;

/// File format version
pub const VERSION: u32 = 3;

/// Minimum page size
pub const MIN_PAGESIZE: u32 = 256;
/// Maximum page size
pub const MAX_PAGESIZE: u32 = 65536;
/// Default page size
pub const DATAPAGESIZE: u32 = 4096;

/// Minimum key size
pub const MIN_KEYSIZE: u32 = 0;

/// Maximum B-Tree depth (to avoid infinite recursion/stack overflow)
pub const MAX_DEPTH: u32 = 64;

/// Invalid/Null Transaction ID
pub const TXID_INVALID: u64 = 0;

/// Invalid page number
pub const PGNO_INVALID: u32 = @import("std").math.maxInt(u32);
