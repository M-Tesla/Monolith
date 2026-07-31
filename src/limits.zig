// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Engine-level compile-time limits for Monolith.

const consts = @import("core/consts.zig");

/// Compiled page size (bytes).
pub const page_size: usize = consts.DATAPAGESIZE;

/// Maximum key length that fits inline on a page (half page minus node header).
pub const max_key_size: usize = page_size / 2 - 8;

/// Maximum inline value length (same constraint as max_key_size).
/// Values larger than this are stored in overflow pages automatically,
/// but the effective structural maximum is the same.
pub const max_val_size: usize = page_size / 2 - 8;

/// Maximum number of named databases (DBI slots) per environment.
pub const max_dbs: usize = 128;

/// Maximum page count given a 32-bit pgno (pgno_invalid = maxInt(u32)).
pub const max_pages: usize = 0xFFFF_FFFE;
