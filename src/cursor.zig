// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Cursor re-export.
//! The Cursor type lives in txn.zig to avoid circular imports.
pub const Cursor = @import("txn.zig").Cursor;
