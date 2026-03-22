// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Transaction and Cursor — pure Zig, no C.
//! MVCC B-tree engine with a dirty-page list.
//!
//! Write strategy:
//!   - Pages are read directly from the mmap.
//!   - Writes go to heap buffers (dirty_list) indexed by pgno.
//!   - Commit: copies dirty pages → mmap, updates the ping-pong meta slot, syncs.
//!   - Abort: discards heap buffers; mmap is unchanged.
//!   - Committed pages touched by a write are path-copied to fresh pgnos,
//!     leaving the original mmap pages intact for concurrent readers.
//!     This provides true MVCC snapshot isolation.
//!
//! DupSort via composite keys:
//!   put(dbi, key, val)  → stores "key||val" as B-tree key, val=""
//!   find(key)           → lower-bound + prefix check
//!   nextDup()           → advances while prefix matches
//!   countDups()         → counts entries with the same prefix

const std = @import("std");
const env_mod = @import("env.zig");
const meta_mod = @import("meta/meta.zig");
const page_mod = @import("page/page.zig");
const core_types = @import("core/types.zig");
const consts = @import("core/consts.zig");
const types = @import("types.zig");
const limits = @import("limits.zig");

const PAGE_SIZE: usize = consts.DATAPAGESIZE; // 4096

// Maximum value that can be stored inline on a leaf page.
// Values larger than this threshold are stored in dedicated overflow pages.
// Each overflow page holds PAGE_SIZE-20 bytes of data (after the page header).
const OVERFLOW_THRESHOLD: usize = PAGE_SIZE / 2; // 2048 bytes

// Usable data bytes per overflow page (page body after the 20-byte PageHeader).
const OVERFLOW_DATA_PER_PAGE: usize = PAGE_SIZE - 20;

// Zig 0.15: ArrayList and AutoHashMap no longer store an allocator internally.
// The "Unmanaged" variants are used; the allocator is passed per call.
const DirtyList = std.AutoHashMapUnmanaged(u32, []u8);
const FreedPages = std.ArrayListUnmanaged(u32);

// Branch traversal record used during btreePut to propagate splits upward.
const PathEntry = struct { pgno: u32, ci: u16 };

// On-page size of a node (key + value + 8-byte header, rounded up to even).
fn nodeSz(klen: usize, vlen: usize) usize {
    var s: usize = 8 + klen + vlen;
    if (s & 1 != 0) s += 1;
    return s;
}

// Fill-factor threshold below which a page becomes a merge candidate (50%).
const REBALANCE_THRESHOLD: usize = PAGE_SIZE / 2;
/// Dirty page count above which trySpill() moves pages to the cold spill list.
const SPILL_THRESHOLD: usize = 64;

/// Given a binary-search result on a branch page, returns the child index to descend into.
/// Branch convention: leftmost entry has key=""; if no match and index==0, caller must handle separately.
inline fn branchCi(result: page_mod.PageHeader.SearchResult) u16 {
    return if (result.match) result.index else if (result.index > 0) result.index - 1 else 0;
}

/// Removes entry `ci` from a branch page, preserving the leftmost-key="" convention.
/// When ci==0, the next entry takes over as leftmost with its key rewritten to "".
fn removeParentEntry(parent: *page_mod.PageHeader, ci: u16) !void {
    if (ci == 0 and parent.getNumEntries() >= 2) {
        // Save the child pgno before any deletion.
        const next_pgno = parent.getNode(1).getChildPgno();
        parent.delNode(0); // remove ("", p_old)
        parent.delNode(0); // remove (k1, p_new) — now at index 0
        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, next_pgno, .little);
        _ = try parent.putNode(0, "", &buf, 0); // reinsert as ("", p_new)
    } else {
        parent.delNode(ci);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Key comparators
// ─────────────────────────────────────────────────────────────────────────────

/// Re-export so callers only need to import txn.zig.
pub const CmpFn = page_mod.CmpFn;

/// Default comparator: standard lexicographic byte order.
pub const cmpLexicographic: CmpFn = page_mod.cmpDefault;

/// ReverseKey: descending lexicographic order (swap a and b).
pub fn cmpReverse(a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, b, a);
}

/// IntegerKey: interpret keys as native-endian u64 (8-byte) or u32 (4-byte)
/// and compare numerically.
pub fn cmpIntegerKey(a: []const u8, b: []const u8) std.math.Order {
    if (a.len == 8 and b.len == 8) {
        const va = std.mem.readInt(u64, a[0..8], .little);
        const vb = std.mem.readInt(u64, b[0..8], .little);
        return std.math.order(va, vb);
    }
    if (a.len == 4 and b.len == 4) {
        const va = std.mem.readInt(u32, a[0..4], .little);
        const vb = std.mem.readInt(u32, b[0..4], .little);
        return std.math.order(va, vb);
    }
    return std.mem.order(u8, a, b); // fallback
}

// ─────────────────────────────────────────────────────────────────────────────
// DbFlags ↔ u16 helpers  (flag persistence in Tree.flags)
// ─────────────────────────────────────────────────────────────────────────────

/// Sentinel bit: when set, Tree.flags was written by Fase-25-aware code.
/// Without it (legacy DBs or DBs with all-false flags pre-Fase-25), we skip
/// compatibility validation and trust the caller's flags instead.
const DB_FLAGS_VALID: u16 = 1 << 15;

fn dbFlagsToU16(flags: types.DbFlags) u16 {
    var v: u16 = DB_FLAGS_VALID;
    if (flags.dupsort) v |= 1 << 0;
    if (flags.integerkey) v |= 1 << 1;
    if (flags.dupfixed) v |= 1 << 2;
    if (flags.integerdup) v |= 1 << 3;
    if (flags.reversekey) v |= 1 << 4;
    if (flags.reversedup) v |= 1 << 5;
    return v;
}

fn u16ToDbFlags(v: u16) types.DbFlags {
    return .{
        .dupsort = (v & (1 << 0)) != 0,
        .integerkey = (v & (1 << 1)) != 0,
        .dupfixed = (v & (1 << 2)) != 0,
        .integerdup = (v & (1 << 3)) != 0,
        .reversekey = (v & (1 << 4)) != 0,
        .reversedup = (v & (1 << 5)) != 0,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-transaction DBI state
// ─────────────────────────────────────────────────────────────────────────────

const DbiState = struct {
    tree: core_types.Tree,
    flags: types.DbFlags,
    cmp_fn: CmpFn = cmpLexicographic,
    name_buf: [256]u8,
    name_len: u8,
    open: bool,
    dirty: bool, // tree was modified in this transaction
};

// ─────────────────────────────────────────────────────────────────────────────
// Transaction
// ─────────────────────────────────────────────────────────────────────────────

pub const Transaction = struct {
    env: *env_mod.Environment,
    parent: ?*Transaction,
    txnid: u64,
    rdonly: bool,
    meta: meta_mod.Meta, // mutable copy of the selected meta slot
    dirty_list: DirtyList, // pgno → PAGE_SIZE heap buffer (hot)
    spill_list: DirtyList, // pgno → PAGE_SIZE heap buffer (cold)
    /// WRITEMAP mode: original content of existing pages, for abort roll-back.
    shadow_list: DirtyList,
    /// WRITEMAP mode: set of page numbers written this transaction (for checksums).
    dirty_pages_wm: std.AutoHashMapUnmanaged(u32, void),
    /// Snapshot of first_unallocated at transaction start (WRITEMAP abort needs it).
    first_unallocated_snapshot: u32,
    freed_pages: FreedPages,
    /// Pages freed from committed state (pgno < first_unallocated_snapshot).
    /// These are sent to GC on commit but MUST NOT be reused within this transaction
    /// because concurrent readers may still be navigating to them via the mmap.
    committed_freed: FreedPages,
    dbi_state: []DbiState, // [0..max_dbs)
    next_dbi: u32, // next free DBI slot (starts at 1)
    reader_slot: ?usize,
    committed: bool,
    writer_held: bool,
    allocator: std.mem.Allocator,
    /// Reusable heap buffer for reading large (overflow) values.
    /// Freed in cleanup(); slice into this buffer is valid until the next overflow read.
    overflow_buf: []u8,
    /// Pages reclaimed from the GC tree; safe to reuse (oldest_reader-verified).
    reclaimed_pages: FreedPages,
    /// Guard: true while loadGCBatch is running — prevents re-entrant GC reclaim.
    loading_gc: bool,
    /// Per-txn nosync override: if true, skip fsync on this commit regardless of env setting.
    nosync: bool,

    // ─── Lifecycle ───────────────────────────────────────────────────────────

    pub fn begin(
        env: *env_mod.Environment,
        parent: ?*Transaction,
        flags: types.TxnFlags,
    ) !Transaction {
        const alloc = std.heap.page_allocator;
        const rdonly = flags.rdonly;

        // ── Child transaction: inherits writer lock and reader slot from parent ─
        if (parent) |par| {
            if (par.rdonly) return error.BadTxn;
            const dbi_state = try alloc.alloc(DbiState, env.max_dbs);
            errdefer alloc.free(dbi_state);
            @memcpy(dbi_state, par.dbi_state);
            return Transaction{
                .env = env,
                .parent = parent,
                .txnid = par.txnid,
                .rdonly = rdonly,
                .meta = par.meta,
                .dirty_list = .{},
                .spill_list = .{},
                .shadow_list = .{},
                .dirty_pages_wm = .{},
                .first_unallocated_snapshot = par.meta.geometry.first_unallocated,
                .freed_pages = .{},
                .committed_freed = .{},
                .dbi_state = dbi_state,
                .next_dbi = env.dbi_next,
                .reader_slot = null,
                .committed = false,
                .writer_held = false,
                .allocator = alloc,
                .overflow_buf = &.{},
                .reclaimed_pages = .{},
                .loading_gc = false,
                .nosync = flags.nosync,
            };
        }

        // ── Root transaction ──────────────────────────────────────────────────
        var writer_held = false;
        if (!rdonly) {
            if (flags.try_begin) {
                try env.lock.tryLockWriter();
            } else {
                try env.lock.lockWriter();
            }
            writer_held = true;
            // Another process may have grown the file while we were waiting
            // for the lock. Remap before touching any pages.
            try env.syncMapToFile();
        } else {
            // Read transactions also need an up-to-date mapping so that pages
            // referenced by the chosen meta snapshot are accessible.
            try env.syncMapToFile();
        }
        errdefer if (writer_held) env.lock.unlockWriter() catch |e|
            std.log.err("unlockWriter failed during txn begin cleanup: {}", .{e});

        const best_meta = env.bestMeta();
        const txnid: u64 = if (rdonly) best_meta.txnid_a else best_meta.txnid_a + 1;

        var reader_slot: ?usize = null;
        if (rdonly) {
            reader_slot = try env.lock.registerReader(txnid);
        }
        errdefer if (reader_slot) |s| env.lock.unregisterReader(s);

        const dbi_state = try alloc.alloc(DbiState, env.max_dbs);
        errdefer alloc.free(dbi_state);
        for (dbi_state) |*s| s.* = .{ .tree = std.mem.zeroes(core_types.Tree), .flags = .{}, .cmp_fn = cmpLexicographic, .name_buf = undefined, .name_len = 0, .open = false, .dirty = false };

        return Transaction{
            .env = env,
            .parent = null,
            .txnid = txnid,
            .rdonly = rdonly,
            .meta = best_meta.*,
            .dirty_list = .{},
            .spill_list = .{},
            .shadow_list = .{},
            .dirty_pages_wm = .{},
            .first_unallocated_snapshot = best_meta.geometry.first_unallocated,
            .freed_pages = .{},
            .committed_freed = .{},
            .dbi_state = dbi_state,
            .next_dbi = env.dbi_next,
            .reader_slot = reader_slot,
            .committed = false,
            .writer_held = writer_held,
            .allocator = alloc,
            .overflow_buf = &.{},
            .reclaimed_pages = .{},
            .loading_gc = false,
            .nosync = flags.nosync,
        };
    }

    pub fn abort(self: *Transaction) void {
        if (self.committed) return;
        // In writemap mode, restore original data for existing pages that were
        // modified in-place. New pages were in dirty_list (heap) and get freed
        // by cleanup(); the committed meta still has the old first_unallocated.
        if (self.env.writemap and !self.rdonly) {
            var it = self.shadow_list.iterator();
            while (it.next()) |entry| {
                const pgno = entry.key_ptr.*;
                const old_data = entry.value_ptr.*;
                const offset = @as(usize, pgno) * PAGE_SIZE;
                @memcpy(self.env.map.ptr[offset .. offset + PAGE_SIZE], old_data[0..PAGE_SIZE]);
            }
        }
        self.cleanup();
    }

    pub fn commit(self: *Transaction) !void {
        if (self.rdonly) {
            self.cleanup();
            return;
        }

        // ── Child commit: merge dirty state into parent ───────────────────────
        if (self.parent) |par| {
            try self.flushDbiState();

            // Transfer dirty page buffers to parent (ownership moves; no free).
            var it = self.dirty_list.iterator();
            while (it.next()) |entry| {
                if (par.dirty_list.get(entry.key_ptr.*)) |old_buf| {
                    par.allocator.free(old_buf);
                    _ = par.dirty_list.remove(entry.key_ptr.*);
                }
                try par.dirty_list.put(par.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }
            // Transfer spill pages to parent as well.
            var sit = self.spill_list.iterator();
            while (sit.next()) |entry| {
                if (par.spill_list.get(entry.key_ptr.*)) |old_buf| {
                    par.allocator.free(old_buf);
                    _ = par.spill_list.remove(entry.key_ptr.*);
                }
                try par.spill_list.put(par.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }
            // Free the hash-map structures only; values are now owned by parent.
            self.dirty_list.deinit(self.allocator);
            self.dirty_list = .{};
            self.spill_list.deinit(self.allocator);
            self.spill_list = .{};

            // Merge freed pages into parent.
            try par.freed_pages.appendSlice(par.allocator, self.freed_pages.items);
            // Merge committed_freed into parent — those pages must not be reused
            // until the root transaction commits and advances the GC.
            try par.committed_freed.appendSlice(par.allocator, self.committed_freed.items);

            // Propagate updated meta and DBI state to parent.
            par.meta = self.meta;
            par.next_dbi = self.next_dbi;
            @memcpy(par.dbi_state, self.dbi_state);

            self.committed = true;
            self.cleanup();
            return;
        }

        // ── Root commit ───────────────────────────────────────────────────────

        // 1. Serialize dirty DBI trees back into the main catalog.
        try self.flushDbiState();

        // 2. Persist freed pages into the GC tree so future transactions can
        //    reclaim them once no reader holds a snapshot of this txnid.
        try self.serializeFreedPages();

        // 3+4. Stamp checksums and write pages to the mmap.
        //
        //  - dirty_pages_wm (writemap only): existing pages modified directly in
        //    the mmap — stamp checksums in-place; no copy needed.
        //  - dirty_list / spill_list: new pages in heap buffers — stamp checksums
        //    in the heap buffer, then copy to mmap (both modes).
        if (self.env.writemap) {
            var it = self.dirty_pages_wm.keyIterator();
            while (it.next()) |pgno_ptr| {
                const offset = @as(usize, pgno_ptr.*) * PAGE_SIZE;
                const ph = @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.env.map.ptr + offset)));
                const skip = (ph.flags & page_mod.P_LEAF2) != 0 or
                    (ph.flags & page_mod.P_META) != 0;
                if (!skip) ph.dupfix_ksize = page_mod.computePageChecksum(ph, PAGE_SIZE);
            }
        }
        // Stamp and write dirty_list / spill_list pages (heap buffers) in all modes.
        // Pages are written in ascending pgno order for sequential I/O performance.
        {
            const writePage = struct {
                fn call(txn: *Transaction, pgno: u32, buf: []u8) !void {
                    const ph = @as(*page_mod.PageHeader, @ptrCast(@alignCast(buf.ptr)));
                    const skip = (ph.flags & page_mod.P_LEAF2) != 0 or
                        (ph.flags & page_mod.P_META) != 0;
                    if (!skip) ph.dupfix_ksize = page_mod.computePageChecksum(ph, PAGE_SIZE);
                    const offset = @as(usize, pgno) * PAGE_SIZE;
                    if (offset + PAGE_SIZE > txn.env.map.len) {
                        const needed = offset + PAGE_SIZE;
                        const new_size = if (txn.env.map_full_fn) |fn_ptr|
                            try fn_ptr(txn.env, needed)
                        else
                            needed;
                        try txn.env.resize(new_size);
                    }
                    @memcpy(txn.env.map.ptr[offset .. offset + PAGE_SIZE], buf[0..PAGE_SIZE]);
                }
            }.call;

            // Collect all dirty pages (both lists) into a temporary array,
            // sort by pgno, then write sequentially.  Sorting n<=SPILL_THRESHOLD
            // pages is O(n log n) with n≤64 — effectively free.
            const PageEntry = struct { pgno: u32, buf: []u8 };
            var pages = std.ArrayListUnmanaged(PageEntry){};
            defer pages.deinit(self.allocator);
            var it = self.dirty_list.iterator();
            while (it.next()) |e|
                try pages.append(self.allocator, .{ .pgno = e.key_ptr.*, .buf = e.value_ptr.* });
            var sit = self.spill_list.iterator();
            while (sit.next()) |e|
                try pages.append(self.allocator, .{ .pgno = e.key_ptr.*, .buf = e.value_ptr.* });
            std.sort.block(PageEntry, pages.items, {}, struct {
                fn lt(_: void, a: PageEntry, b: PageEntry) bool {
                    return a.pgno < b.pgno;
                }
            }.lt);
            for (pages.items) |p| try writePage(self, p.pgno, p.buf);
        }

        // 5. Write meta into the alternate slot (ping-pong).
        const next_slot: u8 = 1 - self.env.best_meta_idx;
        const m = self.env.getMetaAt(next_slot);
        m.* = self.meta;
        m.txnid_a = self.txnid;
        m.txnid_b = self.txnid;

        // 6. Sync to disk (skipped in safe_nosync / nosync modes, or per-txn nosync override).
        if (!self.env.skip_sync and !self.nosync) try self.env.map.sync();
        self.env.best_meta_idx = next_slot;

        // 7. Auto-shrink: truncate trailing over-allocated space if configured.
        self.env.tryAutoShrink(self.meta.geometry.first_unallocated);

        self.committed = true;
        self.cleanup();
    }

    /// Pauses a read-only transaction, releasing its reader slot without freeing the
    /// Transaction handle. Call renew() to reactivate at the current snapshot.
    /// Has no effect on write transactions or already-committed transactions.
    pub fn reset(self: *Transaction) void {
        if (!self.rdonly) return;
        if (self.committed) return;
        if (self.reader_slot) |slot| {
            self.env.lock.unregisterReader(slot);
            self.reader_slot = null;
        }
        self.committed = true; // marks the handle as dormant (renew() accepts this state)
        // Free the overflow buffer to release memory during the dormant period.
        if (self.overflow_buf.len > 0) {
            self.allocator.free(self.overflow_buf);
            self.overflow_buf = &.{};
        }
        // Free dbi_state so renew() can allocate a fresh one.
        self.allocator.free(self.dbi_state);
        self.dbi_state = &.{};
    }

    /// Renews a finished read-only transaction to see the current database snapshot
    /// without reallocating the Transaction object.
    /// Valid only after abort() or commit() on a read-only transaction.
    pub fn renew(self: *Transaction) !void {
        if (!self.rdonly) return error.BadTxn;
        if (!self.committed) return error.BadTxn; // must be in the "done" state

        const alloc = self.allocator;
        const best_meta = self.env.bestMeta();
        const txnid = best_meta.txnid_a;

        const reader_slot = try self.env.lock.registerReader(txnid);
        errdefer self.env.lock.unregisterReader(reader_slot);

        const dbi_state = try alloc.alloc(DbiState, self.env.max_dbs);
        for (dbi_state) |*s| s.* = .{ .tree = std.mem.zeroes(core_types.Tree), .flags = .{}, .cmp_fn = cmpLexicographic, .name_buf = undefined, .name_len = 0, .open = false, .dirty = false };

        self.txnid = txnid;
        self.meta = best_meta.*;
        self.dbi_state = dbi_state;
        self.next_dbi = self.env.dbi_next;
        self.reader_slot = reader_slot;
        self.committed = false;
        // Reinitialize all collections that cleanup() deinit'd.
        self.dirty_list = .{};
        self.spill_list = .{};
        self.shadow_list = .{};
        self.dirty_pages_wm = .{};
        self.freed_pages = .{};
        self.committed_freed = .{};
        self.reclaimed_pages = .{};
        self.overflow_buf = &.{};
        self.loading_gc = false;
    }

    fn cleanup(self: *Transaction) void {
        if (self.overflow_buf.len > 0) {
            self.allocator.free(self.overflow_buf);
            self.overflow_buf = &.{};
        }
        var it = self.dirty_list.iterator();
        while (it.next()) |entry| self.allocator.free(entry.value_ptr.*);
        self.dirty_list.deinit(self.allocator);
        var sit = self.spill_list.iterator();
        while (sit.next()) |entry| self.allocator.free(entry.value_ptr.*);
        self.spill_list.deinit(self.allocator);
        var shit = self.shadow_list.iterator();
        while (shit.next()) |entry| self.allocator.free(entry.value_ptr.*);
        self.shadow_list.deinit(self.allocator);
        self.dirty_pages_wm.deinit(self.allocator);
        self.freed_pages.deinit(self.allocator);
        self.committed_freed.deinit(self.allocator);
        self.reclaimed_pages.deinit(self.allocator);
        if (self.dbi_state.len > 0) self.allocator.free(self.dbi_state);

        if (self.reader_slot) |slot| {
            self.env.lock.unregisterReader(slot);
            self.reader_slot = null;
        }
        if (self.writer_held) {
            self.env.lock.unlockWriter() catch |e|
                std.log.err("unlockWriter failed in cleanup: {}", .{e});
            self.writer_held = false;
        }
        self.committed = true;
    }

    // ─── Page access ─────────────────────────────────────────────────────────

    fn getPage(self: *Transaction, pgno: u32) *page_mod.PageHeader {
        if (self.dirty_list.get(pgno)) |buf|
            return @as(*page_mod.PageHeader, @ptrCast(@alignCast(buf.ptr)));
        if (self.spill_list.get(pgno)) |buf|
            return @as(*page_mod.PageHeader, @ptrCast(@alignCast(buf.ptr)));
        // Walk the parent chain for nested transactions (MVCC).
        if (self.parent) |par| return par.getPage(pgno);
        return self.env.getPagePtr(pgno);
    }

    fn getWritablePage(self: *Transaction, pgno: u32) !struct { pgno: u32, page: *page_mod.PageHeader } {
        // 1. Already in hot dirty set — same pgno, return existing buffer.
        if (self.dirty_list.get(pgno)) |buf|
            return .{ .pgno = pgno, .page = @ptrCast(@alignCast(buf.ptr)) };
        // 2. In spill list — un-spill back to hot set, same pgno.
        if (self.spill_list.fetchRemove(pgno)) |kv| {
            try self.dirty_list.put(self.allocator, pgno, kv.value);
            return .{ .pgno = pgno, .page = @ptrCast(@alignCast(kv.value.ptr)) };
        }
        // 3. Committed page: path-copy to a new pgno.
        //    The original mmap page at `pgno` remains untouched so concurrent
        //    readers holding a snapshot that references `pgno` continue to see
        //    consistent data — true MVCC snapshot isolation.
        const src_page = self.getPage(pgno);
        if (self.parent == null and !page_mod.validatePageChecksum(src_page, PAGE_SIZE)) {
            return error.PageCorrupted;
        }
        const buf = try self.allocator.alloc(u8, PAGE_SIZE);
        errdefer self.allocator.free(buf);
        @memcpy(buf[0..PAGE_SIZE], @as([*]const u8, @ptrCast(src_page))[0..PAGE_SIZE]);
        // Allocate a new pgno WITHOUT creating a dirty_list entry (we place our
        // copied buffer there directly, avoiding an alloc+free of a zeroed buffer).
        const new_pgno = try self.allocPageNoBuf();
        try self.dirty_list.put(self.allocator, new_pgno, buf);
        if (self.parent == null) try self.trySpill();
        // The old committed pgno is no longer referenced by this transaction's
        // tree. Schedule it for GC after commit (do NOT reuse this txn — readers
        // can still navigate to it via the committed mmap).
        try self.committed_freed.append(self.allocator, pgno);
        const page = @as(*page_mod.PageHeader, @ptrCast(@alignCast(buf.ptr)));
        page.pgno = new_pgno;
        return .{ .pgno = new_pgno, .page = page };
    }

    /// Updates a branch node's child pgno in-place.
    /// The branch node at `ci` stores its child pgno in `node.data_shim`.
    fn updateBranchChild(page: *page_mod.PageHeader, ci: u16, new_pgno: u32) void {
        page.getNode(ci).data_shim = new_pgno;
    }

    /// Makes every branch page on `path` writable (path-copying CoW, top-down).
    /// When a branch page gets a new pgno, its parent's child pointer is updated.
    /// After this call, all path[i].pgno values are writable pages in dirty_list.
    fn touchPathDown(self: *Transaction, tree: *align(4) core_types.Tree, path: []PathEntry) !void {
        for (path, 0..) |*entry, i| {
            const old_pgno = entry.pgno;
            const wp = try self.getWritablePage(old_pgno);
            if (wp.pgno == old_pgno) continue; // already dirty — no update needed
            entry.pgno = wp.pgno;
            if (i == 0) {
                tree.root = wp.pgno;
            } else {
                // Parent (path[i-1]) is already dirty (processed in previous iteration).
                const par = &path[i - 1];
                const par_buf = self.dirty_list.get(par.pgno).?;
                updateBranchChild(@ptrCast(@alignCast(par_buf.ptr)), par.ci, wp.pgno);
            }
        }
    }

    fn allocPage(self: *Transaction) !u32 {
        // 1. Intra-txn freed pages are always safe to reuse (not yet committed).
        //    These pages are still in dirty_list from when they were first allocated.
        if (self.freed_pages.items.len > 0)
            return self.freed_pages.pop().?;
        // 2. Pre-loaded pages reclaimed from the GC tree (oldest_reader-verified).
        //    These pages are NOT in dirty_list yet — create a fresh zeroed entry so that
        //    getWritablePage() finds them there and does NOT path-copy them.
        if (self.reclaimed_pages.items.len > 0) {
            const pgno = self.reclaimed_pages.pop().?;
            const buf = try self.allocator.alloc(u8, PAGE_SIZE);
            @memset(buf, 0);
            try self.dirty_list.put(self.allocator, pgno, buf);
            if (self.parent == null) try self.trySpill();
            return pgno;
        }
        // 3. Root write transactions: try to reclaim a batch from the GC tree.
        //    Guard prevents re-entrant calls (btreeDel inside loadGCBatch also calls allocPage).
        if (self.parent == null and !self.rdonly and !self.loading_gc) {
            self.loading_gc = true;
            defer self.loading_gc = false;
            try self.loadGCBatch();
            if (self.reclaimed_pages.items.len > 0)
                return self.reclaimed_pages.pop().?;
        }
        // 4. Fresh allocation.
        const pgno = self.meta.geometry.first_unallocated;
        if (pgno == std.math.maxInt(u32)) return error.MapFull;
        self.meta.geometry.first_unallocated += 1;
        self.meta.geometry.current = self.meta.geometry.first_unallocated;
        // New pages always go to dirty_list (heap), even in writemap mode.
        // This avoids mid-transaction mmap resizes that would invalidate
        // pointers already returned by getWritablePage.
        const buf = try self.allocator.alloc(u8, PAGE_SIZE);
        @memset(buf, 0);
        try self.dirty_list.put(self.allocator, pgno, buf);
        // Spill excess dirty pages to the cold list to limit peak memory use.
        if (self.parent == null) try self.trySpill();
        return pgno;
    }

    /// Like allocPage() but does NOT create a dirty_list entry.
    /// Uses anyerror to break the inferred-error-set cycle:
    ///   getWritablePage → allocPageNoBuf → loadGCBatch → btreeDel → touchPathDown → getWritablePage.
    fn allocPageNoBuf(self: *Transaction) anyerror!u32 {
        if (self.freed_pages.items.len > 0) return self.freed_pages.pop().?;
        if (self.reclaimed_pages.items.len > 0) return self.reclaimed_pages.pop().?;
        if (self.parent == null and !self.rdonly and !self.loading_gc) {
            self.loading_gc = true;
            defer self.loading_gc = false;
            try self.loadGCBatch();
            if (self.reclaimed_pages.items.len > 0) return self.reclaimed_pages.pop().?;
        }
        const new_pgno = self.meta.geometry.first_unallocated;
        if (new_pgno == std.math.maxInt(u32)) return error.MapFull;
        self.meta.geometry.first_unallocated += 1;
        self.meta.geometry.current = self.meta.geometry.first_unallocated;
        return new_pgno;
    }

    /// Routes freed pgno to the correct list.
    /// Committed pages (< first_unallocated_snapshot) go to committed_freed — NOT safe to reuse this txn.
    /// Intra-txn pages (>= first_unallocated_snapshot) go to freed_pages — safe to reuse this txn.
    inline fn freePage(self: *Transaction, pgno: u32) !void {
        if (pgno < self.first_unallocated_snapshot) {
            try self.committed_freed.append(self.allocator, pgno);
        } else {
            try self.freed_pages.append(self.allocator, pgno);
        }
    }

    /// Moves half of the dirty pages to the cold spill list to reduce peak heap use.
    fn trySpill(self: *Transaction) !void {
        if (self.dirty_list.count() <= SPILL_THRESHOLD) return;
        const target = self.dirty_list.count() / 2;
        var to_move = std.ArrayListUnmanaged(u32){};
        defer to_move.deinit(self.allocator);
        var it = self.dirty_list.iterator();
        while (it.next()) |entry| {
            if (to_move.items.len >= target) break;
            try to_move.append(self.allocator, entry.key_ptr.*);
        }
        for (to_move.items) |pgno| {
            const buf = self.dirty_list.get(pgno).?;
            _ = self.dirty_list.remove(pgno);
            try self.spill_list.put(self.allocator, pgno, buf);
        }
    }

    /// Scans the GC tree for a committed txnid whose pages are safe to reclaim
    /// (txnid < oldest active reader), loads those pgnos into `reclaimed_pages`,
    /// and deletes the entry from the GC tree.
    ///
    /// FIFO mode (default): walks to the leftmost leaf (smallest txnid first) —
    /// maximises the chance of finding a reclaimable entry quickly.
    ///
    /// LIFO mode: walks to the rightmost leaf (largest
    /// txnid first) — those pages are most recently freed and therefore most
    /// likely to be hot in the OS page cache, reducing I/O on re-use.
    fn loadGCBatch(self: *Transaction) !void {
        if (self.meta.trees.gc.items == 0) return;
        const oldest = self.env.lock.getOldestReader(self.txnid);
        const lifo = self.env.liforeclaim;

        // Walk to the target leaf (leftmost for FIFO, rightmost for LIFO).
        var pgno = self.meta.trees.gc.root;
        while (true) {
            const page = self.getPage(pgno);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                const n = page.getNumEntries();
                if (lifo and n > 0) {
                    // Rightmost child: last entry holds the rightmost pgno.
                    pgno = page.getNode(n - 1).getChildPgno();
                } else {
                    // Leftmost child: entry 0 is the leftmost (key = "").
                    pgno = page.getNode(0).getChildPgno();
                }
            } else {
                const n = page.getNumEntries();
                if (n == 0) return;
                // For LIFO pick the last entry on the leaf; FIFO picks the first.
                const node_idx: u16 = if (lifo) @intCast(n - 1) else 0;
                const node = page.getNode(node_idx);
                const key = node.getKey();
                if (key.len < 8) return;
                const gc_txnid = std.mem.readInt(u64, key[0..8], .little);
                if (gc_txnid >= oldest) {
                    // A lagging reader is preventing reclaim.  Fire the HSR
                    // callback (if set) so the application can log or act.
                    if (self.env.hsr_fn) |hsr| {
                        hsr(self.env, oldest, gc_txnid - oldest, self.env.hsr_ctx);
                    }
                    return;
                }
                const val = node.getData();
                const n_pgnos = val.len / 4;
                var i: usize = 0;
                while (i < n_pgnos) : (i += 1) {
                    const p = std.mem.readInt(u32, val[i * 4 ..][0..4], .little);
                    try self.reclaimed_pages.append(self.allocator, p);
                }
                // Copy key before btreeDel invalidates the node pointer.
                var key_copy: [8]u8 = undefined;
                @memcpy(&key_copy, key[0..8]);
                try self.btreeDel(&self.meta.trees.gc, &key_copy, cmpIntegerKey);
                return;
            }
        }
    }

    /// Persists the current freed_pages list into the GC tree under this txnid.
    /// Called during root-transaction commit, before the dirty-page flush.
    ///
    /// Uses a while loop so that btreeOps during serialisation (which may
    /// themselves free/allocate a handful of pages) are captured and
    /// serialised in subsequent iterations — no pages are ever leaked.
    ///
    /// Coalesce (default ON): before inserting a new GC entry, drain
    /// all existing entries with txnid < oldest_reader into the current
    /// batch.  This keeps the GC tree compact: instead of one entry per
    /// committed write transaction, there is at most one entry total.
    fn serializeFreedPages(self: *Transaction) !void {
        if (self.parent != null) return; // child txns merge into parent

        while (self.freed_pages.items.len > 0 or self.committed_freed.items.len > 0) {
            // Snapshot the current batch and clear freed_pages so that
            // btreePut/btreeDel inside this loop can accumulate new pages
            // without aliasing the list we're serialising.
            var snapshot = self.freed_pages;
            self.freed_pages = .{};
            defer snapshot.deinit(self.allocator);

            // Also include pages freed from committed state — they go to GC too.
            try snapshot.appendSlice(self.allocator, self.committed_freed.items);
            self.committed_freed.clearRetainingCapacity();

            // Coalesce: absorb reclaimable old GC entries into
            // this batch so the GC tree stays O(1) in size.
            if (self.env.coalesce) {
                try self.absorbOldGCEntries(&snapshot);
            }

            var key_buf: [8]u8 = undefined;
            std.mem.writeInt(u64, &key_buf, self.txnid, .little);
            try self.btreePut(&self.meta.trees.gc, &key_buf, std.mem.sliceAsBytes(snapshot.items), false, cmpIntegerKey);
            // Any pages freed by the btreePut above land in self.freed_pages
            // and will be picked up by the next loop iteration.
        }
    }

    /// Absorbs all GC entries with txnid < oldest_reader into `batch`.
    /// Those entries are then deleted from the GC tree.  Pages freed by
    /// the btreeDel operations land in self.freed_pages and are handled
    /// by serializeFreedPages's outer while loop.
    fn absorbOldGCEntries(self: *Transaction, batch: *FreedPages) !void {
        if (self.meta.trees.gc.items == 0) return;
        const oldest = self.env.lock.getOldestReader(self.txnid);
        // Prevent loadGCBatch re-entry: path-copy inside btreeDel may call
        // allocPageNoBuf → loadGCBatch → btreeDel, double-decrementing items.
        const prev_loading = self.loading_gc;
        self.loading_gc = true;
        defer self.loading_gc = prev_loading;

        while (self.meta.trees.gc.items > 0) {
            // Walk to the leftmost leaf of the GC tree.
            var pgno = self.meta.trees.gc.root;
            var found_txnid: u64 = 0;

            while (true) {
                const page = self.getPage(pgno);
                if ((page.flags & page_mod.P_BRANCH) != 0) {
                    pgno = page.getNode(0).getChildPgno();
                } else {
                    if (page.getNumEntries() == 0) break;
                    const node = page.getNode(0);
                    const key = node.getKey();
                    if (key.len < 8) break;
                    const gc_txnid = std.mem.readInt(u64, key[0..8], .little);
                    if (gc_txnid >= oldest) break; // still visible — stop
                    // Copy pgnos out BEFORE btreeDel invalidates the node.
                    const val = node.getData();
                    const n_pgnos = val.len / 4;
                    var i: usize = 0;
                    while (i < n_pgnos) : (i += 1) {
                        const p = std.mem.readInt(u32, val[i * 4 ..][0..4], .little);
                        try batch.append(self.allocator, p);
                    }
                    // Copy key before the delete invalidates the pointer.
                    var key_copy: [8]u8 = undefined;
                    @memcpy(&key_copy, key[0..8]);
                    found_txnid = gc_txnid;
                    try self.btreeDel(&self.meta.trees.gc, &key_copy, cmpIntegerKey);
                    break;
                }
            }

            // If the inner walk found no absorb-able entry, stop.
            if (found_txnid == 0) break;
        }
    }

    // ─── Overflow page helpers ───────────────────────────────────────────────

    /// Allocates `n_pages` contiguous overflow pages, initialises their headers,
    /// and writes `val` into the page bodies in a single pass.
    /// Returns the pgno of the first page.  Overflow pages are always freshly
    /// allocated (not recycled) to guarantee physical contiguity.
    fn allocOverflow(self: *Transaction, n_pages: u32, val: []const u8) !u32 {
        const first_pgno = self.meta.geometry.first_unallocated;
        if (@as(u64, first_pgno) + n_pages > std.math.maxInt(u32)) return error.MapFull;
        self.meta.geometry.first_unallocated += n_pages;
        self.meta.geometry.current = self.meta.geometry.first_unallocated;
        var val_off: usize = 0;
        var i: u32 = 0;
        while (i < n_pages) : (i += 1) {
            const pgno = first_pgno + i;
            const buf = try self.allocator.alloc(u8, PAGE_SIZE);
            @memset(buf, 0);
            const ph = @as(*page_mod.PageHeader, @ptrCast(@alignCast(buf.ptr)));
            ph.flags = page_mod.P_OVERFLOW;
            ph.pgno = pgno;
            ph.txnid = self.txnid;
            ph.data_union = if (i == 0) n_pages else 0;
            // Write the data slice directly into the page body.
            const remaining = val.len - val_off;
            const chunk = @min(remaining, OVERFLOW_DATA_PER_PAGE);
            @memcpy(buf[20..][0..chunk], val[val_off..][0..chunk]);
            val_off += chunk;
            try self.dirty_list.put(self.allocator, pgno, buf);
        }
        return first_pgno;
    }

    /// Reads `val_size` bytes from the overflow run into `dest`.
    fn readOverflowInto(self: *Transaction, first_pgno: u32, val_size: usize, dest: []u8) void {
        var remaining = val_size;
        var dest_off: usize = 0;
        var pgno = first_pgno;
        while (remaining > 0) {
            const page_bytes = @as([*]const u8, @ptrCast(self.getPage(pgno)));
            const chunk = @min(remaining, OVERFLOW_DATA_PER_PAGE);
            @memcpy(dest[dest_off..][0..chunk], page_bytes[20..][0..chunk]);
            dest_off += chunk;
            remaining -= chunk;
            pgno += 1;
        }
    }

    /// Reads a large value into the transaction's reusable overflow_buf and
    /// returns a slice into it.  The slice is valid until the next overflow read.
    fn readOverflow(self: *Transaction, node: *page_mod.Node) ![]const u8 {
        const val_size = @as(usize, node.data_shim);
        const first_pgno = node.getOverflowPgno();
        if (val_size > self.overflow_buf.len) {
            if (self.overflow_buf.len > 0) self.allocator.free(self.overflow_buf);
            self.overflow_buf = try self.allocator.alloc(u8, val_size);
        }
        self.readOverflowInto(first_pgno, val_size, self.overflow_buf[0..val_size]);
        return self.overflow_buf[0..val_size];
    }

    /// Adds all overflow pages referenced by an overflow node to the appropriate freed list.
    fn freeOverflow(self: *Transaction, node: *page_mod.Node) !void {
        if (!node.isOverflow()) return;
        const val_size = @as(usize, node.data_shim);
        const n_pages: u32 = @intCast((val_size + OVERFLOW_DATA_PER_PAGE - 1) / OVERFLOW_DATA_PER_PAGE);
        const first_pgno = node.getOverflowPgno();
        var i: u32 = 0;
        while (i < n_pages) : (i += 1) {
            try self.freePage(first_pgno + i);
        }
    }

    // ─── DBI management ──────────────────────────────────────────────────────

    pub fn openDbi(
        self: *Transaction,
        name: [:0]const u8,
        flags: types.DbFlags,
    ) !types.Dbi {
        const name_slice = name[0..name.len];
        if (name_slice.len > 255) return error.NameTooLong;

        // Step 1: determine slot (from env registry, or allocate a new one).
        var needs_registration = false;
        const slot: u32 = if (self.env.dbi_registry.get(name_slice)) |cached_slot| blk: {
            const s = &self.dbi_state[cached_slot];
            if (s.open) {
                // Already loaded in this transaction.
                if (flags.dupsort) s.flags.dupsort = true;
                return cached_slot;
            }
            // Registry has the slot; this transaction hasn't loaded it yet.
            break :blk cached_slot;
        } else blk: {
            // Brand-new DBI: assign from free list or bump dbi_next.
            needs_registration = true;
            if (self.env.dbi_free_slots.items.len > 0)
                break :blk self.env.dbi_free_slots.pop().?;
            if (self.env.dbi_next >= self.env.max_dbs) return error.DbsFull;
            const n = self.env.dbi_next;
            self.env.dbi_next += 1;
            break :blk n;
        };
        // Update txn high-water mark.
        if (slot >= self.next_dbi) self.next_dbi = slot + 1;

        var state = &self.dbi_state[slot];
        state.* = .{ .tree = std.mem.zeroes(core_types.Tree), .flags = .{}, .cmp_fn = cmpLexicographic, .name_buf = undefined, .name_len = 0, .open = false, .dirty = false };

        // Look up the DBI in the main catalog tree.
        const catalog_entry = try self.btreeGet(self.meta.trees.main, name_slice, cmpLexicographic);
        if (catalog_entry) |bytes| {
            if (bytes.len < @sizeOf(core_types.Tree)) return error.Corrupted;
            var tree: core_types.Tree = undefined;
            @memcpy(std.mem.asBytes(&tree), bytes[0..@sizeOf(core_types.Tree)]);
            // Restore persisted flags and validate against requested flags.
            const flags_valid = (tree.flags & DB_FLAGS_VALID) != 0;
            if (flags_valid and !flags.accede) {
                const stored = u16ToDbFlags(@as(u16, @truncate(tree.flags)));
                const stored_bits = dbFlagsToU16(stored) & ~DB_FLAGS_VALID;
                const request_bits = dbFlagsToU16(flags) & ~DB_FLAGS_VALID;
                if (stored_bits != request_bits) return error.Incompatible;
                state.flags = stored;
            } else if (flags_valid) {
                // accede=true: skip validation, load whatever flags are stored.
                state.flags = u16ToDbFlags(@as(u16, @truncate(tree.flags)));
            } else {
                // Legacy DBI (no persisted flags) — accept caller's flags.
                state.flags = flags;
            }
            state.flags.create = false;
            state.tree = tree;
            state.cmp_fn = if (state.flags.reversekey) cmpReverse else if (state.flags.integerkey) cmpIntegerKey else cmpLexicographic;
            state.dirty = false;
        } else if (flags.create) {
            if (self.rdonly) return error.BadTxn;
            const root_pgno = try self.allocPage();
            const root_page = (try self.getWritablePage(root_pgno)).page;
            root_page.init(PAGE_SIZE, page_mod.P_LEAF);
            root_page.pgno = root_pgno;
            root_page.txnid = self.txnid;
            state.tree = core_types.Tree{
                .flags = 0,
                .height = 1,
                .dupfix_size = 0,
                .root = root_pgno,
                .branch_pages = 0,
                .leaf_pages = 1,
                .large_pages = 0,
                .sequence = 0,
                .items = 0,
                .mod_txnid = self.txnid,
            };
            state.flags = flags;
            state.cmp_fn = if (flags.reversekey) cmpReverse else if (flags.integerkey) cmpIntegerKey else cmpLexicographic;
            state.dirty = true;
        } else {
            // DBI doesn't exist and .create is false — return allocated slot.
            if (needs_registration) {
                self.env.dbi_free_slots.append(self.env.allocator, slot) catch {};
                self.next_dbi = slot; // undo high-water bump
                self.env.dbi_next = slot; // undo env bump
            }
            return error.NotFound;
        }

        // Step 3: register in env if this was a new allocation.
        if (needs_registration) {
            const name_copy = self.env.allocator.dupe(u8, name_slice) catch |err| {
                // Registration failed — reclaim slot.
                self.env.dbi_free_slots.append(self.env.allocator, slot) catch {};
                return err;
            };
            self.env.dbi_registry.put(self.env.allocator, name_copy, slot) catch |err| {
                self.env.allocator.free(name_copy);
                self.env.dbi_free_slots.append(self.env.allocator, slot) catch {};
                return err;
            };
        }

        const copy_len = @min(name_slice.len, 255);
        @memcpy(state.name_buf[0..copy_len], name_slice[0..copy_len]);
        state.name_len = @as(u8, @truncate(copy_len));
        state.open = true;
        return slot;
    }

    /// Writes modified DBI trees back into the main catalog before commit.
    fn flushDbiState(self: *Transaction) !void {
        var i: u32 = 1;
        while (i < self.next_dbi) : (i += 1) {
            const state = &self.dbi_state[i];
            if (!state.open or !state.dirty) continue;
            state.tree.mod_txnid = self.txnid;
            // Persist structural flags so the next open can validate them.
            state.tree.flags = dbFlagsToU16(state.flags);
            const name: []const u8 = state.name_buf[0..state.name_len];
            const tree_bytes: []const u8 = std.mem.asBytes(&state.tree);
            try self.btreePut(&self.meta.trees.main, name, tree_bytes, false, cmpLexicographic);
        }
    }

    // ─── Public data API ─────────────────────────────────────────────────────

    inline fn checkDbi(self: *const Transaction, dbi: types.Dbi) !void {
        if (dbi == 0 or dbi >= self.next_dbi or !self.dbi_state[dbi].open)
            return error.BadDbi;
    }

    pub fn get(self: *Transaction, dbi: types.Dbi, key: []const u8) !?[]const u8 {
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];
        if (state.flags.dupsort) return self.btreeGetPrefix(state.tree, key, cmpLexicographic);
        return try self.btreeGet(state.tree, key, state.cmp_fn);
    }

    pub fn put(
        self: *Transaction,
        dbi: types.Dbi,
        key: []const u8,
        val: []const u8,
        flags: types.PutFlags,
    ) !void {
        if (self.rdonly) return error.BadTxn;
        if (key.len > limits.max_key_size) return error.BadValSize;
        // Values larger than max_val_size are stored in overflow pages automatically;
        // no upper bound is enforced here.
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];

        if (state.flags.dupsort) {
            // ALLDUPS: replace all existing dups for this key with a single new value.
            if (flags.alldups) {
                try self.btreeDelPrefix(&state.tree, key, cmpLexicographic);
                const composite = try self.makeCompositeKey(key, val, state.flags.reversedup);
                defer self.allocator.free(composite);
                try self.btreePut(&state.tree, composite, "", false, cmpLexicographic);
                state.dirty = true;
                return;
            }
            // APPENDDUP: append dup value guaranteed >= all existing dups (O(1) bulk load).
            if (flags.appenddup) {
                const composite = try self.makeCompositeKey(key, val, state.flags.reversedup);
                defer self.allocator.free(composite);
                try self.btreePut(&state.tree, composite, "", true, cmpLexicographic);
                state.dirty = true;
                return;
            }
            const composite = try self.makeCompositeKey(key, val, state.flags.reversedup);
            defer self.allocator.free(composite);
            // NODUPDATA: reject if this exact (key, val) pair already exists.
            if (flags.nodupdata) {
                if (try self.btreeGet(state.tree, composite, cmpLexicographic) != null)
                    return error.KeyExist;
            }
            try self.btreePut(&state.tree, composite, "", flags.append, cmpLexicographic);
        } else {
            if (flags.nooverwrite) {
                if (try self.btreeGet(state.tree, key, state.cmp_fn) != null) return error.KeyExist;
            }
            try self.btreePut(&state.tree, key, val, flags.append, state.cmp_fn);
        }
        state.dirty = true;
    }

    /// Reserves `val_size` bytes for `key` and returns a mutable slice
    /// pointing into the dirty buffer. The caller writes directly into
    /// the slice — zero-copy, no extra allocation.
    pub fn reserve(
        self: *Transaction,
        dbi: types.Dbi,
        key: []const u8,
        val_size: usize,
    ) ![]u8 {
        if (self.rdonly) return error.BadTxn;
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];
        const slice = try self.btreeReserve(&state.tree, key, val_size, state.cmp_fn);
        state.dirty = true;
        return slice;
    }

    pub fn del(
        self: *Transaction,
        dbi: types.Dbi,
        key: []const u8,
        val: ?[]const u8,
    ) !void {
        if (self.rdonly) return error.BadTxn;
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];

        if (state.flags.dupsort) {
            if (val) |v| {
                const composite = try self.makeCompositeKey(key, v, state.flags.reversedup);
                defer self.allocator.free(composite);
                try self.btreeDel(&state.tree, composite, cmpLexicographic);
            } else {
                try self.btreeDelPrefix(&state.tree, key, cmpLexicographic);
            }
        } else {
            try self.btreeDel(&state.tree, key, state.cmp_fn);
        }
        state.dirty = true;
    }

    pub fn cursor(self: *Transaction, dbi: types.Dbi) !Cursor {
        try self.checkDbi(dbi);
        return Cursor.open(self, dbi);
    }

    /// Clears (delete=false) or permanently removes (delete=true) a named DBI.
    pub fn dropDbi(self: *Transaction, name: [:0]const u8, delete: bool) !void {
        if (self.rdonly) return error.BadTxn;
        const dbi = try self.openDbi(name, .{});
        const state = &self.dbi_state[dbi];

        // Truncate the root page to an empty leaf.
        const root_wp = try self.getWritablePage(state.tree.root);
        // If root got a new pgno (path-copy), update the tree struct.
        state.tree.root = root_wp.pgno;
        const root_page = root_wp.page;
        root_page.init(PAGE_SIZE, page_mod.P_LEAF);
        root_page.pgno = root_wp.pgno;
        root_page.txnid = self.txnid;

        state.tree.items = 0;
        state.tree.height = 1;
        state.tree.mod_txnid = self.txnid;

        if (delete) {
            // Remove the entry from the main catalog.
            try self.btreeDel(&self.meta.trees.main, name[0..name.len], cmpLexicographic);
            state.open = false;
            state.dirty = false; // do not re-insert on flush
        } else {
            state.dirty = true; // serialize the empty tree on commit
        }
    }

    /// Returns the names of all named databases currently in the environment.
    /// The returned slice and each name string are heap-allocated; caller must free both.
    pub fn listTables(self: *Transaction, alloc: std.mem.Allocator) ![][]u8 {
        var pairs = std.ArrayListUnmanaged([2][]u8){};
        defer {
            for (pairs.items) |p| {
                if (p[0].len > 0) alloc.free(p[0]);
                alloc.free(p[1]);
            }
            pairs.deinit(alloc);
        }

        if (self.meta.trees.main.items > 0) {
            try self.collectTreePairs(self.meta.trees.main.root, &pairs, alloc);
        }

        const names = try alloc.alloc([]u8, pairs.items.len);
        errdefer alloc.free(names);
        for (pairs.items, 0..) |*pair, i| {
            names[i] = pair[0];
            pair[0] = &.{}; // transfer ownership; prevent defer double-free
        }
        return names;
    }

    /// Renames a named database from `old_name` to `new_name`.
    /// Fails with error.NotFound if `old_name` does not exist,
    /// error.KeyExist if `new_name` already exists,
    /// error.NameTooLong if `new_name` > 255 bytes.
    pub fn renameDbi(self: *Transaction, old_name: [:0]const u8, new_name: [:0]const u8) !void {
        if (self.rdonly) return error.BadTxn;
        const old_slice = old_name[0..old_name.len];
        const new_slice = new_name[0..new_name.len];
        if (new_slice.len > 255) return error.NameTooLong;

        // Confirm new name doesn't already exist.
        if (try self.btreeGet(self.meta.trees.main, new_slice, cmpLexicographic) != null)
            return error.KeyExist;

        // Read existing Tree bytes for old_name.
        const tree_bytes = try self.btreeGet(self.meta.trees.main, old_slice, cmpLexicographic) orelse return error.NotFound;
        if (tree_bytes.len < @sizeOf(core_types.Tree)) return error.Corrupted;

        // Copy tree bytes before deletion invalidates the mmap/dirty pointer.
        var tree_copy: [@sizeOf(core_types.Tree)]u8 = undefined;
        @memcpy(&tree_copy, tree_bytes[0..@sizeOf(core_types.Tree)]);

        // Atomically rename in the catalog: delete old key, insert new key.
        try self.btreeDel(&self.meta.trees.main, old_slice, cmpLexicographic);
        try self.btreePut(&self.meta.trees.main, new_slice, &tree_copy, false, cmpLexicographic);

        // Update the env-level DBI registry if the DBI has been opened.
        if (self.env.dbi_registry.fetchRemove(old_slice)) |kv| {
            const old_key = kv.key;
            const slot = kv.value;
            self.env.allocator.free(old_key);
            const new_key = try self.env.allocator.dupe(u8, new_slice);
            errdefer self.env.allocator.free(new_key);
            try self.env.dbi_registry.put(self.env.allocator, new_key, slot);

            // Update name_buf in the per-txn DBI state for this slot if it's open here.
            if (slot < self.next_dbi and self.dbi_state[slot].open) {
                const copy_len = @min(new_slice.len, 255);
                @memcpy(self.dbi_state[slot].name_buf[0..copy_len], new_slice[0..copy_len]);
                self.dbi_state[slot].name_len = @as(u8, @truncate(copy_len));
                // Suppress redundant flush: catalog was already updated by btreePut above.
                self.dbi_state[slot].dirty = false;
            }
        }
    }

    // ─── Sequences & statistics ──────────────────────────────────────────────

    /// Atomically advances the per-DBI sequence counter by `increment`.
    /// Returns the value *before* the increment (fetch-and-add semantics).
    /// Pass increment=0 to read the current sequence without modifying it.
    pub fn sequence(self: *Transaction, dbi: types.Dbi, increment: u64) !u64 {
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];
        if (increment > 0 and self.rdonly) return error.BadTxn;
        const old = state.tree.sequence;
        if (increment > 0) {
            state.tree.sequence += increment;
            state.dirty = true;
        }
        return old;
    }

    /// Returns B-tree statistics for the given DBI.
    pub fn dbiStat(self: *Transaction, dbi: types.Dbi) !types.Stat {
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];
        return types.Stat{
            .page_size = @as(u32, PAGE_SIZE),
            .depth = state.tree.height,
            .branch_pages = state.tree.branch_pages,
            .leaf_pages = state.tree.leaf_pages,
            .large_pages = state.tree.large_pages,
            .items = state.tree.items,
        };
    }

    /// Returns overall environment statistics derived from the current meta.
    pub fn envStat(self: *Transaction) types.EnvStat {
        return types.EnvStat{
            .page_size = @as(u32, PAGE_SIZE),
            .total_pages = self.meta.geometry.first_unallocated,
            .last_txnid = self.meta.txnid_a,
            .geo_upper_pages = self.meta.geometry.upper,
        };
    }

    /// Returns live space-accounting for the current transaction.
    /// Useful for monitoring how much map space a long-running write
    /// transaction is consuming and whether a slow reader exists.
    pub fn txnInfo(self: *const Transaction) types.TxnInfo {
        const oldest = self.env.lock.getOldestReader(self.txnid);
        return .{
            .space_used = @as(u64, self.meta.geometry.first_unallocated) * PAGE_SIZE,
            .space_limit = self.env.size_upper,
            .space_dirty = @as(u64, @intCast(self.dirty_list.count() + self.spill_list.count())) * PAGE_SIZE,
            .space_retired = @as(u64, @intCast(self.freed_pages.items.len)) * PAGE_SIZE,
            .reader_txnid = oldest,
        };
    }

    /// Returns the flags the named DBI was opened with in this transaction.
    pub fn dbiFlags(self: *Transaction, dbi: types.Dbi) !types.DbFlags {
        try self.checkDbi(dbi);
        return self.dbi_state[dbi].flags;
    }

    /// Like get(), but also returns the number of duplicate values for the key.
    /// For non-DupSort DBIs the count is always 1.
    /// Returns null if the key is not found.
    pub fn getEx(self: *Transaction, dbi: types.Dbi, key: []const u8) !?types.GetExResult {
        try self.checkDbi(dbi);
        const state = &self.dbi_state[dbi];
        if (state.flags.dupsort) {
            var cur = Cursor.open(self, dbi);
            if (!(try cur.find(key))) return null;
            const count = try cur.countDups();
            const kv = cur.currentKV() orelse return null;
            return .{ .val = kv.val, .count = count };
        }
        const val = (try self.btreeGet(state.tree, key, state.cmp_fn)) orelse return null;
        return .{ .val = val, .count = 1 };
    }

    // ─── Internal B-tree operations ──────────────────────────────────────────

    /// Allocates a composite key "key || val" for DupSort storage.
    /// When reversedup is true, the value bytes are bitwise-complemented so that
    /// lexicographic order on stored bytes equals reverse order on original values.
    fn makeCompositeKey(self: *Transaction, key: []const u8, val: []const u8, reversedup: bool) ![]u8 {
        const buf = try self.allocator.alloc(u8, key.len + val.len);
        @memcpy(buf[0..key.len], key);
        if (reversedup) {
            for (val, 0..) |b, i| buf[key.len + i] = ~b;
        } else {
            @memcpy(buf[key.len..], val);
        }
        return buf;
    }

    /// Exact key lookup. Returns a slice into the mmap/dirty buffer, or into
    /// the transaction's overflow_buf for large values.
    fn btreeGet(self: *Transaction, tree: core_types.Tree, key: []const u8, cmp_fn: CmpFn) !?[]const u8 {
        if (tree.items == 0) return null;
        var pgno = tree.root;
        while (true) {
            const page = self.getPage(pgno);
            const result = page.search(key, cmp_fn);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                if (!result.match and result.index == 0) return null;
                const ci: u16 = if (result.match) result.index else result.index - 1;
                if (ci >= page.getNumEntries()) return null;
                pgno = page.getNode(ci).getChildPgno();
            } else {
                if (!result.match) return null;
                const node = page.getNode(result.index);
                if (node.isOverflow()) return try self.readOverflow(node);
                return node.getData();
            }
        }
    }

    /// Prefix lookup for DupSort DBIs. Returns the value portion (bytes after the key prefix).
    fn btreeGetPrefix(self: *Transaction, tree: core_types.Tree, prefix: []const u8, cmp_fn: CmpFn) ?[]const u8 {
        if (tree.items == 0) return null;
        var pgno = tree.root;
        while (true) {
            const page = self.getPage(pgno);
            const result = page.search(prefix, cmp_fn);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                pgno = page.getNode(branchCi(result)).getChildPgno();
            } else {
                const idx = result.index;
                if (idx >= page.getNumEntries()) return null;
                const k = page.getNode(idx).getKey();
                if (!std.mem.startsWith(u8, k, prefix)) return null;
                return k[prefix.len..];
            }
        }
    }

    // -------------------------------------------------------------------------
    // Congratulations. You have reached the deepest circle of this codebase.
    // This is btreePut, the function that does the actual work while everyone
    // else takes credit. It splits pages, rewrites branch separators, handles
    // overflow, and still never complains. Unlike some people I know.
    //
    // If you are reading this during a production incident: it's not this
    // function's fault. It never is. Check your transaction boundaries.
    //
    // If you are reading this out of curiosity: you now owe me $100.
    // Venmo: @MarceloTesla. Memo: "I read the source, here's your money."
    //
    // — Marcelo Tesla, somewhere between a coffee and a kernel panic hehe :3
    // -------------------------------------------------------------------------

    /// Inserts or overwrites a key in the B-tree with COW on the target leaf.
    /// Tracks the branch traversal path for use in page splits.
    /// `append`: when true, skips binary search and inserts at the rightmost position (O(1) bulk load).
    fn btreePut(self: *Transaction, tree: *align(4) core_types.Tree, key: []const u8, val: []const u8, append: bool, cmp_fn: CmpFn) !void {
        var path: [consts.MAX_DEPTH]PathEntry = undefined;
        var path_len: u8 = 0;
        var pgno = tree.root;
        while (true) {
            const page = self.getPage(pgno);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                const result = page.search(key, cmp_fn);
                const ci = branchCi(result);
                if (path_len >= consts.MAX_DEPTH) return error.TreeTooDeep;
                path[path_len] = .{ .pgno = pgno, .ci = ci };
                path_len += 1;
                pgno = page.getNode(ci).getChildPgno();
            } else {
                // Touch all branch pages on the path (path-copying CoW).
                try self.touchPathDown(tree, path[0..path_len]);

                // Make leaf writable (path-copy if it's a committed page).
                const leaf_wp = try self.getWritablePage(pgno);
                const cow = leaf_wp.page;
                // If the leaf got a new pgno, update the parent's child pointer.
                if (leaf_wp.pgno != pgno) {
                    if (path_len == 0) {
                        tree.root = leaf_wp.pgno;
                    } else {
                        const par = path[path_len - 1];
                        const par_buf = self.dirty_list.get(par.pgno).?;
                        updateBranchChild(@ptrCast(@alignCast(par_buf.ptr)), par.ci, leaf_wp.pgno);
                    }
                }

                const ins_idx: u16 = if (append) blk: {
                    // append: caller guarantees key > all existing keys — O(1)
                    break :blk cow.getNumEntries();
                } else blk: {
                    // Normal binary-search path
                    const result = page.search(key, cmp_fn);
                    if (result.match) {
                        // Free overflow pages before the old node is deleted.
                        try self.freeOverflow(cow.getNode(result.index));
                        cow.delNode(result.index);
                        tree.items -= 1;
                    }
                    const res2 = cow.search(key, cmp_fn);
                    break :blk @intCast(res2.index);
                };

                // Determine whether the value needs overflow pages.
                const is_large = !append and val.len > OVERFLOW_THRESHOLD;
                var pgno_buf: [4]u8 = undefined;
                var overflow_pgno: u32 = 0;
                if (is_large) {
                    const n_pages: u32 = @intCast((val.len + OVERFLOW_DATA_PER_PAGE - 1) / OVERFLOW_DATA_PER_PAGE);
                    overflow_pgno = try self.allocOverflow(n_pages, val);
                    std.mem.writeInt(u32, &pgno_buf, overflow_pgno, .little);
                }
                const inline_val = if (is_large) pgno_buf[0..4] else val;
                const node_flags: u8 = if (is_large) page_mod.Node.F_BIGDATA else 0;
                const data_shim_ov = if (is_large) @as(u32, @truncate(val.len)) else @as(u32, 0);
                const sz = nodeSz(key.len, inline_val.len);

                if (cow.getFreeSpace() >= sz + 2) {
                    const node = try cow.putNode(ins_idx, key, inline_val, node_flags);
                    if (data_shim_ov != 0) node.data_shim = data_shim_ov;
                    tree.items += 1;
                    tree.mod_txnid = self.txnid;
                } else {
                    try self.leafSplit(tree, cow, ins_idx, key, inline_val, node_flags, data_shim_ov, path[0..path_len]);
                }
                break;
            }
        }
    }

    /// Allocates `val_size` bytes for a key without writing the value.
    /// Returns a mutable slice into the dirty buffer for zero-copy fill.
    fn btreeReserve(self: *Transaction, tree: *align(4) core_types.Tree, key: []const u8, val_size: usize, cmp_fn: CmpFn) ![]u8 {
        // Insert node with a zeroed value to claim the space.
        {
            var path: [consts.MAX_DEPTH]PathEntry = undefined;
            var path_len: u8 = 0;
            var pgno = tree.root;
            while (true) {
                const page = self.getPage(pgno);
                if ((page.flags & page_mod.P_BRANCH) != 0) {
                    const result = page.search(key, cmp_fn);
                    const ci = branchCi(result);
                    path[path_len] = .{ .pgno = pgno, .ci = ci };
                    path_len += 1;
                    pgno = page.getNode(ci).getChildPgno();
                } else {
                    const result = page.search(key, cmp_fn);
                    // Touch path before modifying anything.
                    try self.touchPathDown(tree, path[0..path_len]);
                    const leaf_wp = try self.getWritablePage(pgno);
                    const cow = leaf_wp.page;
                    if (leaf_wp.pgno != pgno) {
                        if (path_len == 0) {
                            tree.root = leaf_wp.pgno;
                        } else {
                            const par = path[path_len - 1];
                            const par_buf = self.dirty_list.get(par.pgno).?;
                            updateBranchChild(@ptrCast(@alignCast(par_buf.ptr)), par.ci, leaf_wp.pgno);
                        }
                    }
                    if (result.match) {
                        try self.freeOverflow(cow.getNode(result.index));
                        cow.delNode(result.index);
                        tree.items -= 1;
                    }
                    const res2 = cow.search(key, cmp_fn);
                    const ins_idx: u16 = @intCast(res2.index);
                    const zero_val = try self.allocator.alloc(u8, if (val_size > 0) val_size else 1);
                    defer self.allocator.free(zero_val);
                    @memset(zero_val, 0);
                    const payload = zero_val[0..val_size];
                    const sz = nodeSz(key.len, val_size);
                    if (cow.getFreeSpace() >= sz + 2) {
                        const node = try cow.putNode(ins_idx, key, payload, 0);
                        tree.items += 1;
                        tree.mod_txnid = self.txnid;
                        // Return mutable slice into the dirty buffer
                        const np = @as([*]u8, @ptrCast(node));
                        return np[8 + @as(usize, node.ksize) ..][0..val_size];
                    } else {
                        try self.leafSplit(tree, cow, ins_idx, key, payload, 0, 0, path[0..path_len]);
                        break;
                    }
                }
            }
        }
        // Re-traverse to find and return a pointer to the inserted node (tree may have split above).
        var pgno2 = tree.root;
        while (true) {
            const page = self.getPage(pgno2);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                const result = page.search(key, cmp_fn);
                pgno2 = page.getNode(branchCi(result)).getChildPgno();
            } else {
                const result = page.search(key, cmp_fn);
                if (!result.match) return error.NotFound;
                // pgno2 is already dirty after the first traversal touched the path.
                const writable = (try self.getWritablePage(pgno2)).page;
                const node = writable.getNode(result.index);
                const np = @as([*]u8, @ptrCast(node));
                return np[8 + @as(usize, node.ksize) ..][0..val_size];
            }
        }
    }

    // ─── Page split ──────────────────────────────────────────────────────────

    /// Splits a full leaf page, inserts the new key, and propagates the separator.
    ///
    /// The left page is compactly rebuilt via a temporary buffer: after splitting,
    /// `left` is re-initialised and repopulated, eliminating the fragmentation that
    /// a plain `truncate` call would leave behind.
    fn leafSplit(
        self: *Transaction,
        tree: *align(4) core_types.Tree,
        left: *page_mod.PageHeader,
        ins_idx: u16,
        key: []const u8,
        val: []const u8, // inline val (4-byte pgno for overflow nodes)
        node_flags: u8,
        data_shim_override: u32, // 0 = use val.len; non-zero = logical val size
        path: []const PathEntry,
    ) !void {
        const n = left.getNumEntries();
        const split: u16 = @intCast(n / 2);

        // Save left half [0..split) in a temporary buffer.
        const tmp_buf = try self.allocator.alloc(u8, PAGE_SIZE);
        defer self.allocator.free(tmp_buf);
        @memset(tmp_buf, 0);
        const tmp = @as(*page_mod.PageHeader, @ptrCast(@alignCast(tmp_buf.ptr)));
        tmp.init(PAGE_SIZE, page_mod.P_LEAF);
        try left.copyNodes(tmp, 0, split);

        // Allocate and populate right page with [split..n).
        const right_pgno = try self.allocPage();
        // right_pgno is newly allocated (>= first_unallocated_snapshot) — same pgno returned.
        const right = (try self.getWritablePage(right_pgno)).page;
        right.init(PAGE_SIZE, page_mod.P_LEAF);
        right.pgno = right_pgno;
        right.txnid = self.txnid;
        try left.copyNodes(right, split, n - split);

        // Rebuild left compactly from the temp buffer (init does not touch pgno).
        left.init(PAGE_SIZE, page_mod.P_LEAF);
        left.txnid = self.txnid;
        try tmp.copyNodes(left, 0, split);

        // Insert the new key into whichever half owns its index.
        const put_result = if (ins_idx <= split)
            try left.putNode(ins_idx, key, val, node_flags)
        else
            try right.putNode(ins_idx - split, key, val, node_flags);
        // For overflow nodes, override data_shim with the logical value size.
        if (data_shim_override != 0) put_result.data_shim = data_shim_override;

        tree.items += 1;
        tree.leaf_pages += 1;
        tree.mod_txnid = self.txnid;

        // Separator key = first key of the right page.
        var sep_buf: [512]u8 = undefined;
        const raw = right.getNode(0).getKey();
        const slen = @min(raw.len, 512);
        @memcpy(sep_buf[0..slen], raw[0..slen]);
        try self.pushSep(tree, right_pgno, sep_buf[0..slen], path);
    }

    /// Propagates a separator key to the parent branch; creates a new root if there is no parent.
    fn pushSep(
        self: *Transaction,
        tree: *align(4) core_types.Tree,
        right_pgno: u32,
        sep_key: []const u8,
        path: []const PathEntry,
    ) anyerror!void {
        if (path.len == 0) {
            // No parent: create a new branch root.
            const root_pgno = try self.allocPage();
            // root_pgno is newly allocated — same pgno returned.
            const root_page = (try self.getWritablePage(root_pgno)).page;
            root_page.init(PAGE_SIZE, page_mod.P_BRANCH);
            root_page.pgno = root_pgno;
            root_page.txnid = self.txnid;

            var buf: [4]u8 = undefined;
            std.mem.writeInt(u32, &buf, tree.root, .little);
            _ = try root_page.putNode(0, "", &buf, 0); // leftmost entry has an empty key
            std.mem.writeInt(u32, &buf, right_pgno, .little);
            _ = try root_page.putNode(1, sep_key, &buf, 0);

            tree.root = root_pgno;
            tree.height += 1;
            tree.branch_pages += 1;
            return;
        }

        const pe = path[path.len - 1];
        // pe.pgno is already dirty after touchPathDown — same pgno returned.
        const parent = (try self.getWritablePage(pe.pgno)).page;
        const ins_ci = pe.ci + 1;

        var buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &buf, right_pgno, .little);
        const sz = nodeSz(sep_key.len, 4); // branch value = 4-byte pgno

        if (parent.getFreeSpace() >= sz + 2) {
            _ = try parent.putNode(ins_ci, sep_key, &buf, 0);
        } else {
            // Parent branch is also full: split recursively.
            try self.branchSplit(tree, parent, pe.pgno, ins_ci, sep_key, right_pgno, path[0 .. path.len - 1]);
        }
    }

    /// Splits a full branch page, inserts the separator, and promotes the middle key to the grandparent.
    fn branchSplit(
        self: *Transaction,
        tree: *align(4) core_types.Tree,
        left: *page_mod.PageHeader,
        left_pgno: u32,
        ins_ci: u16,
        sep_key: []const u8,
        sep_pgno: u32,
        parent_path: []const PathEntry,
    ) anyerror!void {
        const n = left.getNumEntries();

        // Temporary buffer large enough to hold n+1 entries.
        const tmp_buf = try self.allocator.alloc(u8, PAGE_SIZE * 2);
        defer self.allocator.free(tmp_buf);
        @memset(tmp_buf, 0);
        const tmp = @as(*page_mod.PageHeader, @ptrCast(@alignCast(tmp_buf.ptr)));
        tmp.init(PAGE_SIZE * 2, page_mod.P_BRANCH);

        try left.copyNodes(tmp, 0, n);

        var pg_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &pg_buf, sep_pgno, .little);
        _ = try tmp.putNode(ins_ci, sep_key, &pg_buf, 0);

        const total: u16 = tmp.getNumEntries(); // n + 1
        const mid: u16 = @intCast(total / 2);

        // Promoted entry = tmp[mid].
        const promo_node = tmp.getNode(mid);
        const promo_raw = promo_node.getKey();
        const promo_child = promo_node.getChildPgno();
        var promo_buf: [512]u8 = undefined;
        const promo_len = @min(promo_raw.len, 512);
        @memcpy(promo_buf[0..promo_len], promo_raw[0..promo_len]);
        const promo_key = promo_buf[0..promo_len];

        // Rebuild left with tmp[0..mid).
        left.init(PAGE_SIZE, page_mod.P_BRANCH);
        left.pgno = left_pgno;
        left.txnid = self.txnid;
        try tmp.copyNodes(left, 0, mid);

        // Build right: ("", promo_child) followed by tmp[mid+1..total).
        const right_pgno = try self.allocPage();
        // right_pgno is newly allocated — same pgno returned by getWritablePage.
        const right = (try self.getWritablePage(right_pgno)).page;
        right.init(PAGE_SIZE, page_mod.P_BRANCH);
        right.pgno = right_pgno;
        right.txnid = self.txnid;

        std.mem.writeInt(u32, &pg_buf, promo_child, .little);
        _ = try right.putNode(0, "", &pg_buf, 0);

        if (mid + 1 < total) {
            try tmp.copyNodes(right, mid + 1, total - mid - 1);
        }

        tree.branch_pages += 1;

        // Push promo_key up to the grandparent, pointing at right_pgno.
        try self.pushSep(tree, right_pgno, promo_key, parent_path);
    }

    // ─── B-tree rebalancing ───────────────────────────────────────────────────

    /// Rebalances / coalesces after a deletion in a branch page (may recurse).
    /// Handles: empty branch → remove from grandparent; root with 1 child → height collapse.
    fn rebalanceBranch(
        self: *Transaction,
        tree: *align(4) core_types.Tree,
        branch: *page_mod.PageHeader,
        pgno: u32,
        path: []const PathEntry,
    ) anyerror!void {
        const n = branch.getNumEntries();

        if (n == 0) {
            if (path.len == 0) {
                // Empty branch root: convert to an empty leaf (height shrinks by 1).
                branch.init(PAGE_SIZE, page_mod.P_LEAF);
                branch.txnid = self.txnid;
                tree.height -= 1;
                tree.branch_pages -= 1;
                tree.leaf_pages += 1; // reuse the same page as a leaf
            } else {
                const gpe = path[path.len - 1];
                // gpe.pgno is already dirty after touchPathDown — same pgno returned.
                const gp = (try self.getWritablePage(gpe.pgno)).page;
                try removeParentEntry(gp, gpe.ci);
                try self.freePage(pgno);
                tree.branch_pages -= 1;
                try self.rebalanceBranch(tree, gp, gpe.pgno, path[0 .. path.len - 1]);
            }
            return;
        }

        if (n == 1 and path.len == 0) {
            // Root with a single child: collapse — the child becomes the new root.
            const child_pgno = branch.getNode(0).getChildPgno();
            try self.freePage(pgno);
            tree.root = child_pgno;
            tree.height -= 1;
            tree.branch_pages -= 1;
        }
        // n >= 2, or non-root branch: no further action needed.
    }

    /// Attempts to merge a leaf with a sibling when fill-factor drops below 50%.
    /// On success, triggers rebalanceBranch to propagate the change upward.
    fn rebalanceLeaf(
        self: *Transaction,
        tree: *align(4) core_types.Tree,
        leaf: *page_mod.PageHeader,
        leaf_pgno: u32,
        path: []const PathEntry,
    ) anyerror!void {
        if (leaf.getUsedSpace() >= REBALANCE_THRESHOLD) return;
        if (path.len == 0) return; // leaf is the root — nothing to merge into

        const pe = path[path.len - 1];
        // pe.pgno is already dirty after touchPathDown — same pgno returned.
        const parent = (try self.getWritablePage(pe.pgno)).page;
        const par_n = parent.getNumEntries();

        // Try merging with the right sibling (parent entry ci+1).
        if (pe.ci + 1 < par_n) {
            const right_pgno = parent.getNode(pe.ci + 1).getChildPgno();
            const right = self.getPage(right_pgno);
            const right_n = right.getNumEntries();
            // Space needed in leaf to absorb right's contents.
            const right_need = (right.getUsedSpace() -| 20) + @as(u32, right_n) * 2;
            if (leaf.getFreeSpace() >= right_need) {
                try right.copyNodes(leaf, 0, right_n);
                parent.delNode(pe.ci + 1);
                try self.freePage(right_pgno);
                tree.leaf_pages -= 1;
                tree.mod_txnid = self.txnid;
                try self.rebalanceBranch(tree, parent, pe.pgno, path[0 .. path.len - 1]);
                return;
            }
        }

        // Try merging into the left sibling (parent entry ci-1).
        if (pe.ci > 0) {
            const lsib_pgno = parent.getNode(pe.ci - 1).getChildPgno();
            const lsib = self.getPage(lsib_pgno);
            const leaf_n = leaf.getNumEntries();
            // Space needed in left sibling to absorb leaf's contents.
            const leaf_need = (leaf.getUsedSpace() -| 20) + @as(u32, leaf_n) * 2;
            if (lsib.getFreeSpace() >= leaf_need) {
                // lsib_pgno may be a committed page — path-copy it.
                const lsib_wp = try self.getWritablePage(lsib_pgno);
                const lsib_cow = lsib_wp.page;
                // If sibling got a new pgno, update the parent's child pointer.
                if (lsib_wp.pgno != lsib_pgno) {
                    updateBranchChild(parent, pe.ci - 1, lsib_wp.pgno);
                }
                try leaf.copyNodes(lsib_cow, 0, leaf_n);
                try removeParentEntry(parent, pe.ci);
                try self.freePage(leaf_pgno);
                tree.leaf_pages -= 1;
                tree.mod_txnid = self.txnid;
                try self.rebalanceBranch(tree, parent, pe.pgno, path[0 .. path.len - 1]);
                return;
            }
        }
        // Merge not possible: page is underfull but structurally sound — leave as-is.
    }

    /// Exact-key delete. Returns error.NotFound if the key does not exist.
    fn btreeDel(self: *Transaction, tree: *align(4) core_types.Tree, key: []const u8, cmp_fn: CmpFn) !void {
        var path: [consts.MAX_DEPTH]PathEntry = undefined;
        var path_len: u8 = 0;
        var pgno = tree.root;
        while (true) {
            const page = self.getPage(pgno);
            const result = page.search(key, cmp_fn);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                if (!result.match and result.index == 0) return error.NotFound;
                const ci: u16 = if (result.match) result.index else result.index - 1;
                if (path_len >= consts.MAX_DEPTH) return error.TreeTooDeep;
                path[path_len] = .{ .pgno = pgno, .ci = ci };
                path_len += 1;
                pgno = page.getNode(ci).getChildPgno();
            } else {
                if (!result.match) return error.NotFound;
                // Touch all branch pages on the path (path-copying CoW).
                try self.touchPathDown(tree, path[0..path_len]);
                // Make leaf writable (path-copy if it's a committed page).
                const leaf_wp = try self.getWritablePage(pgno);
                const cow = leaf_wp.page;
                const cow_pgno = leaf_wp.pgno;
                if (cow_pgno != pgno) {
                    if (path_len == 0) {
                        tree.root = cow_pgno;
                    } else {
                        const par = path[path_len - 1];
                        const par_buf = self.dirty_list.get(par.pgno).?;
                        updateBranchChild(@ptrCast(@alignCast(par_buf.ptr)), par.ci, cow_pgno);
                    }
                }
                try self.freeOverflow(cow.getNode(result.index));
                cow.delNode(result.index);
                tree.items -= 1;
                tree.mod_txnid = self.txnid;
                if (cow.getUsedSpace() < REBALANCE_THRESHOLD) {
                    try self.rebalanceLeaf(tree, cow, cow_pgno, path[0..path_len]);
                }
                break;
            }
        }
    }

    /// Deletes all entries that share the given prefix (DupSort).
    // ─── Canary API ─────────────────────────────────────────────────────────

    /// Writes four u64 canary values into the transaction's meta snapshot.
    /// The canary is persisted on commit and can be used for application-level
    /// integrity checks.
    pub fn putCanary(self: *Transaction, x: u64, y: u64, z: u64, v: u64) !void {
        if (self.rdonly) return error.BadTxn;
        self.meta.canary = .{ .x = x, .y = y, .z = z, .v = v };
    }

    /// Returns the current canary from the transaction's meta snapshot.
    pub fn getCanary(self: *const Transaction) core_types.Canary {
        return self.meta.canary;
    }

    // ─── replace + putMultiple ──────────────────────────────────────────────

    /// Atomically fetches the old value for `key` and writes `new_val`.
    /// Sets `old_val_out` to a slice into the mmap/dirty buffer (valid until
    /// the next write operation on this transaction).
    pub fn replace(
        self: *Transaction,
        dbi: types.Dbi,
        key: []const u8,
        new_val: []const u8,
        old_val_out: *?[]const u8,
    ) !void {
        if (self.rdonly) return error.BadTxn;
        try self.checkDbi(dbi);
        old_val_out.* = try self.get(dbi, key);
        try self.put(dbi, key, new_val, .{});
    }

    /// Inserts multiple values for `key` in a single call (DupSort DBIs).
    /// Each value is inserted with the given `flags`.
    pub fn putMultiple(
        self: *Transaction,
        dbi: types.Dbi,
        key: []const u8,
        vals: []const []const u8,
        flags: types.PutFlags,
    ) !void {
        for (vals) |val| try self.put(dbi, key, val, flags);
    }

    // ─── compacting backup ──────────────────────────────────────────────────

    /// Recursively collects all (key, val) pairs in the B-tree rooted at
    /// `root_pgno` into `pairs`. Both key and val are heap-allocated copies.
    fn collectTreePairs(
        self: *Transaction,
        root_pgno: u32,
        pairs: *std.ArrayListUnmanaged([2][]u8),
        alloc: std.mem.Allocator,
    ) !void {
        if (root_pgno == consts.PGNO_INVALID) return;
        const page = self.getPage(root_pgno);
        const n = page.getNumEntries();
        if ((page.flags & page_mod.P_BRANCH) != 0) {
            var i: u16 = 0;
            while (i < n) : (i += 1)
                try self.collectTreePairs(page.getNode(i).getChildPgno(), pairs, alloc);
        } else {
            var i: u16 = 0;
            while (i < n) : (i += 1) {
                const node = page.getNode(i);
                const raw_key = node.getKey();
                const raw_val = if (node.isOverflow()) try self.readOverflow(node) else node.getData();
                const kc = try alloc.dupe(u8, raw_key);
                errdefer alloc.free(kc);
                const vc = try alloc.dupe(u8, raw_val);
                errdefer alloc.free(vc);
                try pairs.append(alloc, .{ kc, vc });
            }
        }
    }

    /// Creates a compacting copy of the database to `dest_path`.
    /// Only live (reachable) pages are written; freed/GC pages are omitted,
    /// producing a smaller file with no fragmentation.
    /// `self` must be a read-only transaction.
    pub fn copyCompact(self: *Transaction, dest_path: [:0]const u8) !void {
        if (!self.rdonly) return error.BadTxn;
        const alloc = std.heap.page_allocator;
        const map_size = @as(usize, self.meta.geometry.upper) * PAGE_SIZE;

        // 1. Enumerate all named DBIs from the main catalog using parallel
        //    ArrayListUnmanaged lists (consistent with engine coding style).
        var cat_names = std.ArrayListUnmanaged([]u8){};
        var cat_trees = std.ArrayListUnmanaged(core_types.Tree){};
        var cat_flags = std.ArrayListUnmanaged(types.DbFlags){};
        defer {
            for (cat_names.items) |n| alloc.free(n);
            cat_names.deinit(alloc);
            cat_trees.deinit(alloc);
            cat_flags.deinit(alloc);
        }

        if (self.meta.trees.main.items > 0) {
            var cat_pairs = std.ArrayListUnmanaged([2][]u8){};
            defer {
                for (cat_pairs.items) |p| {
                    alloc.free(p[0]);
                    alloc.free(p[1]);
                }
                cat_pairs.deinit(alloc);
            }
            try self.collectTreePairs(self.meta.trees.main.root, &cat_pairs, alloc);
            for (cat_pairs.items) |pair| {
                if (pair[1].len < @sizeOf(core_types.Tree)) continue;
                var tree: core_types.Tree = undefined;
                @memcpy(std.mem.asBytes(&tree), pair[1][0..@sizeOf(core_types.Tree)]);
                const flags_valid = (tree.flags & DB_FLAGS_VALID) != 0;
                const db_flags = if (flags_valid) u16ToDbFlags(@as(u16, @truncate(tree.flags))) else types.DbFlags{};
                try cat_names.append(alloc, pair[0]); // transfer ownership
                try cat_trees.append(alloc, tree);
                try cat_flags.append(alloc, db_flags);
            }
            // Null out keys we transferred to prevent double-free in the defer.
            for (cat_pairs.items) |*p| p[0] = &.{};
        }

        // 2. Open fresh destination environment.
        var dest_env = try env_mod.Environment.open(dest_path, .{}, self.env.max_dbs, map_size);
        defer dest_env.close();

        var dest_txn = try Transaction.begin(&dest_env, null, .{});
        errdefer dest_txn.abort();

        // 3. Copy each named DBI.
        var ci: usize = 0;
        while (ci < cat_names.items.len) : (ci += 1) {
            const entry_name = cat_names.items[ci];
            const entry_tree = cat_trees.items[ci];
            const entry_flags = cat_flags.items[ci];

            var data_pairs = std.ArrayListUnmanaged([2][]u8){};
            defer {
                for (data_pairs.items) |p| {
                    alloc.free(p[0]);
                    alloc.free(p[1]);
                }
                data_pairs.deinit(alloc);
            }
            if (entry_tree.items > 0)
                try self.collectTreePairs(entry_tree.root, &data_pairs, alloc);

            // Build null-terminated DBI name.
            var name_buf: [256]u8 = undefined;
            const nlen = @min(entry_name.len, 255);
            @memcpy(name_buf[0..nlen], entry_name[0..nlen]);
            name_buf[nlen] = 0;
            const name_z: [:0]const u8 = name_buf[0..nlen :0];

            var open_flags = entry_flags;
            open_flags.create = true;
            const dest_dbi = try dest_txn.openDbi(name_z, open_flags);
            const dest_state = &dest_txn.dbi_state[dest_dbi];

            for (data_pairs.items) |pair| {
                // Composite keys are already fully encoded; insert via btreePut
                // directly so the DupSort path of put() doesn't double-encode them.
                const cmp = if (entry_flags.dupsort) cmpLexicographic else dest_state.cmp_fn;
                try dest_txn.btreePut(&dest_state.tree, pair[0], pair[1], false, cmp);
                dest_state.tree.items += 1;
                dest_state.dirty = true;
            }
        }

        try dest_txn.commit();
    }

    /// Deletes all entries whose key starts with `prefix`.
    /// Correctly crosses page boundaries by using btreeDel in a loop,
    /// re-descending from the root after each deletion.
    fn btreeDelPrefix(self: *Transaction, tree: *align(4) core_types.Tree, prefix: []const u8, cmp_fn: CmpFn) !void {
        while (true) {
            // Descend from root to find the first key that starts with prefix.
            var pgno = tree.root;
            var found: ?[]u8 = null;
            while (true) {
                const page = self.getPage(pgno);
                const result = page.search(prefix, cmp_fn);
                if ((page.flags & page_mod.P_BRANCH) != 0) {
                    pgno = page.getNode(branchCi(result)).getChildPgno();
                } else {
                    if (result.index < page.getNumEntries()) {
                        const k = page.getNode(result.index).getKey();
                        if (std.mem.startsWith(u8, k, prefix)) {
                            // Heap-copy the key before deletion invalidates the pointer.
                            found = try self.allocator.dupe(u8, k);
                        }
                    }
                    break;
                }
            }
            const target_key = found orelse break;
            defer self.allocator.free(target_key);
            try self.btreeDel(tree, target_key, cmp_fn);
        }
        tree.mod_txnid = self.txnid;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Cursor
// ─────────────────────────────────────────────────────────────────────────────

pub const CursorEntry = struct {
    pgno: u32,
    index: u16,
};

pub const Cursor = struct {
    txn: *Transaction,
    dbi: types.Dbi,
    stack: [consts.MAX_DEPTH]CursorEntry,
    depth: u8,
    valid: bool,
    dupsort: bool,
    key_prefix_buf: [512]u8,
    key_prefix_len: u16,
    /// Scratch buffer for REVERSEDUP value decoding (un-complement storage).
    val_decode_buf: [512]u8,

    fn open(txn: *Transaction, dbi: types.Dbi) Cursor {
        return Cursor{
            .txn = txn,
            .dbi = dbi,
            .stack = undefined,
            .depth = 0,
            .valid = false,
            .dupsort = txn.dbi_state[dbi].flags.dupsort,
            .key_prefix_buf = undefined,
            .key_prefix_len = 0,
            .val_decode_buf = undefined,
        };
    }

    pub fn close(self: *Cursor) void {
        self.valid = false;
    }

    /// Returns a copy of this cursor sharing the same transaction.
    /// The copy has the same position; navigation is independent after the call.
    /// No heap allocation: Cursor contains no owned pointers.
    pub fn copy(self: *const Cursor) Cursor {
        return self.*;
    }

    /// Compares the current positions of two cursors that share the same DBI.
    /// Returns .lt / .eq / .gt.  An invalid (exhausted) cursor is treated as
    /// "past the end" — greater than any valid position.
    pub fn compare(self: *const Cursor, other: *const Cursor) std.math.Order {
        if (!self.valid and !other.valid) return .eq;
        if (!self.valid) return .gt;
        if (!other.valid) return .lt;
        const ka = self.currentKey() orelse return .gt;
        const kb = other.currentKey() orelse return .gt;
        const flags = self.txn.dbi_state[self.dbi].flags;
        // DupSort trees store composite keys; always compare lexicographically.
        if (flags.dupsort) return cmpLexicographic(ka, kb);
        return self.txn.dbi_state[self.dbi].cmp_fn(ka, kb);
    }

    /// Reads the raw key bytes for the current cursor position directly from
    /// the page without any decoding.  Returns null if the cursor is invalid.
    /// Note: depth=0 is valid for a single-level (leaf-root) tree.
    /// Uses txn.getPage() so dirty (uncommitted) pages are found correctly.
    fn currentKey(self: *const Cursor) ?[]const u8 {
        if (!self.valid) return null;
        const entry = self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        if (entry.index >= page.getNumEntries()) return null;
        return page.getNode(entry.index).getKey();
    }

    /// Re-binds this cursor to `new_txn` without allocating a new cursor.
    /// The same DBI must already be open in `new_txn`.
    /// Resets all navigation state; the caller must re-position the cursor.
    pub fn renew(self: *Cursor, new_txn: *Transaction) !void {
        if (self.dbi == 0 or new_txn.next_dbi <= self.dbi) return error.BadDbi;
        if (!new_txn.dbi_state[self.dbi].open) return error.BadDbi;
        self.txn = new_txn;
        self.valid = false;
        self.key_prefix_len = 0;
    }

    fn getTree(self: *const Cursor) core_types.Tree {
        return self.txn.dbi_state[self.dbi].tree;
    }

    /// Bitwise-complements `raw_val` into `val_decode_buf` and returns the slice.
    /// Used to undo REVERSEDUP storage encoding on cursor reads.
    fn decodeRevDup(self: *Cursor, raw_val: []const u8) []const u8 {
        const vlen = @min(raw_val.len, self.val_decode_buf.len);
        for (raw_val[0..vlen], 0..) |b, i| self.val_decode_buf[i] = ~b;
        return self.val_decode_buf[0..vlen];
    }

    fn currentKV(self: *Cursor) ?types.KV {
        if (!self.valid) return null;
        const entry = self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        if (entry.index >= page.getNumEntries()) return null;
        const node = page.getNode(entry.index);
        const full_key = node.getKey();
        if (self.dupsort and self.key_prefix_len > 0) {
            const kpl = @as(usize, self.key_prefix_len);
            if (kpl > full_key.len) return null;
            const raw_val = full_key[kpl..];
            if (self.txn.dbi_state[self.dbi].flags.reversedup)
                return types.KV{ .key = full_key[0..kpl], .val = self.decodeRevDup(raw_val) };
            return types.KV{ .key = full_key[0..kpl], .val = raw_val };
        }
        if (node.isOverflow()) {
            const val = self.txn.readOverflow(node) catch return null;
            return types.KV{ .key = full_key, .val = val };
        }
        return types.KV{ .key = full_key, .val = node.getData() };
    }

    // ─── Internal navigation ─────────────────────────────────────────────────

    /// Descends the leftmost path of the subtree rooted at `start_pgno`.
    fn descendLeft(self: *Cursor, start_pgno: u32, start_depth: u8) bool {
        var pgno = start_pgno;
        var d = start_depth;
        while (true) {
            if (d >= consts.MAX_DEPTH) return false;
            const page = self.txn.getPage(pgno);
            const n = page.getNumEntries();
            self.stack[d] = .{ .pgno = pgno, .index = 0 };
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                if (n == 0) {
                    self.depth = d;
                    return false;
                }
                pgno = page.getNode(0).getChildPgno();
                d += 1;
            } else {
                self.depth = d;
                return n > 0;
            }
        }
    }

    /// Descends the rightmost path of the subtree rooted at `start_pgno`.
    fn descendRight(self: *Cursor, start_pgno: u32, start_depth: u8) bool {
        var pgno = start_pgno;
        var d = start_depth;
        while (true) {
            if (d >= consts.MAX_DEPTH) return false;
            const page = self.txn.getPage(pgno);
            const n = page.getNumEntries();
            if (n == 0) {
                self.stack[d] = .{ .pgno = pgno, .index = 0 };
                self.depth = d;
                return false;
            }
            self.stack[d] = .{ .pgno = pgno, .index = @as(u16, @truncate(n - 1)) };
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                pgno = page.getNode(@as(u16, @truncate(n - 1))).getChildPgno();
                d += 1;
            } else {
                self.depth = d;
                return true;
            }
        }
    }

    /// Descends to the leaf for `key`. exact=true → exact match; false → lower-bound / prefix.
    fn seekKey(self: *Cursor, key: []const u8, exact: bool) !bool {
        const tree = self.getTree();
        const cmp_fn = self.txn.dbi_state[self.dbi].cmp_fn;
        var pgno = tree.root;
        var d: u8 = 0;
        while (true) {
            if (d >= consts.MAX_DEPTH) return error.CursorFull;
            const page = self.txn.getPage(pgno);
            const result = page.search(key, cmp_fn);
            if ((page.flags & page_mod.P_BRANCH) != 0) {
                const ci = branchCi(result);
                self.stack[d] = .{ .pgno = pgno, .index = ci };
                pgno = page.getNode(ci).getChildPgno();
                d += 1;
            } else {
                self.stack[d] = .{ .pgno = pgno, .index = result.index };
                self.depth = d;
                if (exact) {
                    self.valid = result.match;
                } else {
                    self.valid = result.index < page.getNumEntries();
                }
                return self.valid;
            }
        }
    }

    // ─── Public API ──────────────────────────────────────────────────────────

    pub fn first(self: *Cursor) !?types.KV {
        self.key_prefix_len = 0;
        const tree = self.getTree();
        self.valid = self.descendLeft(tree.root, 0);
        return self.currentKV();
    }

    pub fn last(self: *Cursor) !?types.KV {
        self.key_prefix_len = 0;
        const tree = self.getTree();
        self.valid = self.descendRight(tree.root, 0);
        return self.currentKV();
    }

    pub fn next(self: *Cursor) !?types.KV {
        if (!self.valid) return null;
        const entry = &self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);

        // Advance within the current leaf page.
        if (entry.index + 1 < page.getNumEntries()) {
            entry.index += 1;
            return self.currentKV();
        }

        // Back-track through branch pages.
        if (self.depth == 0) {
            self.valid = false;
            return null;
        }

        var d = self.depth;
        while (d > 0) {
            d -= 1;
            const pe = &self.stack[d];
            const ppage = self.txn.getPage(pe.pgno);
            if (pe.index + 1 < ppage.getNumEntries()) {
                pe.index += 1;
                const child_pgno = ppage.getNode(pe.index).getChildPgno();
                self.valid = self.descendLeft(child_pgno, d + 1);
                return self.currentKV();
            }
        }
        self.valid = false;
        return null;
    }

    pub fn current(self: *Cursor) !?types.KV {
        return self.currentKV();
    }

    /// Positions on an exact key (non-dupsort) or the first dup for the given key (dupsort).
    pub fn find(self: *Cursor, key: []const u8) !bool {
        if (self.dupsort) {
            const klen = @min(key.len, 512);
            @memcpy(self.key_prefix_buf[0..klen], key[0..klen]);
            self.key_prefix_len = @intCast(klen);
            if (!(try self.seekKey(key, false))) return false;
            // For dupsort, the found composite key must start with our plain key.
            const _e = self.stack[self.depth];
            const _pg = self.txn.getPage(_e.pgno);
            const _k = _pg.getNode(_e.index).getKey();
            self.valid = std.mem.startsWith(u8, _k, key);
            return self.valid;
        } else {
            self.key_prefix_len = 0;
            return self.seekKey(key, true);
        }
    }

    /// Positions on the first key >= `key`. Returns null if none exists.
    pub fn findGe(self: *Cursor, key: []const u8) !?types.KV {
        self.key_prefix_len = 0;
        if (!(try self.seekKey(key, false))) return null;
        return self.currentKV();
    }

    /// Positions on the first key >= `key` and returns whether it was an exact match.
    /// Returns null if no key >= `key` exists.
    pub fn seekRange(self: *Cursor, key: []const u8) !?types.SeekResult {
        self.key_prefix_len = 0;
        if (!(try self.seekKey(key, false))) return null;
        const kv = self.currentKV() orelse return null;
        const cmp_fn = self.txn.dbi_state[self.dbi].cmp_fn;
        const exact = cmp_fn(kv.key, key) == .eq;
        return types.SeekResult{ .kv = kv, .exact = exact };
    }

    /// Advances to the next value under the same dup-key. Returns the value portion.
    pub fn nextDup(self: *Cursor) !?[]const u8 {
        if (!self.valid or !self.dupsort or self.key_prefix_len == 0) return null;
        const entry = &self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);

        if (entry.index + 1 >= page.getNumEntries()) {
            self.valid = false;
            return null;
        }
        entry.index += 1;
        const node = page.getNode(entry.index);
        const k = node.getKey();
        const prefix = self.key_prefix_buf[0..self.key_prefix_len];
        if (!std.mem.startsWith(u8, k, prefix)) {
            entry.index -= 1;
            self.valid = false;
            return null;
        }
        const raw_val = k[@as(usize, self.key_prefix_len)..];
        if (self.txn.dbi_state[self.dbi].flags.reversedup) return self.decodeRevDup(raw_val);
        return raw_val;
    }

    /// Moves backward one entry. Mirror of `next()`.
    pub fn prev(self: *Cursor) !?types.KV {
        if (!self.valid) return null;
        const entry = &self.stack[self.depth];

        // Go backward within the current leaf page.
        if (entry.index > 0) {
            entry.index -= 1;
            return self.currentKV();
        }

        // Back-track through branch pages.
        if (self.depth == 0) {
            self.valid = false;
            return null;
        }

        var d = self.depth;
        while (d > 0) {
            d -= 1;
            const pe = &self.stack[d];
            if (pe.index > 0) {
                pe.index -= 1;
                const child_pgno = self.txn.getPage(pe.pgno).getNode(pe.index).getChildPgno();
                self.valid = self.descendRight(child_pgno, d + 1);
                return self.currentKV();
            }
        }
        self.valid = false;
        return null;
    }

    /// Moves backward within the same DupSort key prefix. Returns the value portion.
    pub fn prevDup(self: *Cursor) !?[]const u8 {
        if (!self.valid or !self.dupsort or self.key_prefix_len == 0) return null;
        const entry = &self.stack[self.depth];
        if (entry.index == 0) return null;
        entry.index -= 1;
        const page = self.txn.getPage(entry.pgno);
        const node = page.getNode(entry.index);
        const k = node.getKey();
        const prefix = self.key_prefix_buf[0..self.key_prefix_len];
        if (!std.mem.startsWith(u8, k, prefix)) {
            entry.index += 1; // stepped before our prefix range — restore
            return null;
        }
        const raw_val = k[@as(usize, self.key_prefix_len)..];
        if (self.txn.dbi_state[self.dbi].flags.reversedup) return self.decodeRevDup(raw_val);
        return raw_val;
    }

    /// Repositions to the first (smallest) dup of the current DupSort key.
    pub fn firstDup(self: *Cursor) !?[]const u8 {
        if (!self.valid or !self.dupsort or self.key_prefix_len == 0) return null;
        const entry = &self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        const prefix = self.key_prefix_buf[0..self.key_prefix_len];
        // Scan backward while the previous entry still matches our prefix.
        while (entry.index > 0) {
            const k = page.getNode(entry.index - 1).getKey();
            if (!std.mem.startsWith(u8, k, prefix)) break;
            entry.index -= 1;
        }
        const k = page.getNode(entry.index).getKey();
        const raw_val = k[@as(usize, self.key_prefix_len)..];
        if (self.txn.dbi_state[self.dbi].flags.reversedup) return self.decodeRevDup(raw_val);
        return raw_val;
    }

    /// Repositions to the last (largest) dup of the current DupSort key.
    pub fn lastDup(self: *Cursor) !?[]const u8 {
        if (!self.valid or !self.dupsort or self.key_prefix_len == 0) return null;
        const entry = &self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        const prefix = self.key_prefix_buf[0..self.key_prefix_len];
        const n = page.getNumEntries();
        // Scan forward while the next entry still matches our prefix.
        while (entry.index + 1 < n) {
            const k = page.getNode(entry.index + 1).getKey();
            if (!std.mem.startsWith(u8, k, prefix)) break;
            entry.index += 1;
        }
        const k = page.getNode(entry.index).getKey();
        const raw_val = k[@as(usize, self.key_prefix_len)..];
        if (self.txn.dbi_state[self.dbi].flags.reversedup) return self.decodeRevDup(raw_val);
        return raw_val;
    }

    /// Counts how many dup entries exist for the current key.
    pub fn countDups(self: *Cursor) !usize {
        if (!self.valid) return 0;
        if (!self.dupsort or self.key_prefix_len == 0) return 1;
        const entry = self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        const prefix = self.key_prefix_buf[0..self.key_prefix_len];
        var count: usize = 0;
        var i = entry.index;
        while (i < page.getNumEntries()) : (i += 1) {
            if (!std.mem.startsWith(u8, page.getNode(i).getKey(), prefix)) break;
            count += 1;
        }
        return count;
    }

    /// Positions on the exact (key, val) pair in a DupSort DBI.
    pub fn findDup(self: *Cursor, key: []const u8, val: []const u8) !bool {
        if (!self.dupsort) return error.Incompatible;
        const reversedup = self.txn.dbi_state[self.dbi].flags.reversedup;
        const composite = try self.txn.makeCompositeKey(key, val, reversedup);
        defer self.txn.allocator.free(composite);
        const klen = @min(key.len, 512);
        @memcpy(self.key_prefix_buf[0..klen], key[0..klen]);
        self.key_prefix_len = @intCast(klen);
        return self.seekKey(composite, true);
    }

    /// Inserts or updates via cursor.
    /// When `flags.current` is set, the entry the cursor is positioned on is
    /// replaced: the current entry is deleted (retaining cursor validity) and
    /// the new (key, val) is inserted. The cursor position after this call is
    /// not guaranteed.
    pub fn put(self: *Cursor, key: []const u8, val: []const u8, flags: types.PutFlags) !void {
        if (flags.current) {
            if (!self.valid) return error.BadTxn;
            try self.del();
            try self.txn.put(self.dbi, key, val, .{});
            return;
        }
        return self.txn.put(self.dbi, key, val, flags);
    }

    // ─── getBoth / getBothRange ─────────────────────────────────────────────

    /// Exact (key, val) lookup in a DupSort DBI. Alias for `findDup`.
    pub fn getBoth(self: *Cursor, key: []const u8, val: []const u8) !bool {
        return self.findDup(key, val);
    }

    /// Positions on the first val >= `val` within the dup set of `key`.
    /// Returns the located value, or null if no dup of `key` is >= `val`.
    pub fn getBothRange(self: *Cursor, key: []const u8, val: []const u8) !?[]const u8 {
        if (!self.dupsort) return error.Incompatible;
        const reversedup = self.txn.dbi_state[self.dbi].flags.reversedup;
        // Lower-bound seek on the composite key "key || val".
        const composite = try self.txn.makeCompositeKey(key, val, reversedup);
        defer self.txn.allocator.free(composite);
        const klen = @min(key.len, 512);
        @memcpy(self.key_prefix_buf[0..klen], key[0..klen]);
        self.key_prefix_len = @intCast(klen);
        _ = try self.seekKey(composite, false); // lower-bound, not exact
        if (!self.valid) return null;
        const entry = self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        const node = page.getNode(entry.index);
        const k = node.getKey();
        const prefix = self.key_prefix_buf[0..self.key_prefix_len];
        if (!std.mem.startsWith(u8, k, prefix)) {
            self.valid = false;
            return null;
        }
        const raw_val = k[@as(usize, self.key_prefix_len)..];
        if (reversedup) return self.decodeRevDup(raw_val);
        return raw_val;
    }

    /// Deletes the current element and positions the cursor on the successor.
    /// After del(), `cursor.current()` returns the next entry in order,
    /// or null if the deleted entry was the last one.
    pub fn del(self: *Cursor) !void {
        if (!self.valid) return error.BadTxn;
        const entry = self.stack[self.depth];
        const state = &self.txn.dbi_state[self.dbi];

        // Build branch path from cursor stack (all levels above the leaf).
        var path_buf: [consts.MAX_DEPTH]PathEntry = undefined;
        {
            var d: u8 = 0;
            while (d < self.depth) : (d += 1) {
                path_buf[d] = .{ .pgno = self.stack[d].pgno, .ci = self.stack[d].index };
            }
        }
        // Touch all branch pages (path-copying CoW) and update cursor stack.
        try self.txn.touchPathDown(&state.tree, path_buf[0..self.depth]);
        {
            var d: u8 = 0;
            while (d < self.depth) : (d += 1) {
                self.stack[d].pgno = path_buf[d].pgno;
            }
        }

        // Make leaf writable (path-copy if committed).
        const leaf_wp = try self.txn.getWritablePage(entry.pgno);
        const page = leaf_wp.page;
        const leaf_pgno = leaf_wp.pgno;
        // Update parent pointer if leaf pgno changed.
        if (leaf_pgno != entry.pgno) {
            if (self.depth == 0) {
                state.tree.root = leaf_pgno;
            } else {
                const par = path_buf[self.depth - 1];
                const par_buf = self.txn.dirty_list.get(par.pgno).?;
                Transaction.updateBranchChild(@ptrCast(@alignCast(par_buf.ptr)), par.ci, leaf_pgno);
            }
            self.stack[self.depth].pgno = leaf_pgno;
        }

        try self.txn.freeOverflow(page.getNode(entry.index));
        page.delNode(entry.index);
        state.tree.items -= 1;
        state.tree.mod_txnid = self.txn.txnid;
        state.dirty = true;

        if (page.getUsedSpace() < REBALANCE_THRESHOLD) {
            // Save the successor key (now at entry.index after the shift, if any).
            var next_key_buf: [512]u8 = undefined;
            var next_key_len: usize = 0;
            if (entry.index < page.getNumEntries()) {
                const nk = page.getNode(entry.index).getKey();
                next_key_len = @min(nk.len, 512);
                @memcpy(next_key_buf[0..next_key_len], nk[0..next_key_len]);
            }
            try self.txn.rebalanceLeaf(&state.tree, page, leaf_pgno, path_buf[0..self.depth]);
            // Re-position on the successor after a potential page merge.
            if (state.tree.items == 0) {
                self.valid = false;
            } else if (next_key_len > 0) {
                // Exact seek; fall back to lower-bound if the key shifted due to merge.
                _ = try self.seekKey(next_key_buf[0..next_key_len], true);
                if (!self.valid) _ = try self.seekKey(next_key_buf[0..next_key_len], false);
                self.updateDupPrefixAfterMove();
            } else {
                self.valid = false; // was last entry in the tree
            }
            return;
        }

        const new_n = page.getNumEntries();
        if (new_n == 0) {
            self.valid = false;
            return;
        }
        if (entry.index < new_n) {
            // delNode shifted items left — entry.index now points to the successor. ✓
            self.updateDupPrefixAfterMove();
            return;
        }
        // Deleted the last item on this leaf page — advance to the next leaf.
        self.advanceToNextLeaf();
    }

    /// After the cursor moves to a new entry, checks whether it has left the
    /// current DupSort key range and resets key_prefix_len if so.
    fn updateDupPrefixAfterMove(self: *Cursor) void {
        if (!self.dupsort or self.key_prefix_len == 0 or !self.valid) return;
        const entry = self.stack[self.depth];
        const page = self.txn.getPage(entry.pgno);
        if (entry.index >= page.getNumEntries()) {
            self.key_prefix_len = 0;
            return;
        }
        const k = page.getNode(entry.index).getKey();
        if (!std.mem.startsWith(u8, k, self.key_prefix_buf[0..self.key_prefix_len]))
            self.key_prefix_len = 0;
    }

    /// Advances the cursor from the current (now-exhausted) leaf to the first
    /// entry of the next leaf page.  Mirrors the ascent logic in `next()`.
    fn advanceToNextLeaf(self: *Cursor) void {
        if (self.depth == 0) {
            self.valid = false;
            return;
        }
        var d = self.depth;
        while (d > 0) {
            d -= 1;
            const pe = &self.stack[d];
            const ppage = self.txn.getPage(pe.pgno);
            if (pe.index + 1 < ppage.getNumEntries()) {
                pe.index += 1;
                const child_pgno = ppage.getNode(pe.index).getChildPgno();
                self.valid = self.descendLeft(child_pgno, d + 1);
                if (self.valid) self.updateDupPrefixAfterMove();
                return;
            }
        }
        self.valid = false;
    }
};
