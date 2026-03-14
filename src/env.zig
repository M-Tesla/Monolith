// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Environment — pure Zig, no C.
//! Represents an open .monolith database file.
//! Owns the mmap region, the ping-pong meta pages, and the LockManager.

const std      = @import("std");
const os_mod   = @import("os/os.zig");
const lock_mod = @import("lock.zig");
const meta_mod = @import("meta/meta.zig");
const page_mod = @import("page/page.zig");
const core_types = @import("core/types.zig");
const consts   = @import("core/consts.zig");
const types    = @import("types.zig");

pub const PAGE_SIZE: usize = consts.DATAPAGESIZE;

/// Initial page layout: meta0(0), meta1(1), main_root(2), gc_root(3).
const INIT_PAGES: usize = 4;

pub const Environment = struct {
    file:          std.fs.File,
    map:           os_mod.MmapRegion,
    lock:          lock_mod.LockManager,
    allocator:     std.mem.Allocator,
    max_dbs:       u32,
    rdonly:        bool,
    /// Index of the meta slot with the most recent committed state (0 or 1).
    best_meta_idx: u8,
    /// Optional callback invoked before growing the mmap.
    /// Receives the environment and the minimum new size needed.
    /// Returns the actual new size to use, or an error to abort the commit.
    /// When null the minimum required size is used directly.
    map_full_fn: ?*const fn (*Environment, usize) anyerror!usize,
    /// Arbitrary user pointer attached to the environment.
    user_ctx: ?*anyopaque,
    /// True when opened with the writemap flag: dirty pages are written
    /// directly into the mmap instead of heap-allocated buffers.
    writemap: bool,
    /// Skip map.sync() on commit (safe_nosync or nosync mode).
    skip_sync: bool,
    /// GC reclaims newest-freed pages first (LIFO) instead of oldest (FIFO).
    liforeclaim: bool,
    /// Exclusive mode: no lock file created; single-process/single-thread only.
    exclusive: bool,
    /// Merge reclaimable GC entries into the current txn's entry (default ON).
    coalesce: bool,
    // ─── Geometry ───────────────────────────────────────────────────────────
    /// Maximum map size in bytes (0 = unlimited).
    size_upper: usize,
    /// Grow in multiples of this many bytes (0 = minimum needed).
    growth_step: usize,
    /// Shrink file when free space exceeds this many bytes (0 = never).
    shrink_threshold: usize,
    // ─── DBI registry ────────────────────────────────────────────────────────
    /// Env-level name→slot mapping so DBI handles are stable across transactions.
    /// Keys are heap-allocated; freed in close().
    dbi_registry: std.StringHashMapUnmanaged(u32),
    /// Slots freed by closeDBI() — recycled before allocating dbi_next.
    dbi_free_slots: std.ArrayListUnmanaged(u32),
    /// Next DBI slot number to assign when free_slots is empty (starts at 1).
    dbi_next: u32,

    // ─── Lifecycle ───────────────────────────────────────────────────────────

    /// Opens (or creates) a .monolith database file.
    pub fn open(
        path:     [:0]const u8,
        flags:    types.EnvFlags,
        max_dbs:  u32,
        map_size: usize,
    ) !Environment {
        const rdonly     = flags.rdonly;
        const path_slice = path[0..path.len];

        const file: std.fs.File = blk: {
            if (rdonly) {
                break :blk try std.fs.cwd().openFile(path_slice, .{ .mode = .read_only });
            } else {
                break :blk try std.fs.cwd().createFile(path_slice, .{
                    .read      = true,
                    .truncate  = false,
                    .exclusive = false,
                });
            }
        };
        errdefer file.close();

        const file_size = try file.getEndPos();
        const is_new    = file_size < PAGE_SIZE * INIT_PAGES;

        if (!rdonly and is_new) {
            try file.setEndPos(PAGE_SIZE * INIT_PAGES);
        }

        const mmap_size = if (is_new) PAGE_SIZE * INIT_PAGES else file_size;

        var map = try os_mod.MmapRegion.init(file, mmap_size, rdonly);
        errdefer map.deinit();

        var lock = if (flags.exclusive)
            lock_mod.LockManager.initExclusive()
        else
            try lock_mod.LockManager.init(path_slice, std.heap.page_allocator);
        errdefer lock.deinit();

        var env = Environment{
            .file          = file,
            .map           = map,
            .lock          = lock,
            .allocator     = std.heap.page_allocator,
            .max_dbs       = max_dbs,
            .rdonly        = rdonly,
            .best_meta_idx = 0,
            .map_full_fn   = null,
            .user_ctx      = null,
            .writemap      = flags.writemap,
            .skip_sync     = flags.safe_nosync or flags.nosync,
            .liforeclaim   = flags.liforeclaim,
            .exclusive     = flags.exclusive,
            .coalesce      = flags.coalesce,
            .size_upper    = 0,
            .growth_step   = 0,
            .shrink_threshold = 0,
            .dbi_registry  = .{},
            .dbi_free_slots = .{},
            .dbi_next      = 1,
        };
        // Pre-allocate free-slot list so closeDBI/openDbi appends never OOM.
        try env.dbi_free_slots.ensureTotalCapacity(env.allocator, max_dbs);

        if (is_new) {
            try env.initNewDB(map_size);
        } else {
            try env.openExisting();
        }

        return env;
    }

    /// Flushes and closes the environment, releasing all resources.
    pub fn close(self: *Environment) void {
        if (!self.rdonly) self.map.sync() catch {}; // best-effort flush on close
        self.map.deinit();
        self.lock.deinit();
        self.file.close();
        // Free DBI registry keys.
        var it = self.dbi_registry.keyIterator();
        while (it.next()) |key| self.allocator.free(key.*);
        self.dbi_registry.deinit(self.allocator);
        self.dbi_free_slots.deinit(self.allocator);
    }

    // ─── Meta access ─────────────────────────────────────────────────────────

    /// Pointer to the Meta struct in page slot `idx` (0 or 1).
    /// Meta starts at offset 20, immediately after the 20-byte PageHeader.
    pub fn getMetaAt(self: *const Environment, idx: u8) *align(4) meta_mod.Meta {
        const offset = @as(usize, idx) * PAGE_SIZE + 20;
        return @as(*align(4) meta_mod.Meta, @ptrCast(@alignCast(self.map.ptr + offset)));
    }

    /// Pointer to the most-recently-committed Meta slot.
    pub fn bestMeta(self: *const Environment) *align(4) meta_mod.Meta {
        return self.getMetaAt(self.best_meta_idx);
    }

    // ─── Page access ─────────────────────────────────────────────────────────

    /// Raw pointer to the PageHeader of page `pgno` in the mmap.
    /// Callers must ensure pgno is within the committed range (< first_unallocated).
    pub fn getPagePtr(self: *const Environment, pgno: u32) *page_mod.PageHeader {
        const offset = @as(usize, pgno) * PAGE_SIZE;
        std.debug.assert(offset + PAGE_SIZE <= self.map.len); // catches out-of-range pgno in debug
        return @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.map.ptr + offset)));
    }

    /// Bounds-checked variant. Returns error.InvalidPage if pgno is out of range.
    pub fn getPagePtrSafe(self: *const Environment, pgno: u32) !*page_mod.PageHeader {
        const offset = @as(usize, pgno) * PAGE_SIZE;
        if (offset + PAGE_SIZE > self.map.len) return error.InvalidPage;
        return @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.map.ptr + offset)));
    }

    // ─── Resize ──────────────────────────────────────────────────────────────

    /// Extends the backing file and remaps the mmap region.
    /// Respects growth_step (snaps up to next multiple) and size_upper (hard cap).
    /// WARNING: invalidates all pointers previously obtained from the mmap.
    pub fn resize(self: *Environment, new_size: usize) !void {
        var actual = new_size;
        // Snap up to the next multiple of growth_step.
        if (self.growth_step > 0) {
            const rem = actual % self.growth_step;
            if (rem != 0) actual += self.growth_step - rem;
        }
        // Enforce the upper size limit.
        if (self.size_upper > 0) {
            if (new_size > self.size_upper) return error.MapFull;
            if (actual > self.size_upper) actual = self.size_upper;
        }
        self.map.deinit();
        try self.file.setEndPos(actual);
        self.map = try os_mod.MmapRegion.init(self.file, actual, self.rdonly);
    }

    // ─── Internal ────────────────────────────────────────────────────────────

    fn initNewDB(self: *Environment, map_size: usize) !void {
        const total = PAGE_SIZE * INIT_PAGES;
        @memset(self.map.ptr[0..total], 0);

        const geo_upper: u32 = @truncate(map_size / PAGE_SIZE);

        // Page 0 — Meta slot A
        {
            const ph = @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.map.ptr)));
            ph.flags        = page_mod.P_META;
            ph.pgno         = 0;
            ph.txnid        = 0;
            ph.dupfix_ksize = 0;
            ph.setLowerUpper(20, @as(u16, @truncate(PAGE_SIZE)));

            const m = self.getMetaAt(0);
            m.magic_and_version = consts.MAGIC;
            m.txnid_a        = 1;
            m.txnid_b        = 1;
            m.reserve16      = 0;
            m.validator_id   = 0;
            m.extra_pagehdr  = 0;
            m.geometry = .{
                .grow_pv           = 0,
                .shrink_pv         = 0,
                .lower             = INIT_PAGES,
                .upper             = geo_upper,
                .current           = INIT_PAGES,
                .first_unallocated = INIT_PAGES,
            };
            m.trees = .{
                .main = .{
                    .flags        = 0,
                    .height       = 1,
                    .dupfix_size  = 0,
                    .root         = 2,
                    .branch_pages = 0,
                    .leaf_pages   = 1,
                    .large_pages  = 0,
                    .sequence     = 0,
                    .items        = 0,
                    .mod_txnid    = 1,
                },
                .gc = .{
                    .flags        = 0,
                    .height       = 1,
                    .dupfix_size  = 0,
                    .root         = 3,
                    .branch_pages = 0,
                    .leaf_pages   = 1,
                    .large_pages  = 0,
                    .sequence     = 0,
                    .items        = 0,
                    .mod_txnid    = 1,
                },
            };
            m.canary        = .{ .x = 0, .y = 0, .z = 0, .v = 0 };
            m.sign          = 0;
            m.pages_retired = .{ 0, 0 };
            m.bootid        = .{ .lo = 0, .hi = 0 };
            m.dxbid         = .{ .lo = 0, .hi = 0 };
        }

        // Page 1 — Meta slot B (exact copy of A)
        {
            const ph = @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.map.ptr + PAGE_SIZE)));
            ph.flags        = page_mod.P_META;
            ph.pgno         = 1;
            ph.txnid        = 0;
            ph.dupfix_ksize = 0;
            ph.setLowerUpper(20, @as(u16, @truncate(PAGE_SIZE)));

            const m1 = self.getMetaAt(1);
            m1.* = self.getMetaAt(0).*;
        }

        // Page 2 — main tree root (empty leaf)
        {
            const p = @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.map.ptr + PAGE_SIZE * 2)));
            p.init(@as(u32, @truncate(PAGE_SIZE)), page_mod.P_LEAF);
            p.pgno  = 2;
            p.txnid = 0;
            p.dupfix_ksize = page_mod.computePageChecksum(p, PAGE_SIZE);
        }

        // Page 3 — GC tree root (empty leaf)
        {
            const p = @as(*page_mod.PageHeader, @ptrCast(@alignCast(self.map.ptr + PAGE_SIZE * 3)));
            p.init(@as(u32, @truncate(PAGE_SIZE)), page_mod.P_LEAF);
            p.pgno  = 3;
            p.txnid = 0;
            p.dupfix_ksize = page_mod.computePageChecksum(p, PAGE_SIZE);
        }

        try self.map.sync();
        self.best_meta_idx = 0;
    }

    // ─── MmapFull callback ───────────────────────────────────────────────────

    /// Registers a callback that is invoked when the mmap needs to grow.
    /// The callback may return a size larger than `needed` to over-allocate,
    /// or return an error to abort the in-progress commit.
    pub fn setMapFullHandler(
        self:   *Environment,
        fn_ptr: *const fn (*Environment, usize) anyerror!usize,
    ) void {
        self.map_full_fn = fn_ptr;
    }

    // ─── Auto-shrink ─────────────────────────────────────────────────────────

    /// Truncates trailing over-allocated space when it exceeds shrink_threshold.
    /// Called after each commit. Best-effort: silently skips on remap failure.
    ///
    /// Note: growth_step controls GROW granularity; for shrinking we use
    /// live_size directly so that a large growth_step does not prevent truncation.
    pub fn tryAutoShrink(self: *Environment, live_pages: u32) void {
        if (self.shrink_threshold == 0) return;
        const min_size = PAGE_SIZE * INIT_PAGES; // always keep at least 4 pages
        const live_size = @max(@as(usize, live_pages) * PAGE_SIZE, min_size);
        if (self.map.len <= live_size) return;
        if (self.map.len - live_size <= self.shrink_threshold) return;
        const new_size = live_size;
        if (new_size >= self.map.len) return;
        const old_len = self.map.len;
        if (comptime @import("builtin").os.tag == .windows) {
            // Windows: cannot truncate a file while a view is mapped.
            // Must unmap first, then truncate, then remap.
            self.map.deinit();
            self.file.setEndPos(new_size) catch {
                // Truncate failed — remap at original size.
                self.map = os_mod.MmapRegion.init(self.file, old_len, self.rdonly) catch return;
                return;
            };
            self.map = os_mod.MmapRegion.init(self.file, new_size, self.rdonly) catch {
                // Remap failed — restore file size, remap at old size (best effort).
                self.file.setEndPos(old_len) catch {};
                self.map = os_mod.MmapRegion.init(self.file, old_len, self.rdonly) catch return;
                return;
            };
        } else {
            // POSIX: can truncate while the old mmap is alive.
            // Try-then-swap: keep old map valid until new one is ready.
            self.file.setEndPos(new_size) catch return; // truncate failed — keep old map
            const new_map = os_mod.MmapRegion.init(self.file, new_size, self.rdonly) catch {
                // Remap failed. Restore file size so the old map remains consistent.
                self.file.setEndPos(old_len) catch {};
                return;
            };
            // Success: release the old mapping.
            self.map.deinit();
            self.map = new_map;
        }
    }

    // ─── Sync ────────────────────────────────────────────────────────────────

    /// Explicitly flush all pending writes to disk.
    /// Useful when operating in safe_nosync or nosync mode to durably
    /// persist a batch of committed transactions at a chosen point.
    pub fn sync(self: *Environment) void {
        self.map.sync() catch {}; // public API stays void; errors are best-effort
    }

    // ─── Geometry ────────────────────────────────────────────────────────────

    /// Configure mmap growth behaviour. Fields set to 0 are unchanged.
    pub fn setGeometry(self: *Environment, geo: types.Geometry) void {
        if (geo.size_upper > 0)       self.size_upper       = geo.size_upper;
        if (geo.growth_step > 0)      self.growth_step      = geo.growth_step;
        if (geo.shrink_threshold > 0) self.shrink_threshold = geo.shrink_threshold;
    }

    // ─── DBI lifecycle ───────────────────────────────────────────────────────

    /// Releases the DBI handle, freeing its slot for reuse.
    /// Any transaction that already holds this handle remains valid until it ends.
    /// Passing the stale handle to a NEW transaction returns error.BadDbi.
    pub fn closeDBI(self: *Environment, dbi: types.Dbi) void {
        var it = self.dbi_registry.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* == dbi) {
                const key = entry.key_ptr.*;
                _ = self.dbi_registry.remove(key);
                self.allocator.free(key);
                self.dbi_free_slots.append(self.allocator, dbi) catch {};
                return;
            }
        }
    }

    // ─── Hot Backup ───────────────────────────────────────────────────────────

    /// Copies the database to `dest_path` (hot backup, O(1) snapshot).
    // ─── User context ───────────────────────────────────────────────────────────

    pub fn setUserCtx(self: *Environment, ptr: ?*anyopaque) void {
        self.user_ctx = ptr;
    }

    pub fn getUserCtx(self: *const Environment) ?*anyopaque {
        return self.user_ctx;
    }

    // ─── Backup ───────────────────────────────────────────────────────────────

    /// Briefly holds the writer lock to obtain a consistent page count, then
    /// streams exactly `first_unallocated` pages to the destination file.
    /// Active readers are never blocked.
    pub fn copy(self: *Environment, dest_path: [:0]const u8) !void {
        // Snapshot the page count under the writer lock for consistency.
        try self.lock.lockWriter();
        const num_pages = self.bestMeta().geometry.first_unallocated;
        try self.lock.unlockWriter();

        const dest = try std.fs.cwd().createFile(dest_path[0..dest_path.len], .{ .truncate = true });
        defer dest.close();

        const copy_len = @as(usize, num_pages) * PAGE_SIZE;
        try dest.writeAll(self.map.ptr[0..copy_len]);
    }

    // ─── Deep Audit ──────────────────────────────────────────────────────────

    /// Result of a structural integrity check of the database file.
    pub const CheckResult = struct {
        /// Number of data pages examined (excludes meta pages 0 and 1)
        pages_visited: u64,
        /// Number of structural errors detected
        errors:        u64,
    };

    /// Performs a structural integrity check on all allocated pages.
    /// Validates that each page's stored pgno matches its physical offset
    /// and that its flags contain only recognised bit combinations.
    /// Returns a CheckResult with counts; does not modify any data.
    pub fn check(self: *const Environment) CheckResult {
        const meta        = self.bestMeta();
        const total: u32  = meta.geometry.first_unallocated;
        var result = CheckResult{ .pages_visited = 0, .errors = 0 };

        // Pages 0 and 1 are meta pages — start audit from page 2.
        var i: u32 = 2;
        while (i < total) {
            result.pages_visited += 1;
            const pg = self.getPagePtr(i);

            // The pgno field must match the page's physical position.
            if (pg.pgno != i) result.errors += 1;

            // Validate checksum for pages that carry one (non-LEAF2, non-META).
            const skip_cksum = (pg.flags & page_mod.P_LEAF2) != 0 or
                               (pg.flags & page_mod.P_META)  != 0;
            if (!skip_cksum) {
                const expected = page_mod.computePageChecksum(pg, consts.DATAPAGESIZE);
                if (pg.dupfix_ksize != expected) result.errors += 1;
            }

            // Flags must consist only of recognised page-type bits.
            const known: u16 = page_mod.P_BRANCH | page_mod.P_LEAF |
                               page_mod.P_OVERFLOW | page_mod.P_META |
                               page_mod.P_DIRTY | page_mod.P_LEAF2 |
                               page_mod.P_SUBP | page_mod.P_LOOSE | page_mod.P_KEEP;
            if (pg.flags & ~known != 0) result.errors += 1;

            // A page must have exactly one primary type flag set.
            const type_bits: u16 = pg.flags & (page_mod.P_BRANCH | page_mod.P_LEAF |
                                               page_mod.P_OVERFLOW | page_mod.P_META |
                                               page_mod.P_LEAF2);
            const is_power_of_two = type_bits != 0 and (type_bits & (type_bits - 1)) == 0;
            if (!is_power_of_two) result.errors += 1;

            if ((pg.flags & page_mod.P_OVERFLOW) != 0) {
                // For the first page of an overflow run: validate the page count
                // and skip subsequent pages of the same run in one step.
                const n_pages = pg.data_union; // stored in first overflow page
                if (n_pages == 0 or @as(u64, i) + n_pages > total) {
                    result.errors += 1;
                    i += 1;
                } else {
                    // Validate all pages in the run share the P_OVERFLOW flag.
                    var j: u32 = 1;
                    while (j < n_pages) : (j += 1) {
                        const opg = self.getPagePtr(i + j);
                        result.pages_visited += 1;
                        if (opg.pgno != i + j) result.errors += 1;
                        if ((opg.flags & page_mod.P_OVERFLOW) == 0) result.errors += 1;
                    }
                    i += n_pages;
                }
            } else {
                i += 1;
            }
        }

        return result;
    }

    fn openExisting(self: *Environment) !void {
        const m0 = self.getMetaAt(0);
        const m1 = self.getMetaAt(1);

        const v0 = m0.validate();
        const v1 = m1.validate();

        if (!v0 and !v1) return error.Invalid;

        if (v0 and v1) {
            const ok0 = m0.txnid_a == m0.txnid_b;
            const ok1 = m1.txnid_a == m1.txnid_b;
            if (ok0 and ok1) {
                self.best_meta_idx = if (m0.txnid_a >= m1.txnid_a) 0 else 1;
            } else if (ok0) {
                self.best_meta_idx = 0;
            } else if (ok1) {
                self.best_meta_idx = 1;
            } else {
                return error.Corrupted;
            }
        } else {
            self.best_meta_idx = if (v0) 0 else 1;
        }
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────
test "Environment: open/close new database" {
    const path = "test_env_open.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
    defer env.close();

    const m = env.bestMeta();
    try std.testing.expect(m.validate());
    try std.testing.expectEqual(@as(u64, 1), m.txnid_a);
    try std.testing.expectEqual(@as(u32, 2), m.trees.main.root);
    try std.testing.expectEqual(@as(u32, 3), m.trees.gc.root);
    try std.testing.expectEqual(@as(u32, INIT_PAGES), m.geometry.first_unallocated);
}

test "Environment: reopen preserves meta" {
    const path = "test_env_reopen.monolith";
    defer std.fs.cwd().deleteFile(path) catch {};
    defer std.fs.cwd().deleteFile(path ++ "-lck") catch {};

    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        env.close();
    }
    {
        var env = try Environment.open(path, .{}, 4, 4 * 1024 * 1024);
        defer env.close();
        const m = env.bestMeta();
        try std.testing.expect(m.validate());
        try std.testing.expectEqual(@as(u64, 1), m.txnid_a);
    }
}
