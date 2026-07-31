// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
const std = @import("std");
const os = @import("os/os.zig");
const types = @import("core/types.zig");

const NativeMutex = struct {
    locked: std.atomic.Value(bool) = .init(false),

    pub fn lock(self: *NativeMutex) void {
        while (self.locked.cmpxchgStrong(false, true, .acquire, .monotonic) != null) {
            while (self.locked.load(.monotonic)) {
                std.Thread.yield() catch {};
            }
        }
    }

    pub fn tryLock(self: *NativeMutex) bool {
        return self.locked.cmpxchgStrong(false, true, .acquire, .monotonic) == null;
    }

    pub fn unlock(self: *NativeMutex) void {
        self.locked.store(false, .release);
    }
};

pub const ReaderSlot = extern struct {
    txnid: u64 align(8),
    pid: u32,
    tid: u32,
};

/// Magic value in the .lck file header.  Change this whenever the on-disk
/// format of ReaderTable changes so old files are automatically re-initialised.
const READER_TABLE_MAGIC: u64 = 0xBEEFDEAD_00000002; // v2: 1024 slots

/// Max simultaneous readers.  1024 slots × 16 bytes = 16 KB header.
/// Supports 1024 concurrent read transactions per .monolith file.
pub const MAX_READERS: u32 = 1024;

pub const ReaderTable = extern struct {
    magic:     u64,
    num_slots: u32,
    padding:   u32,
    slots:     [MAX_READERS]ReaderSlot,
};

/// In-memory reader tracking for server mode.
/// Grows without limit — no MAX_READERS cap.
/// Write access serialised by write_mutex; reader list by readers_mutex.
pub const ServerReaders = struct {
    write_mutex:   NativeMutex            = .{},
    readers_mutex: NativeMutex            = .{},
    slots:         std.ArrayListUnmanaged(u64) = .empty, // txnid per entry; 0 = free
    alloc:         std.mem.Allocator,
};

pub const LockManager = struct {
    file: std.Io.File,
    map: os.MmapRegion,
    table: *ReaderTable,
    allocator: std.mem.Allocator,
    /// When true all lock operations are no-ops (exclusive mode).
    exclusive: bool = false,
    /// When non-null, server mode is active: file/map/table are unused.
    server: ?*ServerReaders = null,
    /// In-process write-lock tracking for non-server mode. flock() on the
    /// same fd always succeeds, so without this a second tryLockWriter()
    /// from the same process would not return error.Busy.
    writer_held: bool = false,

    /// Returns a no-op LockManager for exclusive mode.
    /// No .lck file is created; all lock/reader operations succeed immediately.
    pub fn initExclusive() LockManager {
        return LockManager{
            .file      = undefined,
            .map       = undefined,
            .table     = undefined,
            .allocator = undefined,
            .exclusive = true,
        };
    }

    /// Server mode: no .lck file, no MAX_READERS limit.
    /// Write lock is an in-process mutex; readers are tracked in a growable list.
    /// Use this when Monolith is opened by a single server process (e.g. TessDB).
    pub fn initServerMode(allocator: std.mem.Allocator) !LockManager {
        const srv = try allocator.create(ServerReaders);
        srv.* = .{ .alloc = allocator };
        return LockManager{
            .file      = undefined,
            .map       = undefined,
            .table     = undefined,
            .allocator = allocator,
            .server    = srv,
        };
    }

    pub fn init(path: []const u8, allocator: std.mem.Allocator) !LockManager {
        // Lock file is the db path with "-lck" appended (e.g. "data.monolith-lck").
        const key_path = try std.fmt.allocPrint(allocator, "{s}-lck", .{path});
        defer allocator.free(key_path);
        
        const io = std.Io.Threaded.global_single_threaded.io();
        var file = if (std.Io.Dir.path.isAbsolute(key_path))
            try std.Io.Dir.createFileAbsolute(io, key_path, .{
                .read = true, .truncate = false, .exclusive = false,
            })
        else
            try std.Io.Dir.cwd().createFile(io, key_path, .{
                .read = true, .truncate = false, .exclusive = false,
            });
        errdefer file.close(io);
        
        const size = try file.length(io);
        // ReaderTable with 1024 slots = 16 bytes header + 1024×16 bytes = 16400 bytes.
        // Round up to the next 4-KB page boundary: 20480 (5 pages).
        const min_size: u64 = 20480;

        if (size < min_size) {
            try file.setLength(io, min_size);
        }

        var region = try os.MmapRegion.init(file, min_size, false);
        errdefer region.deinit();

        const table = @as(*ReaderTable, @ptrCast(@alignCast(region.ptr)));

        // Re-initialise if the file is brand-new (magic == 0) OR if it was
        // written by an older version with a different magic / slot count.
        if (table.magic != READER_TABLE_MAGIC or table.num_slots != MAX_READERS) {
            table.magic     = READER_TABLE_MAGIC;
            table.num_slots = MAX_READERS;
            // Clear all slots so no stale readers survive a format upgrade.
            @memset(@as([*]u8, @ptrCast(&table.slots[0]))[0..@sizeOf(@TypeOf(table.slots))], 0);
        }
        
        return LockManager{
            .file = file,
            .map = region,
            .table = table,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *LockManager) void {
        if (self.exclusive) return;
        if (self.server) |srv| {
            srv.slots.deinit(srv.alloc);
            self.allocator.destroy(srv);
            return;
        }
        self.map.deinit();
        self.file.close(std.Io.Threaded.global_single_threaded.io());
    }

    pub fn lockWriter(self: *LockManager) !void {
        if (self.exclusive) return;
        if (self.server) |srv| { srv.write_mutex.lock(); return; }
        try os.lockFile(self.file, true, true);
        self.writer_held = true;
    }

    /// Non-blocking write lock attempt. Returns error.Busy if the lock is already held.
    pub fn tryLockWriter(self: *LockManager) !void {
        if (self.exclusive) return;
        if (self.server) |srv| {
            if (!srv.write_mutex.tryLock()) return error.Busy;
            return;
        }
        if (self.writer_held) return error.Busy;
        os.lockFile(self.file, true, false) catch return error.Busy;
        self.writer_held = true;
    }

    pub fn unlockWriter(self: *LockManager) !void {
        if (self.exclusive) return;
        if (self.server) |srv| { srv.write_mutex.unlock(); return; }
        self.writer_held = false;
        try os.unlockFile(self.file);
    }
    
    /// Register a read transaction. Returns a slot index used later by unregisterReader.
    /// In server mode the list grows without bound — no MAX_READERS limit.
    pub fn registerReader(self: *LockManager, txnid: u64) !usize {
        if (self.exclusive) return 0;
        if (self.server) |srv| {
            srv.readers_mutex.lock();
            defer srv.readers_mutex.unlock();
            // Reuse a free (zero) slot before appending a new one.
            for (srv.slots.items, 0..) |v, i| {
                if (v == 0) { srv.slots.items[i] = txnid; return i; }
            }
            const idx = srv.slots.items.len;
            try srv.slots.append(srv.alloc, txnid);
            return idx;
        }
        if (try self.tryClaimSlot(txnid)) |idx| return idx;
        self.recoverDeadSlots();
        if (try self.tryClaimSlot(txnid)) |idx| return idx;
        return error.ReaderSlotsFull;
    }

    pub fn unregisterReader(self: *LockManager, slot_idx: usize) void {
        if (self.exclusive) return;
        if (self.server) |srv| {
            srv.readers_mutex.lock();
            defer srv.readers_mutex.unlock();
            if (slot_idx < srv.slots.items.len) srv.slots.items[slot_idx] = 0;
            return;
        }
        if (slot_idx >= self.table.num_slots) return;
        const slot = &self.table.slots[slot_idx];
        @atomicStore(u32, &slot.pid, 0, .release);
        @atomicStore(u32, &slot.tid, 0, .release);
        @atomicStore(u64, &slot.txnid, 0, .release);
    }

    pub fn getOldestReader(self: *LockManager, limit_txnid: u64) u64 {
        if (self.exclusive) return limit_txnid;
        if (self.server) |srv| {
            srv.readers_mutex.lock();
            defer srv.readers_mutex.unlock();
            var min = limit_txnid;
            for (srv.slots.items) |v| {
                if (v != 0 and v < min) min = v;
            }
            return min;
        }
        var min_txnid = limit_txnid;
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
            const slot = &self.table.slots[i];
            const txnid = @atomicLoad(u64, &slot.txnid, .acquire);
            if (txnid != 0 and txnid < min_txnid) min_txnid = txnid;
        }
        return min_txnid;
    }

    fn tryClaimSlot(self: *LockManager, txnid: u64) !?usize {
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
            const slot = &self.table.slots[i];
            const current = @atomicLoad(u64, &slot.txnid, .acquire);
            if (current == 0) {
                const res = @atomicRmw(u64, &slot.txnid, .Xchg, txnid, .acq_rel);
                if (res == 0) {
                    @atomicStore(u32, &slot.pid, os.getCurrentProcessId(), .release);
                    @atomicStore(u32, &slot.tid, os.getCurrentThreadId(), .release);
                    return i;
                }
            }
        }
        return null;
    }

    pub fn recoverDeadSlots(self: *LockManager) void {
        if (self.exclusive or self.server != null) return;
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
            const slot = &self.table.slots[i];
            const txnid = @atomicLoad(u64, &slot.txnid, .acquire);
            const pid = @atomicLoad(u32, &slot.pid, .acquire);
            if (txnid != 0 and pid != 0 and !os.isProcessAlive(pid)) {
                @atomicStore(u32, &slot.pid, 0, .release);
                @atomicStore(u32, &slot.tid, 0, .release);
                @atomicStore(u64, &slot.txnid, 0, .release);
            }
        }
    }

    pub fn recoverDeadSlotsCount(self: *LockManager) u32 {
        if (self.exclusive or self.server != null) return 0;
        var cleared: u32 = 0;
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
            const slot = &self.table.slots[i];
            const txnid = @atomicLoad(u64, &slot.txnid, .acquire);
            const pid = @atomicLoad(u32, &slot.pid, .acquire);
            if (txnid != 0 and pid != 0 and !os.isProcessAlive(pid)) {
                @atomicStore(u32, &slot.pid, 0, .release);
                @atomicStore(u32, &slot.tid, 0, .release);
                @atomicStore(u64, &slot.txnid, 0, .release);
                cleared += 1;
            }
        }
        return cleared;
    }
};
