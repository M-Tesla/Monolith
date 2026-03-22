// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
const std = @import("std");
const os = @import("os/os.zig");
const types = @import("core/types.zig");

pub const ReaderSlot = extern struct {
    txnid: u64 align(8),
    pid: u32,
    tid: u32,
};

pub const ReaderTable = extern struct {
    magic: u64,
    num_slots: u32,
    padding: u32,
    slots: [126]ReaderSlot, // Fit in 4KB roughly? 126 * 16 = 2016. Plenty of space.
};

pub const LockManager = struct {
    file: std.fs.File,
    map: os.MmapRegion,
    table: *ReaderTable,
    allocator: std.mem.Allocator,
    /// When true all lock operations are no-ops (exclusive mode).
    /// In exclusive mode, reader-slot methods return early without touching self.table.
    exclusive: bool = false,

    /// Returns a no-op LockManager for exclusive mode.
    /// No .lck file is created; all lock/reader operations succeed immediately.
    pub fn initExclusive() LockManager {
        return LockManager{
            .file      = undefined,
            .map       = undefined,
            .table     = undefined, // never accessed in exclusive mode
            .allocator = undefined,
            .exclusive = true,
        };
    }

    pub fn init(path: []const u8, allocator: std.mem.Allocator) !LockManager {
        // Lock file is the db path with "-lck" appended (e.g. "data.monolith-lck").
        const key_path = try std.fmt.allocPrint(allocator, "{s}-lck", .{path});
        defer allocator.free(key_path);
        
        var file = try std.fs.cwd().createFile(key_path, .{
            .read = true,
            .truncate = false,
            .exclusive = false, // Open if exists
        });
        errdefer file.close();
        
        const size = try file.getEndPos();
        const min_size = 4096;

        if (size < min_size) {
            try file.setEndPos(min_size);
        }

        var region = try os.MmapRegion.init(file, min_size, false);
        errdefer region.deinit();

        const table = @as(*ReaderTable, @ptrCast(@alignCast(region.ptr)));

        if (table.magic == 0) {
            table.magic = 0xBEEFDEAD;
            table.num_slots = 126;
            // Clear slots
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
        self.map.deinit();
        self.file.close();
    }

    pub fn lockWriter(self: *LockManager) !void {
        if (self.exclusive) return;
        try os.lockFile(self.file, true, true);
    }

    /// Non-blocking write lock attempt. Returns error.Busy if the lock is already held.
    pub fn tryLockWriter(self: *LockManager) !void {
        if (self.exclusive) return;
        os.lockFile(self.file, true, false) catch return error.Busy;
    }

    pub fn unlockWriter(self: *LockManager) !void {
        if (self.exclusive) return;
        try os.unlockFile(self.file);
    }
    
    // Returns slot index
    pub fn registerReader(self: *LockManager, txnid: u64) !usize {
        if (self.exclusive) return 0; // exclusive mode: single process, no slot needed
        if (try self.tryClaimSlot(txnid)) |idx| return idx;
        self.recoverDeadSlots();
        if (try self.tryClaimSlot(txnid)) |idx| return idx;
        return error.ReaderSlotsFull;
    }
    
    fn tryClaimSlot(self: *LockManager, txnid: u64) !?usize {
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
             const slot = &self.table.slots[i];
             const current_txnid = @atomicLoad(u64, &slot.txnid, .acquire);
             if (current_txnid == 0) {
                 const res = @atomicRmw(u64, &slot.txnid, .Xchg, txnid, .acq_rel);
                 if (res == 0) {
                     slot.pid = os.getCurrentProcessId();
                     slot.tid = os.getCurrentThreadId();
                     return i;
                 }
             }
        }
        return null;
    }
    
    pub fn recoverDeadSlots(self: *LockManager) void {
        if (self.exclusive) return;
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
            const slot = &self.table.slots[i];
            const txnid = @atomicLoad(u64, &slot.txnid, .acquire);
            const pid = slot.pid;

            if (txnid != 0 and pid != 0) {
                if (!os.isProcessAlive(pid)) {
                    slot.pid = 0;
                    slot.tid = 0;
                    @atomicStore(u64, &slot.txnid, 0, .release);
                }
            }
        }
    }

    /// Clears slots belonging to dead processes and returns how many were cleared.
    pub fn recoverDeadSlotsCount(self: *LockManager) u32 {
        if (self.exclusive) return 0;
        var cleared: u32 = 0;
        var i: usize = 0;
        while (i < self.table.num_slots) : (i += 1) {
            const slot = &self.table.slots[i];
            const txnid = @atomicLoad(u64, &slot.txnid, .acquire);
            const pid = slot.pid;
            if (txnid != 0 and pid != 0 and !os.isProcessAlive(pid)) {
                slot.pid = 0;
                slot.tid = 0;
                @atomicStore(u64, &slot.txnid, 0, .release);
                cleared += 1;
            }
        }
        return cleared;
    }

    pub fn unregisterReader(self: *LockManager, slot_idx: usize) void {
        if (self.exclusive) return;
        if (slot_idx >= self.table.num_slots) return;
        const slot = &self.table.slots[slot_idx];
        @atomicStore(u64, &slot.txnid, 0, .release);
    }
    
    pub fn getOldestReader(self: *LockManager, limit_txnid: u64) u64 {
        if (self.exclusive) return limit_txnid; // no readers in exclusive mode
        var min_txnid = limit_txnid;
        var i: usize = 0;
        
        while (i < self.table.num_slots) : (i += 1) {
             const slot = &self.table.slots[i];
             const txnid = @atomicLoad(u64, &slot.txnid, .acquire);
             
             if (txnid != 0 and txnid < min_txnid) {
                 min_txnid = txnid;
             }
        }
        return min_txnid;
    }
};
