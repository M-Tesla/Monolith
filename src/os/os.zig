// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! OS abstraction layer: memory-mapped files and process management.
//! Supports Windows (Win32) and POSIX (Linux, macOS).

const std     = @import("std");
const builtin = @import("builtin");
const consts  = @import("../core/consts.zig");

const is_windows = builtin.os.tag == .windows;
const WINAPI: std.builtin.CallingConvention =
    if (builtin.cpu.arch == .x86) .stdcall else .c;

pub const FileHandle = std.Io.File;

pub const MmapError = error{
    MappingFailed,
    UnmappingFailed,
    ResizeFailed,
    LockFailed,
    UnlockFailed,
};

// ── File locking ─────────────────────────────────────────────────────────────

pub fn lockFile(file: FileHandle, exclusive: bool, wait: bool) !void {
    if (comptime is_windows) {
        var overlapped = std.mem.zeroes(MmapRegion.OVERLAPPED);
        var flags: u32 = 0;
        if (exclusive) flags |= MmapRegion.LOCKFILE_EXCLUSIVE_LOCK;
        if (!wait)     flags |= MmapRegion.LOCKFILE_FAIL_IMMEDIATELY;
        const len: u64 = 1;
        const rc = MmapRegion.LockFileEx(
            file.handle, flags, 0,
            @as(u32, @truncate(len)),
            @as(u32, @truncate(len >> 32)),
            &overlapped,
        );
        if (rc == 0) return MmapError.LockFailed;
    } else {
        const posix = std.posix;
        const c = struct {
            extern "c" fn flock(fd: c_int, operation: c_int) c_int;
        };
        var how: u32 = if (exclusive) posix.LOCK.EX else posix.LOCK.SH;
        if (!wait) how |= posix.LOCK.NB;
        const rc = c.flock(file.handle, @intCast(how));
        if (rc != 0) return MmapError.LockFailed;
    }
}

pub fn unlockFile(file: FileHandle) !void {
    if (comptime is_windows) {
        var overlapped = std.mem.zeroes(MmapRegion.OVERLAPPED);
        const len: u64 = 1;
        const rc = MmapRegion.UnlockFileEx(
            file.handle, 0,
            @as(u32, @truncate(len)),
            @as(u32, @truncate(len >> 32)),
            &overlapped,
        );
        if (rc == 0) return MmapError.UnlockFailed;
    } else {
        const c = struct {
            extern "c" fn flock(fd: c_int, operation: c_int) c_int;
        };
        const rc = c.flock(file.handle, 8); // LOCK_UN
        if (rc != 0) return MmapError.UnlockFailed;
    }
}

// ── Memory-mapped region ──────────────────────────────────────────────────────

pub const MmapRegion = struct {
    ptr:    [*]align(4096) u8,
    len:    usize,
    handle: if (is_windows) ?std.os.windows.HANDLE else void,
    file_handle: if (is_windows) std.os.windows.HANDLE else void,

    // ── Windows-only externs ─────────────────────────────────────────────────
    const windows = std.os.windows;

    const PAGE_READONLY:      u32 = 0x02;
    const PAGE_READWRITE:     u32 = 0x04;
    const FILE_MAP_READ:      u32 = 0x04;
    const FILE_MAP_ALL_ACCESS: u32 = 0xF001F;

    pub const OVERLAPPED = extern struct {
        Internal:     usize,
        InternalHigh: usize,
        Offset:       u32,
        OffsetHigh:   u32,
        hEvent:       ?windows.HANDLE,
    };

    extern "kernel32" fn CreateFileMappingW(
        hFile:                  windows.HANDLE,
        lpFileMappingAttributes: ?*anyopaque,
        flProtect:              u32,
        dwMaximumSizeHigh:      u32,
        dwMaximumSizeLow:       u32,
        lpName:                 ?[*:0]const u16,
    ) callconv(WINAPI) ?windows.HANDLE;

    extern "kernel32" fn MapViewOfFile(
        hFileMappingObject: windows.HANDLE,
        dwDesiredAccess:    u32,
        dwFileOffsetHigh:   u32,
        dwFileOffsetLow:    u32,
        dwNumberOfBytesToMap: usize,
    ) callconv(WINAPI) ?*anyopaque;

    extern "kernel32" fn UnmapViewOfFile(
        lpBaseAddress: ?*anyopaque,
    ) callconv(WINAPI) i32;

    extern "kernel32" fn FlushViewOfFile(
        lpBaseAddress:           ?*const anyopaque,
        dwNumberOfBytesToFlush:  usize,
    ) callconv(WINAPI) i32;

    extern "kernel32" fn FlushFileBuffers(
        hFile: std.os.windows.HANDLE,
    ) callconv(WINAPI) i32;

    extern "kernel32" fn LockFileEx(
        hFile:                    windows.HANDLE,
        dwFlags:                  u32,
        dwReserved:               u32,
        nNumberOfBytesToLockLow:  u32,
        nNumberOfBytesToLockHigh: u32,
        lpOverlapped:             ?*OVERLAPPED,
    ) callconv(WINAPI) i32;

    extern "kernel32" fn UnlockFileEx(
        hFile:                      windows.HANDLE,
        dwReserved:                 u32,
        nNumberOfBytesToUnlockLow:  u32,
        nNumberOfBytesToUnlockHigh: u32,
        lpOverlapped:               ?*OVERLAPPED,
    ) callconv(WINAPI) i32;

    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x00000001;
    const LOCKFILE_EXCLUSIVE_LOCK:   u32 = 0x00000002;

    // ── init / deinit / sync ─────────────────────────────────────────────────

    pub fn init(file: FileHandle, size: usize, read_only: bool) !MmapRegion {
        if (comptime is_windows) {
            const protect = if (read_only) PAGE_READONLY else PAGE_READWRITE;
            const access  = if (read_only) FILE_MAP_READ  else FILE_MAP_ALL_ACCESS;
            const size_low:  u32 = @truncate(size);
            const size_high: u32 = @truncate(size >> 32);

            const mapping = CreateFileMappingW(
                file.handle, null, protect, size_high, size_low, null,
            ) orelse return MmapError.MappingFailed;

            const ptr = MapViewOfFile(mapping, access, 0, 0, size)
                orelse {
                    windows.CloseHandle(mapping);
                    return MmapError.MappingFailed;
                };

            return .{
                .ptr    = @alignCast(@ptrCast(ptr)),
                .len    = size,
                .handle = mapping,
                .file_handle = file.handle,
            };
        } else {
            const posix = std.posix;
            const prot: posix.PROT = if (read_only)
                .{ .READ = true }
            else
                .{ .READ = true, .WRITE = true };
            const flags = posix.MAP{ .TYPE = .SHARED };
            const result = posix.mmap(null, size, prot, flags, file.handle, 0)
                catch return MmapError.MappingFailed;
            return .{
                .ptr    = result.ptr,
                .len    = size,
                .handle = {},
                .file_handle = {},
            };
        }
    }

    pub fn deinit(self: *MmapRegion) void {
        if (comptime is_windows) {
            _ = UnmapViewOfFile(self.ptr);
            if (self.handle) |h| windows.CloseHandle(h);
        } else {
            std.posix.munmap(@alignCast(self.ptr[0..self.len]));
        }
    }

    pub fn sync(self: *MmapRegion) !void {
        if (comptime is_windows) {
            if (FlushViewOfFile(self.ptr, self.len) == 0) return error.SyncFailed;
            if (FlushFileBuffers(self.file_handle) == 0) return error.SyncFailed;
        } else {
            try std.posix.msync(@alignCast(self.ptr[0..self.len]), std.posix.MSF.SYNC);
        }
    }
};

// ── Process / thread identity ─────────────────────────────────────────────────

pub fn getCurrentProcessId() u32 {
    if (comptime is_windows) {
        return GetCurrentProcessId();
    } else {
        const c = struct {
            extern "c" fn getpid() c_int;
        };
        return @intCast(c.getpid());
    }
}

pub fn getCurrentThreadId() u32 {
    if (comptime is_windows) {
        return GetCurrentThreadId();
    } else {
        return @truncate(std.Thread.getCurrentId());
    }
}

/// Returns true if the process with the given PID is still alive.
/// On Windows, ACCESS_DENIED means the process exists but we lack rights: treated as alive.
/// On POSIX, kill(pid, 0) probes existence without sending a signal.
pub fn isProcessAlive(pid: u32) bool {
    if (pid == 0) return false;
    if (comptime is_windows) {
        const handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid)
            orelse {
                const err = std.os.windows.GetLastError();
                return @intFromEnum(err) == 5; // ERROR_ACCESS_DENIED: process exists
            };
        defer std.os.windows.CloseHandle(handle);
        var exit_code: u32 = 0;
        const rc = GetExitCodeProcess(handle, &exit_code);
        if (rc != 0) return exit_code == STILL_ACTIVE;
        return false;
    } else {
        // Out-of-range PIDs cannot exist; avoid a checked-cast panic on pid_t.
        if (pid > std.math.maxInt(std.posix.pid_t)) return false;
        std.posix.kill(@intCast(pid), @enumFromInt(0)) catch |err| {
            return err == error.PermissionDenied; // process exists, no signal permission
        };
        return true;
    }
}

// ── Windows-only process externs ──────────────────────────────────────────────

const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
const STILL_ACTIVE: u32 = 259;

extern "kernel32" fn OpenProcess(
    dwDesiredAccess: u32,
    bInheritHandle:  i32,
    dwProcessId:     u32,
) callconv(WINAPI) ?std.os.windows.HANDLE;

extern "kernel32" fn GetExitCodeProcess(
    hProcess:    std.os.windows.HANDLE,
    lpExitCode:  *u32,
) callconv(WINAPI) i32;

extern "kernel32" fn GetCurrentProcessId() callconv(WINAPI) u32;
extern "kernel32" fn GetCurrentThreadId()  callconv(WINAPI) u32;

// ── Tests ─────────────────────────────────────────────────────────────────────

test "Mmap Basic" {
    const tmp_path = "test_mmap.dat";
    const io = std.Io.Threaded.global_single_threaded.io();
    var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .read = true, .truncate = true });
    defer {
        file.close(io);
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
    }
    try file.setLength(io, 4096);
    var region = try MmapRegion.init(file, 4096, false);
    defer region.deinit();
    const slice = region.ptr[0..region.len];
    slice[0]    = 0xAA;
    slice[4095] = 0xBB;
    try region.sync();
    var buf: [1]u8 = undefined;
    _ = try std.Io.File.readPositionalAll(file, io, &buf, 0);
    try std.testing.expectEqual(@as(u8, 0xAA), buf[0]);
}

test "isProcessAlive" {
    const pid = getCurrentProcessId();
    try std.testing.expect(isProcessAlive(pid));
    try std.testing.expect(!isProcessAlive(0xfffffffc));
    try std.testing.expect(!isProcessAlive(0));
}
