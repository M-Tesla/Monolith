//! OS abstraction layer: memory-mapped files and process management.
//! Supports Windows (Win32) and POSIX (Linux, macOS).

const std     = @import("std");
const builtin = @import("builtin");
const consts  = @import("../core/consts.zig");

const is_windows = builtin.os.tag == .windows;
const WINAPI: std.builtin.CallingConvention =
    if (builtin.cpu.arch == .x86) .stdcall else .c;

pub const FileHandle = std.fs.File;

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
        var how: u32 = if (exclusive) posix.LOCK.EX else posix.LOCK.SH;
        if (!wait) how |= posix.LOCK.NB;
        posix.flock(file.handle, how) catch return MmapError.LockFailed;
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
        std.posix.flock(file.handle, std.posix.LOCK.UN) catch return MmapError.UnlockFailed;
    }
}

// ── Memory-mapped region ──────────────────────────────────────────────────────

pub const MmapRegion = struct {
    ptr:    [*]align(4096) u8,
    len:    usize,
    handle: if (is_windows) ?std.os.windows.HANDLE else void,

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
            };
        } else {
            const posix = std.posix;
            const prot: u32 = if (read_only)
                posix.PROT.READ
            else
                posix.PROT.READ | posix.PROT.WRITE;
            const flags = posix.MAP{ .TYPE = .SHARED };
            const result = posix.mmap(null, size, prot, flags, file.handle, 0)
                catch return MmapError.MappingFailed;
            return .{
                .ptr    = @alignCast(result),
                .len    = size,
                .handle = {},
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

    pub fn sync(self: *MmapRegion) void {
        if (comptime is_windows) {
            _ = FlushViewOfFile(self.ptr, self.len);
        } else {
            std.posix.msync(@alignCast(self.ptr[0..self.len]), std.posix.MSF.SYNC) catch {};
        }
    }
};

// ── Process / thread identity ─────────────────────────────────────────────────

pub fn getCurrentProcessId() u32 {
    if (comptime is_windows) {
        return GetCurrentProcessId();
    } else {
        return @intCast(std.posix.getpid());
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
                const err = std.os.windows.kernel32.GetLastError();
                return @intFromEnum(err) == 5; // ERROR_ACCESS_DENIED: process exists
            };
        defer std.os.windows.CloseHandle(handle);
        var exit_code: u32 = 0;
        const rc = GetExitCodeProcess(handle, &exit_code);
        if (rc != 0) return exit_code == STILL_ACTIVE;
        return false;
    } else {
        std.posix.kill(@intCast(pid), 0) catch |err| {
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
    var file = try std.fs.cwd().createFile(tmp_path, .{ .read = true, .truncate = true });
    defer {
        file.close();
        std.fs.cwd().deleteFile(tmp_path) catch {};
    }
    try file.setEndPos(4096);
    var region = try MmapRegion.init(file, 4096, false);
    defer region.deinit();
    const slice = region.ptr[0..region.len];
    slice[0]    = 0xAA;
    slice[4095] = 0xBB;
    region.sync();
    try file.seekTo(0);
    var buf: [1]u8 = undefined;
    _ = try file.read(&buf);
    try std.testing.expectEqual(@as(u8, 0xAA), buf[0]);
}

test "isProcessAlive" {
    const pid = getCurrentProcessId();
    try std.testing.expect(isProcessAlive(pid));
    try std.testing.expect(!isProcessAlive(0xfffffffc));
    try std.testing.expect(!isProcessAlive(0));
}
