# Monolith

Embedded B+ tree storage engine in pure Zig. No C, no dependencies.

MVCC, copy-on-write pages, crash-safe commits via ping-pong meta, and a GC tree that keeps the file size bounded. Compiles to a single object file.

---

## Architecture

```
Public API — Environment · Transaction · Cursor · DBI
B+ Tree    — splits · rebalance · overflow pages · dupsort
MVCC / GC  — snapshot isolation · reader slots · FreeDB
OS Layer   — mmap · file locking · Windows + POSIX
```

File layout:

```
Page 0-1  meta slots (ping-pong, atomic commit)
Page 2    main B+ tree root
Page 3    GC tree root
Page 4+   data pages
```

---

## Features

- Copy-on-write: writers never touch pages visible to active readers
- Readers never block writers, writers never block readers
- Named sub-databases (DBIs) within a single file
- DupSort, integer keys, reverse keys, custom comparators
- Nested transactions
- Overflow pages for large values
- Spill list for large write transactions
- GC coalescing keeps the free-page tree O(1)
- Hot backup via `env.copy()`
- Full page checksums (FNV-1a), verified on read
- `env.check()` for integrity audits
- Windows and POSIX

---

## Usage

```zig
pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var env = try monolith.Environment.open("data.monolith", .{}, 16, 1 << 30, io);
    defer env.close();

    // Write
    var txn = try monolith.Transaction.begin(&env, null, .{});
    errdefer txn.abort();
    const dbi = try txn.openDbi("users", .{ .create = true });
    try txn.put(dbi, "alice", "admin", .{});
    try txn.commit();

    // Read
    var rtxn = try monolith.Transaction.begin(&env, null, .{
        .rdonly = true,
    });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("users", .{});
    const val = try rtxn.get(rdbi, "alice"); // ?[]const u8
}
```

---

## Build

```bash
zig build
zig test src/lib.zig
```

## Run basic example (located in src/examples)

```bash
zig build run-basic_example
```

Requires Zig 0.16.
(Zig 0.17.0-dev.1099+7db2ef610 fully compatible).

---

## License

MIT
