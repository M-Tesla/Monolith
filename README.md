# Monolith

**Embedded B+ tree storage engine made in pure Zig. No C and no dependencies.**

Monolith is a storage engine built from scratch in pure Zig. No C. No external libraries. Every byte of every page, every B+ tree operation, every MVCC invariant: written, debugged, and proven correct in Zig.

The result is a crash-safe, MVCC-correct, GC-managed key-value storage engine that compiles to a single object file with no runtime dependencies.

---

## What it is

A storage engine like BoltDB (B+ tree, COW, MVCC) or RocksDB (LSM). Not a database, not a query language and not a server.

It is the layer that a database is built on top of. It answers one question: given a key, give me the value, or let me store one. It does this with a B+ tree, a memory-mapped file, and a transaction system that guarantees your data is either fully written or not written at all.

If SQLite WAL mode is the closest analogy, Monolith is SQLite WAL without the SQL: just the storage, done properly, in a language that lets you reason about every byte.

---

## Architecture

```
+-------------------------------------------------+
|                  Public API                     |
|   Environment . Transaction . Cursor . DBI      |
+-------------------------------------------------+
|               B+ Tree Engine                     |
|   btreePut . btreeGet . btreeDel . splits       |
|   rebalance . overflow pages . dupsort          |
+-------------------+-----------------------------+
|   MVCC / GC       |   Page Layer                |
|   FreeDB . reader |   PageHeader . Node         |
|   slots . txnid   |   search . put . checksum   |
+-------------------+-----------------------------+
|              OS / Platform Layer                |
|   MmapRegion . LockManager . file I/O           |
|   Windows (Kernel32) + POSIX (mmap/flock)       |
+-------------------------------------------------+
```

### File layout

```
Page 0: Meta A  (ping-pong pair, atomic commit via alternation)
Page 1: Meta B
Page 2: Main B+ tree root (DBI catalog)
Page 3: GC tree root (FreeDB)
Page 4+: Data pages allocated dynamically
```

Every commit writes dirty pages, then atomically flips the meta slot. Crash at any point leaves the previous meta intact. No WAL file, no journal and no recovery process.

---

## Features

### Core B+ Tree
- Copy-on-write (COW) pages: writers never touch pages visible to readers
- Page splits: leaf split, branch split, recursive promotion, new root creation
- Page rebalancing on delete: merge with sibling (right then left), root collapse
- Overflow pages for values larger than half a page: transparent to caller
- Sorted dirty-page flush at commit: sequential I/O instead of hash-map disorder

### MVCC
- Each transaction sees a consistent snapshot of the database at its start time
- Readers never block writers. Writers never block readers.
- Reader slots tracked in a shared lock file, surviving process crashes gracefully
- `oldest_reader` query drives GC safety: freed pages are never recycled while a reader can still see them

### Garbage Collection
- FreeDB (GC tree): freed pages stored as `txnid -> []pgno` entries
- Pages recycled before allocating new ones: file size stays bounded
- LIFO reclaim mode: recycle most-recently-freed pages first for hot OS cache reuse
- GC coalescing (default ON): old GC entries are absorbed into the current commit batch, GC tree stays O(1) in size regardless of write volume
- `serializeFreedPages` uses a while loop so pages freed *during* GC serialization are themselves captured: no pages ever leak

### Transactions
- Read-only and read-write transactions
- Nested transactions: child txn commits merge dirty list into parent; abort discards only child diff
- `txn.renew()`: reuse a finished read-only transaction object without reallocation
- Spill list: when dirty page count exceeds threshold, half are spilled to cold heap storage and un-spilled on demand
- Writemap mode: pages go through heap COW for pointer stability across mmap resizes; shadow list restores originals on abort

### Named DBIs (Sub-databases)
- Multiple named B+ trees within a single file
- DBI catalog stored in the main tree: key = name, value = serialized `Tree` struct
- Flags persisted and validated on reopen, `error.Incompatible` on mismatch
- `accede` flag: skip flag validation when reopening without knowing original flags
- `env.closeDbi()`: slot returned to free list, reusable by next `openDbi`
- `txn.dropDbi()`: truncate or permanently delete a named DBI

### Cursor API
- `first()`, `last()`, `next()`, `prev()`: full bidirectional traversal
- `seek(key)`: lower-bound search
- `getBothRange(key, val)`: lower-bound on val within a DupSort key's set
- `firstDup()`, `lastDup()`, `nextDup()`, `prevDup()`: DupSort navigation
- `del()` keeps position: after delete, cursor points to successor
- `renew(txn)`: rebind cursor to a new transaction without allocation

### Key Types and Sort Orders
- Default: lexicographic byte order
- `integerkey`: u32/u64 little-endian numerical sort
- `reversekey`: reverse lexicographic order
- `reversedup`: duplicate values stored bitwise-complemented so lex order = descending value order
- Custom compare functions: pass any `fn([]const u8, []const u8) std.math.Order` per DBI

### Put Modes
- Default upsert
- `nooverwrite`: fail if key exists
- `nodupdata`: in DupSort, skip if exact (key, val) pair exists
- `append`: O(1) bulk-load insert, caller guarantees key is the new maximum, skips binary search
- `current`: overwrite value at cursor position in-place
- `reserve`: allocate space for a value and return a writable slice, zero-copy insertion

### Durability Policies
- Default: `fsync` after every commit
- `safe_nosync`: skip fsync, process crash is safe (OS page cache is coherent), power failure may lose last commit
- `nosync`: skip fsync and meta write ordering, fastest, only for ephemeral data
- `env.sync()`: explicit flush, batch multiple commits then sync once
- `close()`: always syncs regardless of policy

### Memory Map Management
- `env.setGeometry()`: control `size_upper`, `growth_step`, `shrink_threshold`
- `growth_step`: file grows in fixed-size chunks instead of page-by-page
- `size_upper`: hard cap, `error.MapFull` if exceeded
- Auto-shrink: after commit, if free space exceeds `shrink_threshold`, remap to live size
- Mmap full callback: user function invoked before resize, can pre-allocate or abort

### Integrity
- FNV-1a checksum on every data page, stamped at commit
- `env.check()`: full audit, validates checksums, overflow page runs, branch/leaf pointer consistency, no orphaned pages
- Meta-page ping-pong guarantees atomic visibility of commits

### Utilities
- `env.copy(dest)`: hot backup, consistent snapshot without stopping readers
- `env.copyCompact(dest)`: compacting backup, rewrites only live pages in contiguous order, eliminating gaps from deleted data
- `txn.putCanary()` / `txn.getCanary()`: four u64 values written into meta for application-level integrity checks
- `txn.sequence()`: atomic monotonic u64 counter per transaction
- `env.setUserCtx()` / `env.getUserCtx()`: attach arbitrary pointer to environment for use in callbacks
- `env.stat()` / `txn.dbiStat()`: B+ tree topology, depth, branch pages, leaf pages, overflow pages, item count
- CLI: `monolith_chk` (integrity audit), `monolith_dump` (export), `monolith_load` (import)

### Exclusive Mode
- `EnvFlags.exclusive`: skip the `.lck` file entirely, single-process use, zero lock overhead

---

## Usage

```zig
const monolith = @import("monolith");

// Open (creates if new, validates if existing)
var env = try monolith.Environment.open("data.monolith", .{}, 16, 1 << 30);
defer env.close();

// Write
var txn = try monolith.Transaction.begin(&env, null, .{});
errdefer txn.abort();
const dbi = try txn.openDbi("users", .{ .create = true });
try txn.put(dbi, "alice", "admin", .{});
try txn.commit();

// Read
var rtxn = try monolith.Transaction.begin(&env, null, .{ .rdonly = true });
defer rtxn.abort();
const rdbi = try rtxn.openDbi("users", .{});
const val = try rtxn.get(rdbi, "alice"); // ?[]const u8
```

---

## The Journey

This did not start clean. Early versions carried C dependencies for platform I/O and locking. That meant a build system that compiled C, a layer of translation between Zig idioms and C conventions, and error handling that went through foreign code nobody fully controlled.

**v0.1** was already on fire: P_LEAF2 (DupFixed pages) was corrupting reads because `data_union` was being overwritten during node parsing. Fixed at the bit level. Then mmap resize was invalidating cursor pointers mid-traversal, fixed by saving pgno on the stack and re-fetching after resize.

**v0.2** required deleting entire sub-trees when DupSort pages drained to empty. Not just the page: the entire sub-tree, clearing the flag in the parent.

**v0.3** brought MVCC correctness: the allocator cannot recycle a freed page if any reader still holds a snapshot that references it. This required a reader slot table in the lock file and a real `oldest_reader` query.

**v0.4** was a load test: 10,000 heavy keys, 5,000 deletions, confirm GC absorbs the freed pages and the file does not grow unboundedly.

**v0.5 onward**, the C layer started dying. Auto-shrink, LIFO/FIFO reclaim, writemap mode, cross-platform lock recovery, geometry API: all rewritten in pure Zig against the native engine.

**v0.6** was the hardest structural piece: page splits. A leaf fills up, splits into two, the separator key propagates to the branch parent, which may itself be full, which splits recursively, which may create a new root. Getting the leftmost-key convention right (the leftmost entry in a branch always has key `""`, the actual key lives in the child) took several iterations of corrupted databases before the invariant clicked.

**v0.7** added the other half: rebalancing on delete. When a page falls below 50% fill, it tries to merge with its right sibling, then left, then gives up. Branch pages that end up with one child collapse the root. This is where most real-world storage engines cut corners. We did not.

**v0.8** nested transactions: a child copies the parent's dirty list at birth. Commit merges back. Abort discards. Simple in theory, subtle in practice because the child must COW pages the parent already COW'd.

**v0.9** overflow pages: any value over roughly 2KB gets its own chain of dedicated pages, tracked with `F_BIGDATA` in the leaf node. The COW path for overflow is different from regular pages because you are COWing a run, not a single page.

**v0.10** closed the GC loop properly: `serializeFreedPages` had a subtle aliasing bug where pages freed *by* btreePut (inserting into the GC tree) were lost because the freed list was cleared before they accumulated. Fixed with a snapshot-and-swap pattern and a while loop that catches everything.

**v0.11** added checksums: every page gets an FNV-1a checksum stamped at commit. Silent disk corruption is caught on read. This required repurposing a field that was dead weight in the page header.

**v0.12** spill list: a single transaction touching 100k keys cannot hold all dirty pages in RAM. When the dirty list exceeds 64 pages, half are spilled to a secondary heap structure and brought back on demand. Without this, large transactions either crash or corrupt.

**v0.13** GC coalescing: without it, a database with N write-then-delete cycles accumulates N GC entries. The tree grows linearly. With coalescing, old entries are absorbed into the current batch before commit, the GC tree stays bounded. Default ON.

**v0.14** sorted flush: dirty pages were written in hash-map iteration order (random). Sorting by pgno before writing turns random I/O into sequential I/O. One sort, real gain, especially on spinning disk.

**v0.15** production hardening. A full audit of every file in the engine. Nine confirmed bugs, all fixed.

`putNode` used `@panic` in four runtime conditions — key too long, page full, wrong page type, bad branch argument. Any of those would kill the process with no recovery path. The function signature changed from `*Node` to `!*Node`, returning typed errors. Every call site in the codebase was updated.

`MmapRegion.sync()` returned `void` and discarded all I/O errors — `FlushViewOfFile` on Windows with `_ =`, `msync` on POSIX with `catch {}`. A committed transaction could believe it was durable when the disk had failed. The function now returns `!void` and the error propagates through `commit()`.

`first_unallocated` is a `u32`. At `0xFFFFFFFF`, an unchecked increment would wrap to 0 and the engine would start allocating pages over the meta pages. Added an explicit guard before every allocation. The same check covers overflow page runs, where the addition `first_pgno + n_pages` could exceed 32 bits.

Checksums were computed and written on every commit but never verified on read. A bit-flip, a partial write, memory corruption — all passed through undetected. Added `validatePageChecksum()` called in `getWritablePage()` when copying a page from the mmap. Skipped for nested transactions: pages from a parent's dirty list have not been flushed yet and carry no checksum.

In exclusive mode, reader slot methods were walking a global `ReaderTable` via a raw pointer. Since exclusive mode is single-process by definition, reader tracking is not needed at all. All methods that touched `self.table` now return early when `exclusive = true`. The pointer is never accessed.

`closeDBI` and the error paths in `openDbi` used `dbi_free_slots.append(...) catch {}`. An OOM failure there silently orphaned the slot. With enough open/close cycles under memory pressure, the environment would report `error.DbsFull` prematurely. Fixed with `ensureTotalCapacity(max_dbs)` at environment open time.

DBI names longer than 255 bytes were silently truncated. Two DBIs whose names shared the first 255 characters would resolve to the same slot. Added explicit validation: `error.NameTooLong`.

`tryAutoShrink` called `self.map.deinit()` before attempting the remap. If `MmapRegion.init()` failed, `self.map` pointed at an unmapped region. Platform-aware fix: on POSIX, the file can be truncated while the old mapping is alive, so the new mapping is obtained before releasing the old one (try-then-swap). On Windows, unmapping before truncating is required, so the original unmap-first sequence is kept but with a proper fallback that restores the file size if remap fails.

`getPagePtr` had no bounds check. A corrupted pgno from a B-tree node would read beyond the mmap with no signal. Added `std.debug.assert` to catch the condition in debug and test builds. A `getPagePtrSafe()` variant returning `!*PageHeader` is available for callers that need explicit error handling.


Throughout all of this, every feature shipped with tests. The test suite grew from a handful of sanity checks to 146 tests covering basic CRUD, splits, merges, overflow, dupsort, nested txns, GC recycling, MVCC isolation, crash safety, writemap, geometry, shrink, checksums, backup, compaction, and every flag combination that matters.

---

## Test Suite

```
zig test src/lib.zig
```

146 tests. All pass.

```
test_basic            . test_dupsort          . test_integerkey
test_multi_dbi        . test_splits_drop      . test_nested
test_advanced_put     . test_utilities        . test_advanced_cursors
test_overflow         . test_cursor_prev      . test_reversekey
test_mvcc_gc          . test_flags_persist    . test_checksums
test_limits           . test_canary           . test_dupsort_positioned
test_replace          . test_compact          . test_spill
test_writemap         . test_sync_flags       . test_txn_renew
test_close_dbi        . test_geometry         . test_shrink
test_liforeclaim      . test_exclusive        . test_coalesce
test_sorted_flush     . test_accede
```

---

## What Monolith is not

It is not a database. It has no SQL, no query planner, no network protocol, no authentication.

It is not a replacement for PostgreSQL. Postgres is a full relational system with row-level locking, replication, and decades of production hardening. 

Monolith is the layer that lives below all of that.

It is not slow. Zero-copy reads from mmap, O(log n) seeks, sequential commit I/O, no VACUUM process, no buffer pool competing with the OS page cache.

It is a foundation, like BoltDB (B+ tree, COW, MVCC) or RocksDB (LSM). Build on it. :)

---

## Building

```bash
zig build
zig test src/lib.zig
```

Zig 0.15+. No C compiler. No system libraries beyond the OS itself.

---

## License

MIT
