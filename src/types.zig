// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Public API types for Monolith — pure Zig, no C dependency.

// ---------------------------------------------------------------------------
// DBI handle — slot index in the DbiState array
// ---------------------------------------------------------------------------
pub const Dbi = u32;

// ---------------------------------------------------------------------------
// Engine error set
// ---------------------------------------------------------------------------
pub const Error = error{
    /// Key not found
    NotFound,
    /// Page not found — database corrupted
    PageNotFound,
    /// Database corrupted
    Corrupted,
    /// Severe internal error
    Panic,
    /// Incompatible file format version
    VersionMismatch,
    /// File is not a Monolith database
    Invalid,
    /// Memory-map size exhausted
    MapFull,
    /// Maximum number of DBIs reached
    DbsFull,
    /// Maximum number of readers reached
    ReadersFull,
    /// Transaction too large
    TxnFull,
    /// Cursor stack overflow
    CursorFull,
    /// Page full (split required)
    PageFull,
    /// Unable to extend memory map
    UnableExtendMapSize,
    /// DBI flags incompatible with existing database
    Incompatible,
    /// Invalid key or value size
    BadValSize,
    /// Invalid transaction handle
    BadTxn,
    /// Invalid reader slot reference
    BadRslot,
    /// Invalid DBI handle
    BadDbi,
    /// I/O problem
    Problem,
    /// Environment already open / busy
    Busy,
    /// Key already exists (nooverwrite flag set)
    KeyExist,
    /// Generic I/O error
    ErrnoIo,
    /// Unknown/unmapped error
    Unknown,
};

// ---------------------------------------------------------------------------
// Environment flags
// ---------------------------------------------------------------------------
pub const EnvFlags = struct {
    /// Path points directly to the data file, not a directory
    nosubdir: bool = true,
    /// Open in read-only mode
    rdonly: bool = false,
    /// Write-map mode (reserved for compatibility — not used in pure-Zig path)
    writemap: bool = false,
    /// Skip fsync on commit — OS page cache coherent so process crash is safe,
    /// but power failure may lose the last committed transaction.
    safe_nosync: bool = false,
    /// Skip fsync AND skip meta write-ordering guarantees. Fastest mode.
    /// Only safe for purely ephemeral / cache databases.
    nosync: bool = false,
    /// GC reclaims most recently freed pages first (LIFO) instead of oldest (FIFO).
    /// Better OS page-cache hit rate for write-heavy workloads.
    liforeclaim: bool = false,
    /// Open without a lock file — single-process/single-thread use only.
    /// No `.lck` file is created; writer lock and reader table are no-ops.
    exclusive: bool = false,
    /// Merge freed-page lists from reclaimable GC entries into the current
    /// transaction's GC entry, keeping the GC tree compact.
    /// Default ON — almost always a win for write-heavy workloads.
    coalesce: bool = true,
};

// ---------------------------------------------------------------------------
// Geometry — controls mmap growth behaviour
// ---------------------------------------------------------------------------
pub const Geometry = struct {
    /// Minimum map size in bytes (0 = keep current).
    size_lower: usize = 0,
    /// Maximum map size in bytes (0 = unlimited).
    size_upper: usize = 0,
    /// Grow in multiples of this value in bytes (0 = grow by minimum needed).
    growth_step: usize = 0,
    /// Shrink the file when free space exceeds this many bytes (0 = never shrink).
    shrink_threshold: usize = 0,
};

// ---------------------------------------------------------------------------
// Database (DBI) flags
// ---------------------------------------------------------------------------
pub const DbFlags = struct {
    /// Create the DBI if it does not exist
    create: bool = false,
    /// Allow multiple values per key (sorted duplicate keys)
    dupsort: bool = false,
    /// Keys are native integers, sorted numerically
    integerkey: bool = false,
    /// Duplicate values have a fixed size (requires dupsort)
    dupfixed: bool = false,
    /// Duplicate values are native integers (requires dupsort)
    integerdup: bool = false,
    /// Keys are compared in reverse byte order
    reversekey: bool = false,
    /// Duplicate values are compared in reverse byte order (requires dupsort).
    /// Stored bytes are bitwise-complemented so lexicographic order equals
    /// descending order on original values.
    reversedup: bool = false,
    /// Accept whatever flags are already stored for this DBI — skip
    /// compatibility validation.  Useful when reopening a DBI without
    /// remembering its original flags.
    accede: bool = false,
};

// ---------------------------------------------------------------------------
// Transaction flags
// ---------------------------------------------------------------------------
pub const TxnFlags = struct {
    rdonly: bool = false,
};

// ---------------------------------------------------------------------------
// Put / cursor-put flags
// ---------------------------------------------------------------------------
pub const PutFlags = struct {
    /// Do not overwrite if key already exists
    nooverwrite: bool = false,
    /// DupSort: do not insert if the (key, value) pair already exists
    nodupdata: bool = false,
    /// Update the current record in-place; do not insert a new one
    current: bool = false,
    /// Key MUST be greater than the last inserted key (bulk-load optimisation)
    append: bool = false,
};

// ---------------------------------------------------------------------------
// KV — key/value pair as byte slices
// ---------------------------------------------------------------------------
pub const KV = struct {
    key: []const u8,
    val: []const u8,
};

// ---------------------------------------------------------------------------
// Stat — per-DBI B-tree statistics
// ---------------------------------------------------------------------------
pub const Stat = struct {
    /// Page size in bytes
    page_size:    u32,
    /// Height of the B-tree (1 = root is a leaf)
    depth:        u16,
    /// Number of internal (branch) pages
    branch_pages: u64,
    /// Number of leaf pages
    leaf_pages:   u64,
    /// Number of large (overflow) pages
    large_pages:  u64,
    /// Total number of key/value entries
    items:        u64,
};

// ---------------------------------------------------------------------------
// EnvStat — overall environment statistics
// ---------------------------------------------------------------------------
pub const EnvStat = struct {
    /// Page size in bytes
    page_size:       u32,
    /// Number of pages currently allocated (used or free)
    total_pages:     u64,
    /// Transaction ID of the last committed write transaction
    last_txnid:      u64,
    /// Maximum pages allowed by the current geometry
    geo_upper_pages: u64,
};
