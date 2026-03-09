//! Page Structures
// Layouts based on monolith-internals.h

const std = @import("std");
const types = @import("../core/types.zig");
const consts = @import("../core/consts.zig");

/// Key comparator function type used by page.search and all B-tree traversals.
pub const CmpFn = *const fn (a: []const u8, b: []const u8) std.math.Order;

/// Default lexicographic comparator (used by tests and as the catalog default).
pub fn cmpDefault(a: []const u8, b: []const u8) std.math.Order {
    return std.mem.order(u8, a, b);
}

pub const P_BRANCH: u16 = 0x01;
pub const P_LEAF: u16 = 0x02;
pub const P_OVERFLOW: u16 = 0x04;
pub const P_META: u16 = 0x08;
pub const P_DIRTY: u16 = 0x10;
pub const P_LEAF2: u16 = 0x20;
pub const P_SUBP: u16 = 0x40;
pub const P_LOOSE: u16 = 0x4000;
pub const P_KEEP: u16 = 0x8000;

/// Header common to all pages (20 bytes)
pub const PageHeader = extern struct {
    /// Transaction ID that created the page
    txnid: u64 align(4), // 8 bytes, align(4) to match #pragma pack(4)
    
    /// Key size for DUPFIX (P_LEAF2) pages
    dupfix_ksize: u16,    // 2 bytes

    /// Page flags
    flags: u16,           // 2 bytes

    /// Slotted-page index area / overflow page count.
    /// For standard pages: low 16 bits = lower (end of index array),
    ///                     high 16 bits = upper (start of data heap).
    /// For P_LEAF2 pages: entire u32 = item count.
    data_union: u32,      // 4 bytes
    
    /// Page number
    pgno: types.pgno_t,   // 4 bytes (u32)

    pub fn getPages(self: *const PageHeader) u32 {
        return self.data_union;
    }

    pub fn getLower(self: *const PageHeader) types.indx_t {
         // Little Endian: lower is first 16 bits
         return @as(u16, @truncate(self.data_union));
    }
    
    pub fn getUpper(self: *const PageHeader) types.indx_t {
        // Little Endian: upper is high 16 bits
        return @as(u16, @truncate(self.data_union >> 16));
    }
    
    pub fn setLowerUpper(self: *PageHeader, lower: types.indx_t, upper: types.indx_t) void {
        self.data_union = @as(u32, lower) | (@as(u32, upper) << 16);
    }

    /// Returns the number of entries (nodes) in the page
    pub fn getNumEntries(self: *const PageHeader) u16 {
        const PAGEHDRSZ: u16 = 20; // 20 bytes
        const IsDupFixed = (self.flags & P_LEAF2) != 0;
        
        if (IsDupFixed) {
            // For P_LEAF2 (DUPFIXED), data_union stores the item count directly.
            return @as(u16, @truncate(self.data_union));
        }

        const lower = self.getLower();
        if (lower < PAGEHDRSZ) return 0; // Should not happen if valid
        return (lower - PAGEHDRSZ) / 2; // indx_t is u16 (2 bytes)
    }
    


    /// Returns the pointer to the Node at the specified index
    /// NOTE: Only for standard Branch/Leaf pages.
    /// Returns value from P_DUPFIXED page
    pub fn getDupFixedVal(self: *const PageHeader, index: u16) []const u8 {
        const PAGEHDRSZ: u16 = 20;
        const ksize = self.dupfix_ksize;
        const offset = @as(usize, PAGEHDRSZ) + (@as(usize, index) * ksize);
        
        const ptr = @as([*]const u8, @ptrCast(self));
        return ptr[offset .. offset + ksize];
    }
    
    pub fn getNode(self: *const PageHeader, index: u16) *Node {
        const PAGEHDRSZ: usize = 20;
        const ptr_u8 = @as([*]const u8, @ptrCast(self));
        
        const indices_ptr = @as([*]const types.indx_t, @ptrCast(@alignCast(ptr_u8 + PAGEHDRSZ)));
        const offset = indices_ptr[index];
        return @as(*Node, @ptrCast(@constCast(@alignCast(ptr_u8 + offset))));
    }

    pub const SearchResult = struct {
        index: u16,
        match: bool,
    };

    /// Binary search for key in the page using the provided comparator.
    /// Returns the key index (exact match) or insertion point (no match).
    pub fn search(self: *const PageHeader, key: []const u8, cmp_fn: CmpFn) SearchResult {
        var low: u16 = 0;
        var high: u16 = self.getNumEntries();

        while (low < high) {
            const mid = low + (high - low) / 2;
            const node_key = self.getNode(mid).getKey();
            switch (cmp_fn(node_key, key)) {
                .lt => low  = mid + 1,
                .gt => high = mid,
                .eq => return .{ .index = mid, .match = true },
            }
        }

        return .{ .index = low, .match = false };
    }
    
    /// Returns the contiguous free space in the page (in bytes)
    pub fn getFreeSpace(self: *const PageHeader) u32 {
        const PAGEHDRSZ: u16 = 20;
        if ((self.flags & P_LEAF2) != 0) {
             const used = PAGEHDRSZ + (@as(u32, self.getNumEntries()) * @as(u32, self.dupfix_ksize));
             if (used > consts.DATAPAGESIZE) return 0;
             return consts.DATAPAGESIZE - used;
        }
        
        const lower = self.getLower();
        const upper = self.getUpper();
        
        if (upper < lower) return 0; // Sanity check error
        return @as(u32, upper) - @as(u32, lower);
    }

    pub fn getUsedSpace(self: *PageHeader) u32 {
        return consts.DATAPAGESIZE - self.getFreeSpace();
    }

    /// Initializes page pointers
    pub fn init(self: *PageHeader, page_size: u32, flags: u16) void {
        self.flags = flags;
        
        if ((flags & P_LEAF2) != 0) {
            // For P_LEAF2 (DUPFIXED), data_union is the item count.
            self.data_union = 0;
        } else {
            // For standard pages, data_union is Lower/Upper offsets.
            self.setLowerUpper(20, @as(u16, @truncate(page_size))); // 20 = Header Size
        }
        
        self.txnid = 0;
    }

    /// Inserts a node at the specified position.
    /// Assumes there is enough space (check with getFreeSpace).
    /// Returns error if it fails (but here we assume void and panic or caller check).
    /// Returns pointer to the written node.
    pub fn putNode(self: *PageHeader, index: u16, key: []const u8, val: []const u8, flags: u8) *Node {
        // Assert NOT P_LEAF2 (DUPFIXED)
        // DUPFIXED pages use a flat array layout, not the slotted layout handled by putNode.
        if ((self.flags & P_LEAF2) != 0) {
             @panic("putNode called on P_LEAF2 (DUPFIXED) page!");
        }

        // 1. Calculate node size
        // Node Header (8) + Key + Data
        var node_size: u32 = 8 + @as(u32, @truncate(key.len)) + @as(u32, @truncate(val.len));
        
        if (key.len > 4000) {
             std.debug.panic("putNode FATAL: key.len={d} exceeds limit", .{key.len});
        }
        
        // Alignment to 2 bytes (word alignment)
        if (node_size % 2 != 0) node_size += 1;
        
        // 2. Allocate space (from upper down)
        var upper = self.getUpper();
        const lower = self.getLower();
        
        // Check space (needs node_size + 2 bytes for offset index)
        if (upper - lower < node_size + 2) {
             std.debug.panic("PageFull! Pgno {d}, upper {d}, lower {d}, node_size {d}, entries {d}", .{self.pgno, upper, lower, node_size, self.getNumEntries()});
        }
        
        upper -= @as(u16, @truncate(node_size));
        
        // 3. Write Node
        const ptr_u8 = @as([*]u8, @ptrCast(self));
        const node_ptr = ptr_u8 + upper;
        const node = @as(*Node, @ptrCast(@alignCast(node_ptr)));
        
        const is_branch = (self.flags & P_BRANCH) != 0;
        
        if (is_branch) {
             // Branch: val holds child pgno
             if (val.len != 4) @panic("Branch insert val must be 4 bytes (pgno)");
             const child_pgno = std.mem.readInt(u32, val[0..4], .little);
             node.data_shim = child_pgno;
        } else {
             // Leaf: data_shim holds data size
             node.data_shim = @as(u32, @truncate(val.len));
        }
        
        node.flags = @as(u8, @truncate(flags)); // Cast u16 flags to u8 for Node flags
        node.ksize = @as(u16, @truncate(key.len));
        node.extra = 0;
        
        // Copy Key
        const key_dst = node_ptr + 8;
        @memcpy(key_dst[0..key.len], key);
        
        // Copy Val (Data)
        if (!is_branch) {
             const val_dst = key_dst + key.len;
             @memcpy(val_dst[0..val.len], val);
        }
        
        // Update index array and header boundaries.
        const ptr_u8_header = @as([*]u8, @ptrCast(self));
        const indices_ptr = @as([*]u16, @ptrCast(@alignCast(ptr_u8_header + 20)));
        const num = self.getNumEntries();

        // Shift existing offsets right to make room at `index`.
        if (index < num) {
             const src = indices_ptr + index;
             const dst = indices_ptr + index + 1;
             const count = num - index;
             std.mem.copyBackwards(u16, dst[0..count], src[0..count]);
        }

        indices_ptr[index] = upper;
        // Entry count is derived from (lower - PAGEHDRSZ) / 2; update lower by 2.
        self.setLowerUpper(lower + 2, upper);
        
        return node;
    }

    /// Copies `count` nodes starting at `start_index` from this page into `dest_page`.
    /// `dest_page` is expected to be empty or in append-only mode.
    pub fn copyNodes(self: *const PageHeader, dest_page: *PageHeader, start_index: u16, count: u16) void {
        var i: u16 = 0;
        while (i < count) : (i += 1) {
            const idx = start_index + i;
            const node = self.getNode(idx);
            const dest_idx = dest_page.getNumEntries();
             
             const is_branch = (self.flags & P_BRANCH) != 0;
             var pgno_buf: [4]u8 = undefined;
             var val: []const u8 = undefined;
             
             if (is_branch) {
                 std.mem.writeInt(u32, &pgno_buf, node.getChildPgno(), .little);
                 val = &pgno_buf;
             } else if (node.isOverflow()) {
                 // Overflow node: the inline payload is a 4-byte page number.
                 // data_shim holds the total logical size — do not use getData() here.
                 const data_ptr = @as([*]const u8, @ptrCast(node)) + 8 + node.ksize;
                 val = data_ptr[0..4];
             } else if ((node.flags & P_LEAF2) != 0) {
                 val = self.getDupFixedVal(idx);
             } else {
                 val = node.getData();
             }
             
             _ = dest_page.putNode(dest_idx, node.getKey(), val, node.flags);
         }
    }
    
    /// Truncates the page to `new_count` nodes by adjusting lower/upper boundaries.
    /// Does not zero reclaimed memory — callers must rebuild if compaction is needed.
    pub fn truncate(self: *PageHeader, new_count: u16) void {
        const current_count = self.getNumEntries();
        if (new_count >= current_count) return;
        
        const new_lower = 20 + new_count * 2; // Header(20) + count * sizeof(indx_t)
        var new_upper: u16 = 0;
        var min_offset: u16 = 65535;
        const indices_ptr = @as([*]const types.indx_t, @ptrCast(@alignCast(@as([*]const u8, @ptrCast(self)) + 20)));
        
        var i: u16 = 0;
        while (i < new_count) : (i += 1) {
            const off = indices_ptr[i];
            if (off < min_offset) min_offset = off;
        }
        
        if (min_offset == 65535 and new_count == 0) {
            min_offset = consts.DATAPAGESIZE;
        }

        new_upper = min_offset;
        
        self.setLowerUpper(@as(u16, @truncate(new_lower)), new_upper);
    }

    /// Deletes the node at the specified index.
    /// Compacts the offset array AND the data heap to free up space.
    pub fn delNode(self: *PageHeader, index: u16) void {
        const num_entries = self.getNumEntries();
        if (index >= num_entries) return;
        
        const ptr_u8 = @as([*]u8, @ptrCast(self));
        
        if ((self.flags & P_LEAF2) != 0) {
             // DUPFIXED: no offsets, just array of values
             const ksize = self.dupfix_ksize;
             const base = 20; // P_LEAF2 header size
             
             if (index < num_entries - 1) {
                 const dest_offset = base + (@as(usize, index) * ksize);
                 const src_offset = base + (@as(usize, index + 1) * ksize);
                 const len = @as(usize, num_entries - index - 1) * ksize;
                 
                 const dest_slice = ptr_u8[dest_offset .. dest_offset + len];
                 const src_slice = ptr_u8[src_offset .. src_offset + len];
                 std.mem.copyForwards(u8, dest_slice, src_slice);
             }
             self.data_union -= 1;
             return;
        }

        // Standard Page (P_LEAF, P_BRANCH)
        const indices_ptr = @as([*]types.indx_t, @ptrCast(@alignCast(ptr_u8 + 20)));
        const node_offset = indices_ptr[index];
        const node = self.getNode(index);
        
        // Inline data size: branch = 0 (pgno is in data_shim, no inline bytes);
        // overflow leaf = 4 (overflow pgno stored inline); normal leaf = data_shim.
        const inline_data: u32 = if ((self.flags & P_BRANCH) != 0)
            0
        else if (node.isOverflow())
            4
        else
            node.getDataSize();
        const node_size: u16 = @intCast(8 + node.ksize + inline_data);
            
        // Align to even
        const aligned_size: u16 = (node_size + 1) & ~@as(u16, 1);
        
        // To delete a node at offset N, shift the block [upper..N) up by aligned_size bytes,
        // then advance upper by the same amount.
        
        const upper = self.getUpper();
        if (node_offset > upper) { // The block to move is everything before this node
             const block_len = node_offset - upper;
             const src_slice = ptr_u8[upper .. upper + block_len];
             const dest_slice = ptr_u8[upper + aligned_size .. upper + aligned_size + block_len];
             std.mem.copyBackwards(u8, dest_slice, src_slice);
             
             // Adjust all offsets for nodes that were below the deleted one.
             var i: u16 = 0;
             while (i < num_entries) : (i += 1) {
                 if (i == index) continue;
                 if (indices_ptr[i] < node_offset) {
                     indices_ptr[i] += aligned_size;
                 }
             }
        }
        
        // Compact the index array by removing the slot for the deleted entry.
        if (index < num_entries - 1) {
             const dest_slice = indices_ptr[index..num_entries-1];
             const src_slice = indices_ptr[index+1..num_entries];
             std.mem.copyForwards(types.indx_t, dest_slice, src_slice);
        }
        
        // Shrink the index area (lower) and grow the data heap (upper).
        const lower = self.getLower();
        self.setLowerUpper(lower - @sizeOf(types.indx_t), upper + aligned_size);
    }
};

/// Based on monolith Little Endian layout
pub const Node = extern struct {
    /// Data or Pointer to child page (u32)
    /// Little Endian: dsize/child_pgno comes first
    data_shim: u32 align(2),

    /// Node flags
    flags: u8,
    
    /// Extra (not used much, padding or alignment)
    extra: u8,
    
    /// Key size
    ksize: u16,
    
    pub const F_BIGDATA: u8 = 0x01;
    pub const F_SUBDATA: u8 = 0x02;
    pub const F_DUPDATA: u8 = 0x04;

    pub fn getChildPgno(self: *const Node) types.pgno_t {
        return self.data_shim;
    }
    
    pub fn getDataSize(self: *const Node) u32 {
        return self.data_shim;
    }

    /// Returns the node key
    pub fn getKey(self: *const Node) []const u8 {
        const ptr = @as([*]const u8, @ptrCast(self));
        // Key starts after header (8 bytes)
        const ksize = self.ksize;
        const key_ptr = ptr + 8;
        return key_ptr[0..ksize];
    }

    pub fn isOverflow(self: *const Node) bool {
        return (self.flags & F_BIGDATA) != 0;
    }
    
    pub fn getOverflowPgno(self: *const Node) types.pgno_t {
        const ptr = @as([*]const u8, @ptrCast(self));
        const data_ptr = ptr + 8 + self.ksize;
        return std.mem.readInt(u32, data_ptr[0..4], .little);
    }
    
    /// Returns the node's value bytes. Data starts immediately after the key.
    pub fn getData(self: *const Node) []const u8 {
        const ptr = @as([*]const u8, @ptrCast(self));
        const data_ptr = ptr + 8 + self.ksize;
        return data_ptr[0..self.data_shim];
    }
};

// ─── Checksum ─────────────────────────────────────────────────────────────────

/// FNV-1a 32-bit hash of the page body (bytes 20..page_size) with pgno mixed in.
/// Stored in `dupfix_ksize` for all non-P_LEAF2 pages (dupfix_ksize is unused there).
/// Detects silent data corruption and page transpositions.
pub fn computePageChecksum(page: *const PageHeader, page_size: usize) u16 {
    const bytes = @as([*]const u8, @ptrCast(page));
    var h: u32 = 2166136261; // FNV-1a 32-bit offset basis
    // Mix pgno so a page written to the wrong slot is detected.
    const pgno_bytes = std.mem.asBytes(&page.pgno);
    for (pgno_bytes) |b| h = (h ^ b) *% 16777619;
    // Hash the page body (everything after the 20-byte header).
    for (bytes[20..page_size]) |b| h = (h ^ b) *% 16777619;
    // XOR-fold to 16 bits.
    return @as(u16, @truncate(h ^ (h >> 16)));
}

test "PageHeader Size" {
    // 8 + 2 + 2 + 4 + 4 = 20 bytes
    try std.testing.expectEqual(@as(usize, 20), @sizeOf(PageHeader));
}

test "Node Size" {
    // 4 + 1 + 1 + 2 = 8 bytes
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(Node));
}

test "PageHeader Put/Search" {
    var buffer: [4096]u8 align(4) = undefined;
    const ptr = @as(*PageHeader, @ptrCast(&buffer));
    
    // Init
    ptr.init(4096, P_LEAF);
    
    // Check Free Space (4096 - 20 = 4076)
    try std.testing.expectEqual(@as(u32, 4076), ptr.getFreeSpace());
    
    // Insert "key1" -> "val1" at index 0
    _ = ptr.putNode(0, "key1", "val1", 0);
    
    // Verify
    try std.testing.expectEqual(@as(u16, 1), ptr.getNumEntries());
    const n1 = ptr.getNode(0);
    try std.testing.expectEqualStrings("key1", n1.getKey());
    try std.testing.expectEqualStrings("val1", n1.getData());
    
    // Search "key1"
    const res1 = ptr.search("key1", cmpDefault);
    try std.testing.expect(res1.match);
    try std.testing.expectEqual(@as(u16, 0), res1.index);
    
    // Search "key2" (should be index 1)
    const res2 = ptr.search("key2", cmpDefault);
    try std.testing.expect(!res2.match);
    try std.testing.expectEqual(@as(u16, 1), res2.index);
    
    // Insert "key2" -> "val2" at index 1
    _ = ptr.putNode(1, "key2", "val2", 0);
    
    // Verify order
    try std.testing.expectEqual(@as(u16, 2), ptr.getNumEntries());
    const n2 = ptr.getNode(1);
    try std.testing.expectEqualStrings("key2", n2.getKey());
    
    // Insert "key0" -> "val0" at index 0 (should shift others)
    _ = ptr.putNode(0, "key0", "val0", 0);
    
    // Verify order: key0, key1, key2
    try std.testing.expectEqual(@as(u16, 3), ptr.getNumEntries());
    try std.testing.expectEqualStrings("key0", ptr.getNode(0).getKey());
    try std.testing.expectEqualStrings("key1", ptr.getNode(1).getKey());
    try std.testing.expectEqualStrings("key2", ptr.getNode(2).getKey());
    
    // Verify search finds shifted items
    const res3 = ptr.search("key2", cmpDefault);
    try std.testing.expect(res3.match);
    try std.testing.expectEqual(@as(u16, 2), res3.index);
}

test "PageHeader Copy/Truncate" {
    var buf1: [4096]u8 align(4) = undefined;
    var buf2: [4096]u8 align(4) = undefined;
    const p1 = @as(*PageHeader, @ptrCast(&buf1));
    const p2 = @as(*PageHeader, @ptrCast(&buf2));
    
    p1.init(4096, P_LEAF);
    p2.init(4096, P_LEAF);
    
    // Fill p1
    _ = p1.putNode(0, "A", "ValueA", 0);
    _ = p1.putNode(1, "B", "ValueB", 0);
    _ = p1.putNode(2, "C", "ValueC", 0);
    _ = p1.putNode(3, "D", "ValueD", 0);
    
    try std.testing.expectEqual(@as(u16, 4), p1.getNumEntries());
    
    // Copy last 2 to p2
    // p1: A, B, C, D
    // copy C, D (indices 2, 3) -> count=2
    p1.copyNodes(p2, 2, 2);
    
    try std.testing.expectEqual(@as(u16, 2), p2.getNumEntries());
    try std.testing.expectEqualStrings("C", p2.getNode(0).getKey());
    try std.testing.expectEqualStrings("D", p2.getNode(1).getKey());
    
    // Truncate p1 to 2
    p1.truncate(2);
    try std.testing.expectEqual(@as(u16, 2), p1.getNumEntries());
    try std.testing.expectEqualStrings("A", p1.getNode(0).getKey());
    try std.testing.expectEqualStrings("B", p1.getNode(1).getKey());
}
