const std = @import("std");
const monolith = @import("monolith");
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

    std.debug.print("{s}\n", .{val orelse ""});
}
