const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lz4_mod = b.addModule("lz4", .{
        .root_source_file = b.path("zig-lz4/src/root.zig"),
        .target           = target,
        .optimize         = optimize,
    });

    const monolith_mod = b.addModule("monolith", .{
        .root_source_file = b.path("src/lib.zig"),
        .target           = target,
        .optimize         = optimize,
        .link_libc        = true,
    });
    monolith_mod.addImport("lz4", lz4_mod);

    const lib = b.addLibrary(.{
        .linkage     = .static,
        .name        = "monolith",
        .root_module = monolith_mod,
    });
    b.installArtifact(lib);

    // ---- Tests: zig build test ---------------------------------------------
    const unit_tests = b.addTest(.{ .root_module = monolith_mod });
    const run_tests  = b.addRunArtifact(unit_tests);
    const test_step  = b.step("test", "Run the Monolith test suite");
    test_step.dependOn(&run_tests.step);
}
