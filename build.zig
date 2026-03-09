const std = @import("std");

pub fn build(b: *std.Build) void {
    const target   = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Public Zig module — imported by consumers as a path or registry dependency.
    const monolith_mod = b.addModule("monolith", .{
        .root_source_file = b.path("src/lib.zig"),
        .target           = target,
        .optimize         = optimize,
    });

    // Installable static library artifact for standalone use.
    const lib = b.addLibrary(.{
        .linkage     = .static,
        .name        = "monolith",
        .root_module = monolith_mod,
    });
    b.installArtifact(lib);

    // Tests
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target           = target,
        .optimize         = optimize,
    });

    const unit_tests = b.addTest(.{ .root_module = test_mod });
    const run_tests  = b.addRunArtifact(unit_tests);
    const test_step  = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
