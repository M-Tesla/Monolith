const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Public Zig module — imported by consumers as a path or registry dependency.
    const monolith_mod = b.addModule("monolith", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Installable static library artifact for standalone use.
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "monolith",
        .root_module = monolith_mod,
    });
    b.installArtifact(lib);

    const basic_example = b.addExecutable(.{
        .name = "basic-example",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/basic-example.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    basic_example.root_module.addImport("monolith", monolith_mod);
    b.installArtifact(basic_example);

    const run_example_cmd = b.addRunArtifact(basic_example);
    run_example_cmd.step.dependOn(b.getInstallStep());

    const run_example_step = b.step("run-basic_example", "Run the basic example");
    run_example_step.dependOn(&run_example_cmd.step);

    // Tests
    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{ .root_module = test_mod });
    const run_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);
}
