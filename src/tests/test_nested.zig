// Copyright (c) 2026 Marcelo Tesla
//
// This software is provided under the MIT License.
// See the LICENSE file at the root of the project for the full text.
//
// SPDX-License-Identifier: MIT
//! Nested Transactions (Sub-Transactions)
//!
//! Cenários cobertos:
//!   1. child commit: dados ficam visíveis na txn pai e após commit da raiz
//!   2. child abort:  dados descartados, pai inalterado
//!   3. múltiplos filhos sequenciais: cada filho vê o estado do pai após anterior
//!   4. filho de filho (aninhamento duplo)
//!   5. child commit preserva dados pré-existentes do pai

const std   = @import("std");
const lib   = @import("../lib.zig");
const Env   = lib.Environment;
const Txn   = lib.Transaction;
const DbFlags = lib.DbFlags;
const TxnFlags = lib.TxnFlags;

const tmp_dir = "test_nested_tmp";

fn openEnv() !Env {
    std.fs.cwd().deleteTree(tmp_dir) catch {};
    try std.fs.cwd().makePath(tmp_dir);
    return Env.open(tmp_dir ++ "/data.db", .{}, 16, 64 * 1024 * 1024);
}

// ─── Teste 1: child commit — dados visíveis no pai ──────────────────────────

test "nested: child commit propagates to parent" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();

    const dbi = try par.openDbi("t1", .{ .create = true });

    // Criar filho
    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();

    const dbi_c = try child.openDbi("t1", .{});
    try child.put(dbi_c, "hello", "world", .{});
    try child.commit();

    // Após commit do filho, dado deve estar visível no pai
    const v = (try par.get(dbi, "hello")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("world", v);

    try par.commit();

    // Verificar após commit da raiz
    var rtxn = try Txn.begin(&env, null, .{ .rdonly = true });
    defer rtxn.abort();
    const rdbi = try rtxn.openDbi("t1", .{});
    const rv = (try rtxn.get(rdbi, "hello")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("world", rv);
}

// ─── Teste 2: child abort — pai inalterado ──────────────────────────────────

test "nested: child abort discards changes" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();

    const dbi = try par.openDbi("t2", .{ .create = true });
    try par.put(dbi, "existing", "value", .{});

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();

    const dbi_c = try child.openDbi("t2", .{});
    try child.put(dbi_c, "temp", "data", .{});
    child.abort();

    // "temp" não deve existir no pai
    const miss = try par.get(dbi, "temp");
    try std.testing.expect(miss == null);

    // "existing" deve continuar presente
    const v = (try par.get(dbi, "existing")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("value", v);

    par.abort();
}

// ─── Teste 3: múltiplos filhos sequenciais ──────────────────────────────────

test "nested: sequential children see cumulative state" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();

    const dbi = try par.openDbi("t3", .{ .create = true });

    // Filho 1: insere "a"
    {
        var c1 = try Txn.begin(&env, &par, .{});
        const d = try c1.openDbi("t3", .{});
        try c1.put(d, "a", "1", .{});
        try c1.commit();
    }

    // Filho 2: deve ver "a" e insere "b"
    {
        var c2 = try Txn.begin(&env, &par, .{});
        const d = try c2.openDbi("t3", .{});
        const va = (try c2.get(d, "a")) orelse return error.NotFound;
        try std.testing.expectEqualStrings("1", va);
        try c2.put(d, "b", "2", .{});
        try c2.commit();
    }

    // Pai vê "a" e "b"
    const va = (try par.get(dbi, "a")) orelse return error.NotFound;
    const vb = (try par.get(dbi, "b")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("1", va);
    try std.testing.expectEqualStrings("2", vb);

    try par.commit();
}

// ─── Teste 4: filho de filho (aninhamento duplo) ─────────────────────────────

test "nested: double nesting" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    var root = try Txn.begin(&env, null, .{});
    errdefer root.abort();

    const dbi = try root.openDbi("t4", .{ .create = true });

    var child = try Txn.begin(&env, &root, .{});
    errdefer child.abort();
    {
        var grandchild = try Txn.begin(&env, &child, .{});
        const d = try grandchild.openDbi("t4", .{});
        try grandchild.put(d, "deep", "value", .{});
        try grandchild.commit();
    }
    // child vê "deep" após grandchild commit
    const dbi_c = try child.openDbi("t4", .{});
    const v = (try child.get(dbi_c, "deep")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("value", v);
    try child.commit();

    // root vê "deep"
    const vr = (try root.get(dbi, "deep")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("value", vr);
    try root.commit();
}

// ─── Teste 5: child commit preserva dados pré-existentes do pai ─────────────

test "nested: child commit preserves parent data" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    // Inserir dados na DB antes
    {
        var t = try Txn.begin(&env, null, .{});
        errdefer t.abort();
        const d = try t.openDbi("t5", .{ .create = true });
        try t.put(d, "pre", "exist", .{});
        try t.commit();
    }

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();
    const dbi = try par.openDbi("t5", .{});

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();
    const dbi_c = try child.openDbi("t5", .{});
    try child.put(dbi_c, "new", "data", .{});
    try child.commit();

    // Ambas as chaves visíveis no pai
    const v1 = (try par.get(dbi, "pre")) orelse return error.NotFound;
    const v2 = (try par.get(dbi, "new")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("exist", v1);
    try std.testing.expectEqualStrings("data", v2);

    try par.commit();
}

// ─── Teste 6: child commit com delete ────────────────────────────────────────

test "nested: child delete propagates" {
    var env = try openEnv();
    defer env.close();
    defer std.fs.cwd().deleteTree(tmp_dir) catch {};

    {
        var t = try Txn.begin(&env, null, .{});
        errdefer t.abort();
        const d = try t.openDbi("t6", .{ .create = true });
        try t.put(d, "gone", "bye", .{});
        try t.put(d, "stay", "here", .{});
        try t.commit();
    }

    var par = try Txn.begin(&env, null, .{});
    errdefer par.abort();
    const dbi = try par.openDbi("t6", .{});

    var child = try Txn.begin(&env, &par, .{});
    errdefer child.abort();
    const dbi_c = try child.openDbi("t6", .{});
    try child.del(dbi_c, "gone", null);
    try child.commit();

    // "gone" removido, "stay" intacto
    try std.testing.expect((try par.get(dbi, "gone")) == null);
    const v = (try par.get(dbi, "stay")) orelse return error.NotFound;
    try std.testing.expectEqualStrings("here", v);

    try par.commit();
}
