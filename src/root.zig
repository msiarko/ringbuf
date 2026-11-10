const static = @import("static.zig");
const dynamic = @import("dynamic.zig");
const @"error" = @import("error.zig");
const Allocator = @import("std").mem.Allocator;

pub const Error = @"error".Error;
pub const StaticRingBuffer = static.StaticRingBuffer;
pub const DynamicRingBuffer = dynamic.DynamicRingBuffer;

test {
    const std = @import("std");
    std.testing.refAllDecls(static);
    std.testing.refAllDecls(dynamic);
}
