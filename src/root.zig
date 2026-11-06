const static = @import("static.zig");
const dynamic = @import("dynamic.zig");

pub const StaticRingBuffer = static.StaticRingBuffer;
pub const DynamicRingBuffer = dynamic.DynamicRingBuffer;
pub const Error = dynamic.Error;

test {
    const std = @import("std");
    std.testing.refAllDecls(static);
    std.testing.refAllDecls(dynamic);
}
