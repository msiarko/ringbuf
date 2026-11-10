const Allocator = @import("std").mem.Allocator;

pub const Error = error{ InvalidCapacity, BufferIsFull } || Allocator.Error;
