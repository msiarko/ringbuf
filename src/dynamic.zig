const std = @import("std");
const Atomic = std.atomic.Value;
const cache_line = std.atomic.cache_line;
const Allocator = std.mem.Allocator;
const Error = @import("error.zig").Error;

/// A lock-free Single-Producer Single-Consumer (SPSC) ring buffer with runtime capacity.
/// Note: Because one slot is used to distinguish full from empty, a buffer of length `cap`
/// can hold at most `cap - 1` elements.
pub fn DynamicRingBuffer(comptime T: type) type {
    return struct {
        const Self = @This();

        /// Internal field; do not modify directly.
        read: Atomic(usize) align(cache_line) = .init(0),
        /// Internal field; do not modify directly.
        write: Atomic(usize) align(cache_line) = .init(0),
        /// Internal field; do not modify directly.
        buf: []T align(cache_line),

        /// Initializes a ring buffer backed by an existing slice `buf` (`buf.len` must be >= 2).
        /// The caller retains ownership of `buf`; do not call `deinit` on an instance created with `init`.
        pub fn init(buf: []T) Error!Self {
            if (buf.len < 2) return Error.InvalidCapacity;
            return .{ .buf = buf };
        }

        /// Allocates a backing buffer of length `cap` (`cap` must be >= 2) using `gpa`.
        /// Returns `Error.InvalidCapacity` when `cap` is less than 2
        /// Call `deinit` with the same allocator to free the backing memory.
        pub fn initAlloc(gpa: Allocator, cap: usize) Error!Self {
            if (cap < 2) return Error.InvalidCapacity;
            return .{ .buf = try gpa.alloc(T, cap) };
        }

        /// Frees the backing buffer allocated by `initAlloc` and invalidates `self`.
        /// Must only be called on instances initialized via `initAlloc`.
        pub fn deinit(self: *Self, gpa: Allocator) void {
            gpa.free(self.buf);
            self.* = undefined;
        }

        /// Attempts to push `item` into the ring buffer.
        /// Returns `Error.BufferIsFull` back if the buffer is full.
        pub fn push(self: *Self, item: T) Error!void {
            const w_idx = self.write.load(.monotonic);
            const next_w_idx = (w_idx + 1) % self.buf.len;

            if (next_w_idx == self.read.load(.acquire))
                return Error.BufferIsFull;

            self.buf[w_idx] = item;
            self.write.store(next_w_idx, .release);
        }

        /// Attempts to pop the oldest item from the ring buffer.
        /// Returns the popped item, or `null` if the buffer is empty.
        pub fn pop(self: *Self) ?T {
            const r_idx = self.read.load(.monotonic);
            const w_idx = self.write.load(.acquire);

            if (r_idx == w_idx) return null;

            const item = self.buf[r_idx];
            self.buf[r_idx] = undefined;
            self.read.store((r_idx + 1) % self.buf.len, .release);
            return item;
        }
    };
}

test "init with valid slice capacities" {
    var min_buf: [2]u8 = undefined;
    const min_dyn = try DynamicRingBuffer(u8).init(&min_buf);
    try std.testing.expectEqual(@as(usize, 2), min_dyn.buf.len);
    try std.testing.expectEqual(@as([*]u8, &min_buf), min_dyn.buf.ptr);
    try std.testing.expectEqual(@as(usize, 0), min_dyn.read.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), min_dyn.write.load(.monotonic));

    var buf: [12]u8 = undefined;
    const dyn = try DynamicRingBuffer(u8).init(&buf);
    try std.testing.expectEqual(@as(usize, 12), dyn.buf.len);
    try std.testing.expectEqual(@as([*]u8, &buf), dyn.buf.ptr);
}

test "init rejects capacities smaller than 2" {
    var empty_buf: [0]u8 = .{};
    try std.testing.expectError(
        Error.InvalidCapacity,
        DynamicRingBuffer(u8).init(&empty_buf),
    );

    var one_buf: [1]u8 = undefined;
    try std.testing.expectError(
        Error.InvalidCapacity,
        DynamicRingBuffer(u8).init(&one_buf),
    );
}

test "initAlloc with valid capacities and deinit" {
    var min_dyn = try DynamicRingBuffer(u8).initAlloc(std.testing.allocator, 2);
    defer min_dyn.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), min_dyn.buf.len);
    try std.testing.expectEqual(@as(usize, 0), min_dyn.read.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), min_dyn.write.load(.monotonic));

    var dyn = try DynamicRingBuffer(u8).initAlloc(std.testing.allocator, 12);
    defer dyn.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 12), dyn.buf.len);
}

test "initAlloc rejects capacities smaller than 2" {
    try std.testing.expectError(
        Error.InvalidCapacity,
        DynamicRingBuffer(u8).initAlloc(std.testing.allocator, 0),
    );
    try std.testing.expectError(
        Error.InvalidCapacity,
        DynamicRingBuffer(u8).initAlloc(std.testing.allocator, 1),
    );
}

test "initAlloc propagates OutOfMemory" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        Error.OutOfMemory,
        DynamicRingBuffer(u64).initAlloc(failing.allocator(), 8),
    );
}

test "non-primitive element types (structs, slices, optionals, void)" {
    const Event = struct {
        code: u16,
        payload: []const u8,
        extra: ?u64,
    };

    var ev_dyn = try DynamicRingBuffer(Event).initAlloc(std.testing.allocator, 4);
    defer ev_dyn.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), ev_dyn.buf.len);

    var void_dyn = try DynamicRingBuffer(void).initAlloc(std.testing.allocator, 4);
    defer void_dyn.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), void_dyn.buf.len);
}

test "cache-line alignment and field separation" {
    const RB = DynamicRingBuffer(u64);
    const r_off = @offsetOf(RB, "read");
    const w_off = @offsetOf(RB, "write");
    const b_off = @offsetOf(RB, "buf");

    try std.testing.expectEqual(@as(usize, 0), r_off % cache_line);
    try std.testing.expectEqual(@as(usize, 0), w_off % cache_line);
    try std.testing.expectEqual(@as(usize, 0), b_off % cache_line);

    try std.testing.expect(r_off != w_off);
    try std.testing.expect(r_off != b_off);
    try std.testing.expect(w_off != b_off);
}
