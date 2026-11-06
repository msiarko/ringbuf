const std = @import("std");
const Atomic = std.atomic.Value;

const cache_line = std.atomic.cache_line;

/// A lock-free Single-Producer Single-Consumer (SPSC) ring buffer with compile-time capacity `N`.
/// Note: Because one slot is used to distinguish full from empty, a buffer of length `N`
/// can hold at most `N - 1` elements.
pub fn StaticRingBuffer(comptime T: type, comptime N: usize) type {
    comptime {
        if (N < 2) @compileError("Buffer capacity N must be >= 2");
    }

    return struct {
        const Self = @This();

        /// Internal field; do not modify directly.
        read: Atomic(usize) align(cache_line) = .init(0),
        /// Internal field; do not modify directly.
        write: Atomic(usize) align(cache_line) = .init(0),
        /// Internal field; do not modify directly.
        buf: [N]T align(cache_line) = undefined,

        /// Default-initialized empty ring buffer.
        pub const init: Self = .{};

        /// Attempts to push `item` into the ring buffer.
        /// Returns `null` on success, or returns `item` back if the buffer is full.
        pub fn push(self: *Self, item: T) ?T {
            const w_idx = self.write.load(.monotonic);
            const next_w_idx = (w_idx + 1) % N;

            if (next_w_idx == self.read.load(.acquire)) {
                return item;
            }

            self.buf[w_idx] = item;
            self.write.store(next_w_idx, .release);
            return null;
        }

        /// Attempts to pop the oldest item from the ring buffer.
        /// Returns the popped item, or `null` if the buffer is empty.
        pub fn pop(self: *Self) ?T {
            const r_idx = self.read.load(.monotonic);
            const w_idx = self.write.load(.acquire);

            if (r_idx == w_idx) {
                return null;
            }

            const item = self.buf[r_idx];
            self.buf[r_idx] = undefined;
            self.read.store((r_idx + 1) % N, .release);
            return item;
        }
    };
}

test "empty buffer tryPop returns null" {
    var rb = StaticRingBuffer(u32, 4).init;
    try std.testing.expectEqual(@as(?u32, null), rb.pop());
    try std.testing.expectEqual(@as(?u32, null), rb.pop());
}

test "minimum capacity N = 2 holds 1 element and wraps around" {
    var rb = StaticRingBuffer(u32, 2).init;

    for (0..10) |cycle| {
        const item: u32 = @intCast(cycle * 10 + 1);
        try std.testing.expectEqual(@as(?u32, null), rb.push(item));
        // Buffer is now full (N - 1 = 1 item); next push must return the rejected item
        try std.testing.expectEqual(@as(?u32, 999), rb.push(999));

        try std.testing.expectEqual(@as(?u32, item), rb.pop());
        try std.testing.expectEqual(@as(?u32, null), rb.pop());
    }
}

test "FIFO order and full buffer rejection" {
    const N = 8;
    var rb = StaticRingBuffer(usize, N).init;

    // Fill to usable capacity (N - 1)
    for (0..N - 1) |i| {
        try std.testing.expectEqual(@as(?usize, null), rb.push(i + 100));
    }

    // Pushing when full returns the item back without overwriting
    try std.testing.expectEqual(@as(?usize, 777), rb.push(777));
    try std.testing.expectEqual(@as(?usize, 888), rb.push(888));

    // Pop all items and verify FIFO order
    for (0..N - 1) |i| {
        try std.testing.expectEqual(@as(?usize, i + 100), rb.pop());
    }

    // Buffer is empty again
    try std.testing.expectEqual(@as(?usize, null), rb.pop());
}

test "interleaved push and pop across multiple wrap-arounds" {
    const N = 5; // usable capacity = 4
    var rb = StaticRingBuffer(u64, N).init;

    var next_write: u64 = 1;
    var next_read: u64 = 1;

    for (0..50) |_| {
        // Push 3 items
        for (0..3) |_| {
            try std.testing.expectEqual(@as(?u64, null), rb.push(next_write));
            next_write += 1;
        }
        // Pop 3 items
        for (0..3) |_| {
            try std.testing.expectEqual(@as(?u64, next_read), rb.pop());
            next_read += 1;
        }
        try std.testing.expectEqual(@as(?u64, null), rb.pop());
    }
}

test "non-primitive element types (structs, slices, optionals, void)" {
    const Message = struct {
        id: u32,
        name: []const u8,
        flag: ?bool,
    };

    var msg_rb = StaticRingBuffer(Message, 4).init;
    const m1: Message = .{ .id = 1, .name = "alpha", .flag = true };
    const m2: Message = .{ .id = 2, .name = "beta", .flag = null };

    try std.testing.expectEqual(@as(?Message, null), msg_rb.push(m1));
    try std.testing.expectEqual(@as(?Message, null), msg_rb.push(m2));

    const popped1 = msg_rb.pop().?;
    try std.testing.expectEqual(@as(u32, 1), popped1.id);
    try std.testing.expectEqualStrings("alpha", popped1.name);
    try std.testing.expectEqual(@as(?bool, true), popped1.flag);

    const popped2 = msg_rb.pop().?;
    try std.testing.expectEqual(@as(u32, 2), popped2.id);
    try std.testing.expectEqualStrings("beta", popped2.name);
    try std.testing.expectEqual(@as(?bool, null), popped2.flag);
    try std.testing.expectEqual(@as(?Message, null), msg_rb.pop());

    // Zero-sized type (void)
    var void_rb = StaticRingBuffer(void, 3).init;
    try std.testing.expectEqual(@as(?void, null), void_rb.push({}));
    try std.testing.expectEqual(@as(?void, null), void_rb.push({}));
    try std.testing.expectEqual(@as(?void, {}), void_rb.push({})); // full
    try std.testing.expectEqual(@as(?void, {}), void_rb.pop());
    try std.testing.expectEqual(@as(?void, {}), void_rb.pop());
    try std.testing.expectEqual(@as(?void, null), void_rb.pop());
}

test "cache-line alignment and field separation" {
    const RB = StaticRingBuffer(u64, 16);
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

test "concurrent SPSC stress test" {
    const ItemCount = 1_000_000;
    const BufferCapacity = 16;
    const TestBuffer = StaticRingBuffer(u64, BufferCapacity);

    const helper = struct {
        fn producerThread(rb: *TestBuffer) void {
            var val: u64 = 0;
            while (val < ItemCount) {
                if (rb.push(val)) |_| {
                    std.atomic.spinLoopHint();
                } else {
                    val += 1;
                }
            }
        }

        fn consumerThread(rb: *TestBuffer, expected_sum: *u64) void {
            var received_count: usize = 0;
            var sum: u64 = 0;

            while (received_count < ItemCount) {
                if (rb.pop()) |val| {
                    sum += val;
                    received_count += 1;
                } else {
                    std.atomic.spinLoopHint();
                }
            }

            expected_sum.* = sum;
        }
    };

    var rb = TestBuffer.init;
    var total_sum: u64 = 0;

    const producer = try std.Thread.spawn(.{}, helper.producerThread, .{&rb});
    const consumer = try std.Thread.spawn(.{}, helper.consumerThread, .{ &rb, &total_sum });

    producer.join();
    consumer.join();

    // Sum of arithmetic progression from 0 to ItemCount - 1
    const expected = (ItemCount - 1) * ItemCount / 2;
    try std.testing.expectEqual(expected, total_sum);
}
