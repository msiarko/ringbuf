const std = @import("std");
const Atomic = std.atomic.Value;

const cache_line = std.atomic.cache_line;

pub fn StaticRingBuffer(comptime T: type, comptime N: usize) type {
    comptime {
        if (N < 2) @compileError("Buffer capacity N must be >= 2");
    }

    return extern struct {
        const Self = @This();

        read_idx: Atomic(usize) align(cache_line) = .init(0),
        write_idx: Atomic(usize) align(cache_line) = .init(0),
        buf: [N]T align(cache_line) = undefined,

        comptime {
            std.debug.assert(@offsetOf(Self, "write_idx") >= 64);
            std.debug.assert(@offsetOf(Self, "buf") >= 128);
        }

        pub const init: Self = .{};

        pub fn tryPush(self: *Self, item: T) ?T {
            const w_idx = self.write_idx.load(.monotonic);
            const next_w_idx = (w_idx + 1) % N;

            if (next_w_idx == self.read_idx.load(.acquire)) {
                return item;
            }

            self.buf[w_idx] = item;
            self.write_idx.store(next_w_idx, .release);
            return null;
        }

        pub fn tryPop(self: *Self) ?T {
            const r_idx = self.read_idx.load(.monotonic);
            const w_idx = self.write_idx.load(.acquire);

            if (r_idx == w_idx) {
                return null;
            }

            const item = self.buf[r_idx];
            self.buf[r_idx] = undefined;
            self.read_idx.store((r_idx + 1) % N, .release);
            return item;
        }
    };
}

const ItemCount = 1_000_000;
const BufferCapacity = 16;
const TestBuffer = StaticRingBuffer(u64, BufferCapacity);

fn producerThread(rb: *TestBuffer) void {
    var val: u64 = 0;
    while (val < ItemCount) {
        if (rb.tryPush(val)) |_| {
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
        if (rb.tryPop()) |val| {
            sum += val;
            received_count += 1;
        } else {
            std.atomic.spinLoopHint();
        }
    }

    expected_sum.* = sum;
}

test "concurrent SPSC stress test" {
    var rb = TestBuffer.init;
    var total_sum: u64 = 0;

    const producer = try std.Thread.spawn(.{}, producerThread, .{&rb});
    const consumer = try std.Thread.spawn(.{}, consumerThread, .{ &rb, &total_sum });

    producer.join();
    consumer.join();

    // Sum of arithmetic progression from 0 to ItemCount - 1
    const expected = (ItemCount - 1) * ItemCount / 2;
    try std.testing.expectEqual(expected, total_sum);
}
