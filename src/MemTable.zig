const MemTable = @This();

fba: std.heap.FixedBufferAllocator,
entries: []Entry,
count: usize = 0,
wal: std.fs.File,

const Entry = struct {
    key: []const u8,
    val: []const u8,
};

const Options = struct {
    walPath: []const u8,
};

pub fn init(buf: []u8, opts: Options) !MemTable {
    var fba = std.heap.FixedBufferAllocator.init(buf);
    const entries = try fba.allocator().alloc(Entry, 512);

    const wal = std.fs.openFileAbsolute(opts.walPath, .{
        .mode = .read_write,
    }) catch |err| switch (err) {
        error.FileNotFound => try std.fs.createFileAbsolute(opts.walPath, .{}),
        else => return err,
    };

    var mt = MemTable{
        .fba = fba,
        .entries = entries,
        .wal = wal,
    };

    mt.replayWal() catch |err| switch (err) {
        error.EndOfStream => {},
        error.NotOpenForReading => {},
        else => return err,
    };

    return mt;
}

fn writeToWal(self: *MemTable, entry: Entry) !void {
    const w = self.wal.writer();

    const keyLen: u32 = @intCast(entry.key.len);
    const valLen: u32 = @intCast(entry.val.len);

    try w.writeInt(u32, keyLen, .little);
    try w.writeAll(entry.key);
    try w.writeInt(u32, valLen, .little);
    try w.writeAll(entry.val);
}

fn replayWal(self: *MemTable) !void {
    const r = self.wal.reader();

    while (true) {
        const keyLen = try r.readInt(u32, .little);
        const key = try self.fba.allocator().alloc(u8, keyLen);
        try r.readNoEof(key);

        const valLen = try r.readInt(u32, .little);
        const val = try self.fba.allocator().alloc(u8, valLen);
        try r.readNoEof(val);

        std.debug.print("replay {s} => {s}\n", .{ key, val });

        try self.put(key, val, .{
            .skipWal = true,
        });
    }
}

pub const PutOptions = struct {
    skipWal: bool = false,
};

pub fn put(self: *MemTable, key: []const u8, val: []const u8, opts: PutOptions) !void {
    if (self.count >= self.entries.len)
        return error.MemTableFull;

    const k = try self.fba.allocator().dupe(u8, key);
    const v = try self.fba.allocator().dupe(u8, val);
    const entry = Entry{ .key = k, .val = v };

    if (!opts.skipWal) {
        try self.writeToWal(entry);
    }

    const pos = self.findInsertPosition(key);

    std.mem.copyBackwards(Entry, self.entries[pos + 1 .. self.count + 1], self.entries[pos..self.count]);

    self.entries[pos] = entry;
    self.count += 1;

    std.debug.print("mem used: {d}/{d}\n", .{ self.fba.end_index, self.fba.buffer.len });
}

fn findInsertPosition(self: *MemTable, key: []const u8) usize {
    var left: usize = 0;
    var right: usize = self.count;
    while (left < right) {
        const mid = (left + right) / 2;
        const cmp = std.mem.order(u8, key, self.entries[mid].key);
        if (cmp == .lt) {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    return left;
}

pub fn get(self: *MemTable, key: []const u8, buf: []u8) !?[]u8 {
    var left: usize = 0;
    var right: usize = self.count;
    while (left < right) {
        const mid = (left + right) / 2;
        const cmp = std.mem.order(u8, key, self.entries[mid].key);
        switch (cmp) {
            .lt => right = mid,
            .gt => left = mid + 1,
            .eq => {
                const val = self.entries[mid].val;
                std.mem.copyForwards(u8, buf, val);
                return buf[0..val.len];
            },
        }
    }
    return null;
}

test "put entries" {
    const buf = try std.testing.allocator.alloc(u8, 64000);
    defer std.testing.allocator.free(buf);

    var mt = try MemTable.init(buf, .{
        .walPath = "/tmp/wal",
    });

    try mt.put("foo", "bar", .{});
    try mt.put("baz", "luhrmann", .{});

    const valBuf = try std.testing.allocator.alloc(u8, 32);
    defer std.testing.allocator.free(valBuf);

    const val = try mt.get("foo", valBuf);
    try std.testing.expectEqualSlices(u8, "bar", val.?);

    const val2 = try mt.get("baz", valBuf);
    try std.testing.expectEqualSlices(u8, "luhrmann", val2.?);
}

const std = @import("std");
