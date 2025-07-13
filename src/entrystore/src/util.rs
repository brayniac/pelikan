// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

/// Efficiently format a u64 into a byte buffer without allocations.
/// Returns the number of bytes written.
#[inline]
pub fn write_u64(buf: &mut [u8], mut value: u64) -> usize {
    if value == 0 {
        buf[0] = b'0';
        return 1;
    }

    // Write digits in reverse
    let mut pos = 0;
    while value > 0 {
        buf[pos] = b'0' + (value % 10) as u8;
        value /= 10;
        pos += 1;
    }

    // Reverse the digits
    buf[..pos].reverse();
    pos
}
