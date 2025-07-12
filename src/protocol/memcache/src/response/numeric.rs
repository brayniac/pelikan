// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::util::{u64_decimal_len, write_u64};

#[derive(Debug, PartialEq, Eq)]
pub struct Numeric {
    value: u64,
    noreply: bool,
}

impl Numeric {
    pub fn new(value: u64, noreply: bool) -> Self {
        Self { value, noreply }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        if self.noreply {
            0
        } else {
            u64_decimal_len(self.value) + 2 // +2 for "\r\n"
        }
    }
}

impl Compose for Numeric {
    fn compose(&self, session: &mut dyn BufMut) -> usize {
        if !self.noreply {
            let mut buf = [0u8; 22]; // Max u64 is 20 digits + "\r\n"
            let len = write_u64(&mut buf, self.value);
            buf[len] = b'\r';
            buf[len + 1] = b'\n';
            let total_len = len + 2;
            session.put_slice(&buf[..total_len]);
            total_len
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(
            response(b"0\r\n"),
            Ok((&b""[..], Response::numeric(0, false),))
        );

        assert_eq!(
            response(b"42 \r\n"),
            Ok((&b""[..], Response::numeric(42, false),))
        );
    }
}
