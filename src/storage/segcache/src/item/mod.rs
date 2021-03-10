// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

mod header;
mod raw;
mod reserved;

pub use header::ItemHeader;
pub use raw::RawItem;
pub use reserved::ReservedItem;

const KLEN_MASK: u32 = 0x000000FF;
const VLEN_MASK: u32 = 0xFFFFFF00;
const VLEN_SHIFT: u32 = 8;

const OLEN_MASK: u8 = 0b00111111;
const DEL_MASK: u8 = 0b01000000;
const NUM_MASK: u8 = 0b10000000;

pub struct Item {
    pub(crate) cas: u32,
    pub(crate) raw: RawItem,
}

impl Item {
    pub fn key(&self) -> &[u8] {
        self.raw.key()
    }

    pub fn klen(&self) -> u8 {
        self.raw.klen()
    }

    pub fn value(&self) -> &[u8] {
        self.raw.value()
    }

    pub fn check_magic(&self) {
        self.raw.check_magic()
    }

    pub fn deleted(&self) -> bool {
        self.raw.deleted()
    }

    pub fn size(&self) -> usize {
        self.raw.size()
    }

    pub fn cas(&self) -> u32 {
        self.cas
    }

    pub fn optional(&self) -> Option<&[u8]> {
        self.raw.optional()
    }
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Item")
            .field("cas", &self.cas())
            .field("size", &self.size())
            .field("header", self.raw.header())
            .field(
                "raw",
                &format!("{:02X?}", unsafe {
                    &std::slice::from_raw_parts(self.raw.data, self.size())
                }),
            )
            .finish()
    }
}
