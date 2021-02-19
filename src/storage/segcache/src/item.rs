// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use core::sync::atomic::{AtomicU32, Ordering};

const KLEN_MASK: u32 = 0x000000FF;
const VLEN_MASK: u32 = 0xFFFFFF00;
const VLEN_SHIFT: u32 = 8;

const OLEN_MASK: u8 = 0b00111111;
const DEL_MASK: u8 = 0b01000000;
const NUM_MASK: u8 = 0b10000000;

#[repr(C)]
pub struct Item {
    pub(crate) data: *mut u8,
}

// NOTE: repr(packed) is necessary to get the smallest representation. The
// struct is always taken from an aligned pointer cast. This can potentially
// result in UB when fields are referenced. Fields that require access by
// reference must be strategically placed to ensure alignment and avoid UB.
#[repr(C)]
#[repr(packed)]
pub struct ItemHeader {
    #[cfg(feature = "magic")]
    magic: u32,
    len: u32,  // packs vlen:24 klen:8
    flags: u8, // packs is_num:1, deleted:1, olen:6
}

#[cfg(not(feature = "magic"))]
impl std::fmt::Debug for ItemHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("ItemHeader")
            .field("klen", &self.klen())
            .field("vlen", &self.vlen())
            .field("is_num", &self.is_num())
            .field("deleted", &self.is_deleted())
            .field("olen", &self.olen())
            .finish()
    }
}

#[cfg(feature = "magic")]
impl std::fmt::Debug for ItemHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let magic = self.magic;
        f.debug_struct("ItemHeader")
            .field("magic", &format!("0x{:X}", magic))
            .field("klen", &self.klen())
            .field("vlen", &self.vlen())
            .field("is_num", &self.is_num())
            .field("deleted", &self.is_deleted())
            .field("olen", &self.olen())
            .finish()
    }
}

impl ItemHeader {
    // return the magic
    #[cfg(feature = "magic")]
    pub fn magic(&self) -> u32 {
        self.magic
    }

    #[cfg(feature = "magic")]
    pub fn set_magic(&mut self) {
        self.magic = ITEM_MAGIC;
    }

    pub fn check_magic(&self) {
        #[cfg(feature = "magic")]
        assert_eq!(self.magic(), ITEM_MAGIC);
    }

    // key length is the low byte
    pub fn klen(&self) -> u8 {
        (self.len & KLEN_MASK) as u8
    }

    // value length is all but the low byte
    pub fn vlen(&self) -> u32 {
        self.len >> VLEN_SHIFT
    }

    pub fn olen(&self) -> u8 {
        self.flags & OLEN_MASK
    }

    pub fn is_num(&self) -> bool {
        self.flags & NUM_MASK != 0
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & DEL_MASK != 0
    }

    // set the key length by changing just the low byte
    fn set_klen(&mut self, len: u8) {
        self.len = (self.len & !KLEN_MASK) | (len as u32);
    }

    // set the value length by changing just the upper bytes
    // TODO(bmartin): where should we do error handling for out-of-range?
    fn set_vlen(&mut self, len: u32) {
        debug_assert!(len <= (u32::MAX >> VLEN_SHIFT));
        self.len = (self.len & !VLEN_MASK) | (len << VLEN_SHIFT);
    }

    fn set_deleted(&mut self, deleted: bool) {
        if deleted {
            self.flags |= DEL_MASK
        } else {
            self.flags &= !DEL_MASK
        }
    }

    fn set_num(&mut self, num: bool) {
        if num {
            self.flags |= NUM_MASK
        } else {
            self.flags &= !NUM_MASK
        }
    }

    fn set_olen(&mut self, len: u8) {
        debug_assert!(len <= OLEN_MASK);
        self.flags = (self.flags & !OLEN_MASK) | len;
    }
}

impl Item {
    fn header(&self) -> &ItemHeader {
        unsafe { &*(self.data as *const ItemHeader) }
    }

    fn header_mut(&mut self) -> *mut ItemHeader {
        self.data as *mut ItemHeader
    }

    pub fn from_ptr(ptr: *mut u8) -> Item {
        Item { data: ptr }
    }

    #[inline]
    pub fn klen(&self) -> u8 {
        self.header().klen()
    }

    pub fn key(&self) -> &[u8] {
        unsafe {
            let ptr = self.data.add(self.key_offset());
            let len = self.klen() as usize;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    #[inline]
    pub fn vlen(&self) -> u32 {
        self.header().vlen()
    }

    // TODO(bmartin): should probably change this to be Option<>
    pub fn value(&self) -> &[u8] {
        unsafe {
            let ptr = self.data.add(self.value_offset());
            let len = self.vlen() as usize;
            std::slice::from_raw_parts(ptr, len)
        }
    }

    #[inline]
    pub fn olen(&self) -> u8 {
        self.header().olen()
    }

    pub fn optional(&self) -> Option<&[u8]> {
        if self.olen() > 0 {
            unsafe {
                let ptr = self.data.add(self.optional_offset());
                let len = self.olen() as usize;
                Some(std::slice::from_raw_parts(ptr, len))
            }
        } else {
            None
        }
    }

    #[inline]
    pub fn check_magic(&self) {
        self.header().check_magic()
    }

    #[inline]
    pub fn set_magic(&mut self) {
        #[cfg(feature = "magic")]
        unsafe {
            (*self.header_mut()).set_magic()
        }
    }

    pub(crate) fn define(&mut self, key: &[u8], value: &[u8], optional: &[u8]) {
        unsafe {
            self.set_magic();
            (*self.header_mut()).set_deleted(false);
            (*self.header_mut()).set_num(false);
            (*self.header_mut()).set_olen(optional.len() as u8);
            std::ptr::copy_nonoverlapping(
                optional.as_ptr(),
                self.data.add(self.optional_offset()),
                optional.len(),
            );
            (*self.header_mut()).set_klen(key.len() as u8);
            std::ptr::copy_nonoverlapping(
                key.as_ptr(),
                self.data.add(self.key_offset()),
                key.len(),
            );
            (*self.header_mut()).set_vlen(value.len() as u32);
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                self.data.add(self.value_offset()),
                value.len(),
            );
        }
    }

    #[inline]
    fn optional_offset(&self) -> usize {
        ITEM_HDR_SIZE
    }

    #[inline]
    fn key_offset(&self) -> usize {
        self.optional_offset() + self.olen() as usize
    }

    #[inline]
    fn value_offset(&self) -> usize {
        self.key_offset() + self.klen() as usize
    }

    // NOTE: size is rounded up for alignment
    pub fn size(&self) -> usize {
        (((ITEM_HDR_SIZE + self.klen() as usize + self.vlen() as usize) >> 3) + 1) << 3
    }

    pub fn tombstone(&mut self) {
        unsafe { (*self.header_mut()).set_deleted(true) }
    }

    pub fn deleted(&self) -> bool {
        self.header().is_deleted()
    }
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Item")
            .field("size", &self.size())
            .field("header", self.header())
            .field(
                "raw",
                &format!("{:02X?}", unsafe {
                    &std::slice::from_raw_parts(self.data, self.size())
                }),
            )
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct ReservedItem {
    pub(crate) item: Item,
    pub(crate) seg: i32,
    pub(crate) offset: usize,
}
