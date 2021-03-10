use crate::ITEM_HDR_SIZE;
use crate::item::*;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawItem {
    pub(crate) data: *mut u8,
}

impl RawItem {
    pub fn header(&self) -> &ItemHeader {
        unsafe { &*(self.data as *const ItemHeader) }
    }

    pub fn header_mut(&mut self) -> *mut ItemHeader {
        self.data as *mut ItemHeader
    }

    pub fn from_ptr(ptr: *mut u8) -> RawItem {
        Self { data: ptr }
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
        (((ITEM_HDR_SIZE + self.olen() as usize + self.klen() as usize + self.vlen() as usize)
            >> 3)
            + 1)
            << 3
    }

    pub fn tombstone(&mut self) {
        unsafe { (*self.header_mut()).set_deleted(true) }
    }

    pub fn deleted(&self) -> bool {
        self.header().is_deleted()
    }
}

impl std::fmt::Debug for RawItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("RawItem")
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