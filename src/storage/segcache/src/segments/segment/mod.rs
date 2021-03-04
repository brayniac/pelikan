use super::SegmentHeader;
use crate::*;

pub struct Segment<'a> {
    pub(crate) header: &'a mut SegmentHeader,
    pub(crate) data: &'a mut [u8],
}

impl<'a> Segment<'a> {
    pub fn init(&mut self) {
        if cfg!(feature = "magic") {
            for (i, byte) in SEG_MAGIC.to_be_bytes().iter().enumerate() {
                self.data[i] = *byte;
            }
        }
        self.header.init();
    }

    #[cfg(feature = "magic")]
    #[inline]
    pub fn magic(&self) -> u64 {
        u64::from_be_bytes([
            self.data[0],
            self.data[1],
            self.data[2],
            self.data[3],
            self.data[4],
            self.data[5],
            self.data[6],
            self.data[7],
        ])
    }

    #[inline]
    pub fn check_magic(&self) {
        #[cfg(feature = "magic")]
        assert_eq!(self.magic(), SEG_MAGIC)
    }

    fn max_item_offset(&self) -> usize {
        if self.header.write_offset() >= ITEM_HDR_SIZE as i32 {
            std::cmp::min(self.header.write_offset() as usize, self.data.len()) - ITEM_HDR_SIZE
        } else if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        }
    }

    pub fn check_integrity(&mut self) -> bool {
        self.check_magic();

        let mut integrity = true;

        let max_offset = self.max_item_offset();
        let mut offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        let mut count = 0;

        while offset < max_offset {
            let item = Item::from_ptr(unsafe { self.data.as_mut_ptr().add(offset) });
            if item.klen() == 0 {
                break;
            }
            if !item.deleted() {
                count += 1;
            }
            offset += item.size();
        }

        if count != self.n_item() {
            error!(
                "seg: {} has mismatch between counted items: {} and header items: {}",
                self.id(),
                count,
                self.n_item()
            );
            integrity = false;
        }

        integrity
    }

    pub fn n_item(&self) -> i32 {
        self.header.n_item()
    }

    pub fn accessible(&self) -> bool {
        self.header.accessible()
    }

    pub fn id(&self) -> i32 {
        self.header.id()
    }

    pub fn create_at(&self) -> CoarseInstant {
        self.header.create_at()
    }

    pub fn ttl(&self) -> CoarseDuration {
        self.header.ttl()
    }

    pub fn next_seg(&self) -> Option<i32> {
        self.header.next_seg()
    }

    pub(crate) fn remove_item(&mut self, item_info: u64, tombstone: bool) {
        let offset = get_offset(item_info) as usize;
        self.remove_item_at(offset, tombstone)
    }

    pub(crate) fn remove_item_at(&mut self, offset: usize, _tombstone: bool) {
        let mut item = self.get_item_at(offset).unwrap();
        if item.deleted() {
            return;
        }

        self.check_magic();
        self.header.decr_occupied_size(item.size() as i32);
        self.header.decr_n_item();
        assert!(self.header.occupied_size() >= 0);
        assert!(self.header.n_item() >= 0);
        item.tombstone();
        // if tombstone {
        //     item.tombstone();
        // }

        self.check_magic();
    }

    // returns the item looking it up from the item_info
    // TODO(bmartin): consider changing the return type here and removing asserts?
    pub(crate) fn get_item(&mut self, item_info: u64) -> Option<Item> {
        assert_eq!(get_seg_id(item_info) as i32, self.id());
        self.get_item_at(get_offset(item_info) as usize)
    }

    // returns the item at the given offset
    // TODO(bmartin): consider changing the return type here and removing asserts?
    pub(crate) fn get_item_at(&mut self, offset: usize) -> Option<Item> {
        assert!(offset <= self.max_item_offset());
        Some(Item::from_ptr(unsafe {
            self.data.as_mut_ptr().add(offset)
        }))
    }

    pub(crate) fn clear<S: BuildHasher>(
        &mut self,
        hashtable: &mut HashTable<S>,
        expire: bool,
    ) -> Result<(), ()> {
        self.header.set_accessible(false);
        self.header.set_evictable(false);

        let max_offset = self.max_item_offset();
        let mut offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC)
        } else {
            0
        };

        while offset <= max_offset {
            let item = self.get_item_at(offset).unwrap();
            if item.klen() == 0 && self.header.n_item() == 0 {
                break;
            }

            item.check_magic();

            debug_assert!(item.klen() > 0, "invalid klen: ({})", item.klen());

            if !item.deleted() {
                trace!("evicting from hashtable");
                if !hashtable.evict(item.key(), offset.try_into().unwrap(), self) {
                    // this *shouldn't* happen, but to keep header integrity, we
                    // warn and remove the item even if it wasn't in the
                    // hashtable
                    warn!("unlinked item was present in segment");
                    self.remove_item_at(offset, true);
                }
            }

            debug_assert!(
                self.header.n_item() >= 0,
                "cleared segment has invalid n_item: ({})",
                self.header.n_item()
            );
            debug_assert!(
                self.header.occupied_size() >= 0,
                "cleared segment has invalid occupied_size: ({})",
                self.header.occupied_size()
            );
            offset += item.size();
        }

        // skips over seg_wait_refcount and evict retry, because no threading

        if self.n_item() != 0 {
            println!("segment has items after clearing");
            // if !self.check_integrity() {
            //     println!("segment failed integrity check");
            // } else {
            //     println!("segment passed integrity check");
            // }
            // println!("segment is: {:?}", self);
            assert_eq!(self.n_item(), 0);
        }

        let expected_size = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC) as i32
        } else {
            0
        };
        if self.header.occupied_size() != expected_size {
            println!("segment size unexpected!");
            // if !self.check_integrity() {
            //     println!("segment failed integrity check");
            // } else {
            //     println!("segment passed integrity check");
            // }
            // println!("segment is: {:?}", self);
            assert_eq!(self.header.occupied_size(), expected_size)
        }

        self.header.set_write_offset(self.header.occupied_size());

        #[allow(clippy::if_same_then_else)]
        if expire {
            increment_counter!(&Stat::SegmentExpire)
        } else {
            increment_counter!(&Stat::SegmentEvict)
        }

        Ok(())
    }
}

#[cfg(feature = "magic")]
impl<'a> std::fmt::Debug for Segment<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Segment")
            .field("header", &self.header)
            .field("magic", &format!("0x{:X}", self.magic()))
            .field("data", &format!("{:02X?}", self.data))
            .finish()
    }
}

#[cfg(not(feature = "magic"))]
impl<'a> std::fmt::Debug for Segment<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Segment")
            .field("header", &self.header)
            .field("data", &format!("{:X?}", self.data))
            .finish()
    }
}
