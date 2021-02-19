use crate::common::ThinOption;
use crate::SEG_MAGIC;
use rustcommon_time::*;

pub const SEG_HDR_SIZE: usize = std::mem::size_of::<SegmentHeader>();

#[derive(Debug, Clone, Copy)]
#[repr(C)]
/// The `SegmentHeader` contains metadata about the segment. It is intended to
/// be stored in DRAM as the fields are frequently accessed and changed.
pub struct SegmentHeader {
    id: i32,
    write_offset: i32,
    occupied_size: i32,
    n_item: i32,
    prev_seg: i32,
    next_seg: i32,
    w_ref: i16,
    r_ref: i16,
    n_hit: i32,
    n_active_items: i32,
    n_active_bytes: i32,
    last_merge_epoch: i16,
    create_at: CoarseInstant,
    ttl: CoarseDuration,
    merge_at: CoarseInstant,
    accessible: u8,
    evictable: u8,
    recovered: u8,
    unused: u16,
}

impl SegmentHeader {
    pub fn new(id: i32) -> Self {
        Self {
            id,
            write_offset: 0,
            occupied_size: 0,
            n_item: 0,
            prev_seg: -1,
            next_seg: -1,
            w_ref: 0,
            r_ref: 0,
            n_hit: 0,
            n_active_items: 0,
            n_active_bytes: 0,
            last_merge_epoch: -1,
            create_at: recent_coarse!(),
            ttl: CoarseDuration::ZERO,
            merge_at: recent_coarse!(),
            accessible: 0,
            evictable: 0,
            recovered: 0,
            unused: 0,
        }
    }

    pub fn init(&mut self) {
        // TODO(bmartin): should these be `debug_assert` or are we enforcing
        // invariants? Eitherway, keeping them before changing values in the
        // header is probably wise?
        assert!(!self.accessible());
        assert!(!self.evictable());

        self.reset();

        self.prev_seg = -1;
        self.next_seg = -1;
        self.n_item = 0;
        self.create_at = recent_coarse!();
        self.merge_at = recent_coarse!();
        self.accessible = 1;
        self.n_hit = 0;
        self.last_merge_epoch = 0;
        self.n_active_items = 0;
        self.n_active_bytes = 0;
    }

    // TODO(bmartin): maybe have some debug_assert for n_item == 0 ?
    pub fn reset(&mut self) {
        self.write_offset = if cfg!(feature = "magic") {
            std::mem::size_of_val(&SEG_MAGIC) as i32
        } else {
            0
        };

        self.occupied_size = self.write_offset;
    }

    #[inline]
    pub fn id(&self) -> i32 {
        self.id
    }

    #[inline]
    pub fn write_offset(&self) -> i32 {
        self.write_offset
    }

    #[inline]
    pub fn set_write_offset(&mut self, bytes: i32) {
        self.write_offset = bytes
    }

    #[inline]
    pub fn incr_write_offset(&mut self, bytes: i32) -> i32 {
        let prev = self.write_offset;
        self.write_offset += bytes;
        prev
    }

    #[inline]
    pub fn accessible(&self) -> bool {
        self.accessible != 0
    }

    #[inline]
    pub fn set_accessible(&mut self, accessible: bool) {
        if accessible {
            self.accessible = 1
        } else {
            self.accessible = 0
        }
    }

    #[inline]
    pub fn evictable(&self) -> bool {
        self.evictable != 0
    }

    #[inline]
    pub fn set_evictable(&mut self, evictable: bool) {
        if evictable {
            self.evictable = 1
        } else {
            self.evictable = 0
        }
    }

    #[inline]
    pub fn n_item(&self) -> i32 {
        self.n_item
    }

    #[inline]
    pub fn incr_n_item(&mut self) {
        self.n_item += 1;
    }

    #[inline]
    pub fn decr_n_item(&mut self) {
        self.n_item -= 1;
    }

    #[inline]
    pub fn ttl(&self) -> CoarseDuration {
        self.ttl
    }

    #[inline]
    pub fn set_ttl(&mut self, ttl: CoarseDuration) {
        self.ttl = ttl
    }

    #[inline]
    pub fn occupied_size(&self) -> i32 {
        self.occupied_size
    }

    #[inline]
    pub fn incr_occupied_size(&mut self, bytes: i32) -> i32 {
        let prev = self.occupied_size;
        self.occupied_size += bytes;
        prev
    }

    #[inline]
    pub fn decr_occupied_size(&mut self, bytes: i32) -> i32 {
        let prev = self.occupied_size;
        self.occupied_size -= bytes;
        prev
    }

    #[inline]
    pub fn prev_seg(&self) -> Option<i32> {
        self.prev_seg.as_option()
    }

    #[inline]
    pub fn set_prev_seg(&mut self, id: i32) {
        self.prev_seg = id;
    }

    #[inline]
    pub fn next_seg(&self) -> Option<i32> {
        self.next_seg.as_option()
    }

    #[inline]
    pub fn set_next_seg(&mut self, id: i32) {
        self.next_seg = id;
    }

    #[inline]
    pub fn w_ref(&self) -> i16 {
        self.w_ref
    }

    #[inline]
    pub fn incr_w_ref(&mut self) {
        self.w_ref += 1
    }

    #[inline]
    pub fn decr_w_ref(&mut self) {
        self.w_ref += 1
    }

    #[inline]
    // TODO(bmartin): should this be `created` instead?
    pub fn create_at(&self) -> CoarseInstant {
        self.create_at
    }

    #[inline]
    pub fn merge_at(&self) -> CoarseInstant {
        self.merge_at
    }

    #[inline]
    pub fn can_evict(&self) -> bool {
        self.w_ref() == 0
            && self.evictable()
            && (self.create_at() + self.ttl()) >= recent_coarse!() + CoarseDuration::from_secs(5)
            && self.next_seg().is_some()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(SEG_HDR_SIZE, 64);
        assert_eq!(std::mem::size_of::<SegmentHeader>(), SEG_HDR_SIZE)
    }
}
