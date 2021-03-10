// item info masks
pub const TAG_MASK: u64 = 0xFFF0_0000_0000_0000;
pub const FREQ_MASK: u64 = 0x000F_F000_0000_0000;
pub const SEG_ID_MASK: u64 = 0x0000_0FFF_FFF0_0000;
pub const OFFSET_MASK: u64 = 0x0000_0000_000F_FFFF;

// consts for frequency
pub const CLEAR_FREQ_SMOOTH_MASK: u64 = 0xFFF7_FFFF_FFFF_FFFF;

// bucket info masks and shifts
pub const LOCK_MASK: u64 = 0xFF00_0000_0000_0000;
pub const BUCKET_CHAIN_LEN_MASK: u64 = 0x00FF_0000_0000_0000;
pub const TS_MASK: u64 = 0x0000_FFFF_0000_0000;
pub const CAS_MASK: u64 = 0x0000_0000_FFFF_FFFF;

pub const TS_BIT_SHIFT: u64 = 32;
pub const FREQ_BIT_SHIFT: u64 = 44;

// only use the lower 16-bits of the timestamp
pub const PROC_TS_MASK: u64 = 0x0000_0000_0000_FFFF;

// item constants
pub const ITEM_HDR_SIZE: usize = std::mem::size_of::<crate::item::ItemHeader>();
pub const ITEM_MAGIC: u32 = 0xDECAFBAD;
pub const ITEM_MAGIC_SIZE: usize = std::mem::size_of::<u32>();

// segment constants
pub const SEG_MAGIC: u64 = 0xBADC0FFEEBADCAFE;

// TODO(bmartin): consider making this a newtype so that we're able to enforce
// how ThinOption is used through the type system. Currently, we can still do
// numeric comparisons.

// A super thin option type that can be used with reduced-range integers. For
// instance, we can treat signed types < 0 as a None variant. This could also
// be used to wrap unsigned types by reducing the representable range by one bit
pub trait ThinOption: Sized {
    fn is_some(&self) -> bool;
    fn is_none(&self) -> bool;
    fn as_option(&self) -> Option<Self>;
}

// We're currently only using i32
impl ThinOption for i32 {
    fn is_some(&self) -> bool {
        *self >= 0
    }

    fn is_none(&self) -> bool {
        *self < 0
    }

    fn as_option(&self) -> Option<Self> {
        if self.is_some() {
            Some(*self)
        } else {
            None
        }
    }
}

#[inline]
pub const fn tag_from_hash(hash: u64) -> u64 {
    (hash & TAG_MASK) | 0x0010000000000000
}

#[inline]
pub const fn get_offset(item_info: u64) -> u64 {
    (item_info & OFFSET_MASK) << 3
}

#[inline]
pub const fn get_seg_id(item_info: u64) -> i32 {
    ((item_info & SEG_ID_MASK) >> 20) as i32
}

#[inline]
pub const fn get_freq(item_info: u64) -> u64 {
    (item_info & FREQ_MASK) >> 44
}

#[inline]
pub const fn get_cas(bucket_info: u64) -> u32 {
    (bucket_info & CAS_MASK) as u32
}

#[inline]
pub const fn get_ts(bucket_info: u64) -> u64 {
    bucket_info & TS_MASK
}

#[inline]
pub const fn get_tag(item_info: u64) -> u64 {
    item_info & TAG_MASK
}

#[inline]
pub const fn clear_freq(item_info: u64) -> u64 {
    item_info & !FREQ_MASK
}

#[inline]
pub const fn chain_len(bucket_info: u64) -> u64 {
    (bucket_info & BUCKET_CHAIN_LEN_MASK) >> 20
}

#[inline]
pub const fn build_item_info(tag: u64, seg_id: u64, offset: u64) -> u64 {
    tag | (seg_id << 20) | (offset >> 3)
}

