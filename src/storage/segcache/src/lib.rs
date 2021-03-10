// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate rustcommon_logger;

#[macro_use]
extern crate metrics;

use crate::rand::*;
use metrics::Stat;
use rustcommon_time::*;

use core::hash::BuildHasher;
use std::convert::TryInto;
use std::hash::Hasher;

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
pub const ITEM_HDR_SIZE: usize = std::mem::size_of::<ItemHeader>();
pub const ITEM_MAGIC: u32 = 0xDECAFBAD;
pub const ITEM_MAGIC_SIZE: usize = std::mem::size_of::<u32>();

// segment constants
pub const SEG_MAGIC: u64 = 0xBADC0FFEEBADCAFE;

mod common;
mod item;
mod rand;
mod segments;
mod ttl_buckets;

pub(crate) use item::*;
pub(crate) use segments::*;
pub(crate) use ttl_buckets::*;

pub struct SegCache<S: std::hash::BuildHasher> {
    hashtable: HashTable<S>,
    segments: Segments,
    ttl_buckets: TtlBuckets,
    start: CoarseInstant,
}

pub enum SegCacheError {
    HashTableInsertEx,
    EvictionEx,
    ItemOversized,
    NoFreeSegments,
    Exists,
    NotFound,
}

impl<S: std::hash::BuildHasher> SegCache<S> {
    pub fn new(power: u8, hash_builder: S, segments: i32, seg_size: i32) -> Self {
        let hashtable = HashTable::with_hasher(power, hash_builder);

        let evict_policy = Policy::Random;
        let segments = Segments::builder()
            .seg_size(seg_size)
            .segments(segments)
            .eviction_policy(evict_policy)
            .build();
        let ttl_buckets = TtlBuckets::default();
        Self {
            hashtable,
            segments,
            ttl_buckets,
            start: recent_coarse!(),
        }
    }

    pub fn items(&mut self) -> usize {
        trace!("getting segment item counts");
        self.segments.items()
    }

    fn reserve_item(
        &mut self,
        key: &[u8],
        value: &[u8],
        optional: &[u8],
        ttl: CoarseDuration,
    ) -> Result<ReservedItem, SegCacheError> {
        let ttl_bucket = self.ttl_buckets.get_mut_bucket(ttl);
        let size = (((ITEM_HDR_SIZE + key.len() + value.len() + optional.len()) >> 3) + 1) << 3;

        match ttl_bucket.reserve(size, &mut self.segments) {
            Ok(mut reserved_item) => {
                reserved_item.define(key, value, optional);
                Ok(reserved_item)
            }
            Err(TtlBucketError::ItemOversized) => Err(SegCacheError::ItemOversized),
            Err(TtlBucketError::NoFreeSegments) => Err(SegCacheError::NoFreeSegments),
        }
    }

    // TODO(bmartin): cas and incr_ref
    pub fn get(&mut self, key: &[u8]) -> Option<Item> {
        self.hashtable.get(key, &mut self.segments)
    }

    fn insert_reserved(&mut self, reserved: ReservedItem) -> Result<(), SegCacheError> {
        reserved.check_magic();
        let seg_id = reserved.seg();
        let offset = reserved.offset();
        if self
            .hashtable
            .insert(reserved.item(), seg_id, offset as u64, &mut self.segments)
            .is_err()
        {
            let _ = self.segments.remove_at(seg_id, offset, false);
            Err(SegCacheError::HashTableInsertEx)
        } else {
            Ok(())
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn insert(
        &mut self,
        key: &[u8],
        value: &[u8],
        optional: Option<&[u8]>,
        ttl: CoarseDuration,
    ) -> Result<(), SegCacheError> {
        let optional = optional.unwrap_or(&[]);

        // we handle a failure to reserve the item by evicting a segment and
        // retrying
        match self.reserve_item(key, value, optional, ttl) {
            Ok(reserved) => self.insert_reserved(reserved),
            Err(SegCacheError::NoFreeSegments) => {
                if let Some(id) = self.segments.least_valuable_seg() {
                    if self.evict_segment(id).is_ok() {
                        let reserved = self.reserve_item(key, value, optional, ttl)?;
                        self.insert_reserved(reserved)
                    } else {
                        Err(SegCacheError::EvictionEx)
                    }
                } else {
                    // TODO(bmartin): think about these error types more
                    Err(SegCacheError::EvictionEx)
                }
            }
            Err(e) => Err(e),
        }
    }

    #[allow(clippy::result_unit_err)]
    pub fn cas(
        &mut self,
        key: &[u8],
        value: &[u8],
        optional: Option<&[u8]>,
        ttl: CoarseDuration,
        cas: u32,
    ) -> Result<(), SegCacheError> {
        match self.hashtable.try_update_cas(key, cas, &mut self.segments) {
            Ok(()) => self.insert(key, value, optional, ttl),
            Err(e) => Err(e),
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.hashtable.delete(key, &mut self.segments)
    }

    // tries to satisfy the need for a new segment from the free queue and then
    // through eviction
    // TODO(bmartin): probably use a result here
    fn get_new_segment(&mut self) -> Option<i32> {
        // avoid lifetime nonsense by just returning a segment id
        if let Some(s) = self.segments.pop_free() {
            Some(s)
        } else if let Some(id) = self.segments.least_valuable_seg() {
            if self.evict_segment(id).is_ok() {
                Some(id)
            } else {
                None
            }
        } else {
            None
        }
    }

    #[allow(clippy::result_unit_err)]
    fn evict_segment(&mut self, id: i32) -> Result<(), ()> {
        self.clear_segment(id, false)?;
        self.segments.push_free(id);
        Ok(())
    }

    #[allow(clippy::result_unit_err)]
    // TODO(bmartin): change bool arg to an enum 'reason'
    fn clear_segment(&mut self, id: i32, expire: bool) -> Result<(), ()> {
        let mut segment = self.segments.get_mut(id).unwrap();
        assert_eq!(segment.header.evictable(), true);
        segment.header.set_evictable(false);
        segment.header.set_accessible(false);

        // check to see if this is the tail of any list, the selection should
        // have avoided this - can really only happen with thread races, and we
        // don't support threading in this version (yet).
        if segment.header.next_seg().is_none() && !expire {
            Err(())
        } else {
            segment.clear(&mut self.hashtable, expire)
        }
    }

    // Loops through the TTL Buckets to handle expiration
    pub fn expire(&mut self) {
        clock_refresh!();
        self.ttl_buckets
            .expire(&mut self.hashtable, &mut self.segments);
    }
}

#[repr(C)]
pub struct HashTable<S: BuildHasher> {
    hash_builder: Box<S>, // boxed so the size is consistent independent of type
    power: u64,
    mask: u64,
    data: Box<[HashBucket]>,
    rng: Box<Random>,
    started: CoarseInstant,
    // this pads the struct out to a full cache line
    _pad0: u64,
}

impl<S> HashTable<S>
where
    S: BuildHasher,
{
    pub fn with_hasher(power: u8, hash_builder: S) -> HashTable<S> {
        let slots = 1_u64 << power;
        let buckets = slots as usize / 8;
        let mask = buckets as u64 - 1;

        let mut data = Vec::with_capacity(0);
        data.reserve_exact(buckets);
        data.resize(buckets, HashBucket::new());
        debug!(
            "hashtable has: {} slots across {} buckets",
            slots,
            data.len()
        );
        Self {
            hash_builder: Box::new(hash_builder),
            power: power.into(),
            mask,
            data: data.into_boxed_slice(),
            rng: Box::new(rng()),
            started: recent_coarse!(),
            _pad0: 0,
        }
    }

    // get the item info for a key if it exists in the hash
    pub fn get(&mut self, key: &[u8], segments: &mut Segments) -> Option<Item> {
        increment_counter!(&Stat::HashLookup);
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;
        trace!("hash: {} mask: {} bucket: {}", hash, self.mask, bucket_id);

        let bucket = self.data.get_mut(bucket_id as usize).unwrap();

        let chain_len = chain_len(bucket.data[0]);
        let n_item_slot = if chain_len > 0 { 7 } else { 8 };

        let curr_ts = self.started.elapsed().as_sec() as u64 & PROC_TS_MASK;
        if curr_ts != get_ts(bucket.data[0]) {
            bucket.data[0] = (bucket.data[0] & !TS_MASK) | (curr_ts << TS_BIT_SHIFT);
            for i in 1..n_item_slot {
                bucket.data[i] &= CLEAR_FREQ_SMOOTH_MASK;
            }
        }

        // NOTE: this is only valid for the first bucket in a chain, note that
        // we start scanning from 1 not 0. Chaining is currently not implemented
        for i in 1..n_item_slot {
            let current_info = bucket.data[i];

            if get_tag(current_info) == tag {
                let current_item = segments.get_item(current_info).unwrap();
                if current_item.key() != key {
                    increment_counter!(&Stat::HashTagCollision);
                } else {
                    // update item frequency
                    let mut freq = get_freq(current_info);
                    if freq < 127 {
                        let rand: u64 = self.rng.gen();
                        if freq <= 16 || rand % freq == 0 {
                            freq = ((freq + 1) | 0x80) << FREQ_BIT_SHIFT;
                        } else {
                            freq = (freq | 0x80) << FREQ_BIT_SHIFT;
                        }
                        if bucket.data[i] == current_info {
                            bucket.data[i] = (current_info & !FREQ_MASK) | freq;
                        }
                    }

                    let cas = get_cas(bucket.data[0]);

                    let item = Item {
                        raw: current_item,
                        cas,
                    };

                    return Some(item);
                }
            }
        }

        None
    }

    // get the item info for a key if it exists in the hash without incrementing
    // the item frequency
    pub fn get_no_freq_incr(&mut self, key: &[u8], segments: &mut Segments) -> Option<Item> {
        // TODO(bmartin): should this actually increment the stat?
        increment_counter!(&Stat::HashLookup);
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;
        trace!("hash: {} mask: {} bucket: {}", hash, self.mask, bucket_id);

        let bucket = self.data.get_mut(bucket_id as usize).unwrap();

        let chain_len = chain_len(bucket.data[0]);
        let n_item_slot = if chain_len > 0 { 7 } else { 8 };

        // NOTE: this is only valid for the first bucket in a chain, note that
        // we start scanning from 1 not 0. Chaining is currently not implemented
        for i in 1..n_item_slot {
            let current_info = bucket.data[i];

            if get_tag(current_info) == tag {
                let current_item = segments.get_item(current_info).unwrap();
                if current_item.key() != key {
                    increment_counter!(&Stat::HashTagCollision);
                } else {
                    let cas = get_cas(bucket.data[0]);

                    let item = Item {
                        raw: current_item,
                        cas,
                    };

                    return Some(item);
                }
            }
        }

        None
    }

    // TODO(bmartin): decide on what width to actually return here...
    pub fn get_freq(&mut self, key: &[u8], segments: &mut Segments) -> Option<u64> {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;

        let bucket = self.data.get_mut(bucket_id as usize).unwrap();

        let chain_len = chain_len(bucket.data[0]);
        let n_item_slot = if chain_len > 0 { 7 } else { 8 };

        // NOTE: this is only valid for the first bucket in a chain, note that
        // we start scanning from 1 not 0. Chaining is currently not implemented
        for i in 1..n_item_slot {
            let current_info = bucket.data[i];

            if get_tag(current_info) == tag {
                let current_item = segments.get_item(current_info).unwrap();
                if current_item.key() != key {
                    increment_counter!(&Stat::HashTagCollision);
                } else {
                    return Some(get_freq(current_info) & 0x7F);
                }
            }
        }

        None
    }

    #[allow(clippy::result_unit_err)]
    pub fn insert(
        &mut self,
        item: RawItem,
        seg: i32,
        offset: u64,
        segments: &mut Segments,
    ) -> Result<(), ()> {
        increment_counter!(&Stat::HashInsert);
        let hash = self.hash(&item.key());
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;
        let bucket = self.data.get_mut(bucket_id as usize).unwrap();

        let mut insert_item_info = tag | ((seg as u64) << 20) | (offset >> 3);

        let chain_len = chain_len(bucket.data[0]);
        let n_item_slot = if chain_len > 0 { 7 } else { 8 };

        // NOTE: this is only valid for the first bucket in a chain, note that
        // we start scanning from 1 not 0. Chaining is currently not implemented
        for i in 1..n_item_slot {
            let current_item_info = bucket.data[i];
            if get_tag(current_item_info) != tag {
                if insert_item_info != 0 && current_item_info == 0 {
                    // found a blank slot
                    bucket.data[i] = insert_item_info;
                    insert_item_info = 0;
                }
                continue;
            }
            if segments.get_item(current_item_info).unwrap().key() != item.key() {
                increment_counter!(&Stat::HashTagCollision);
            } else {
                // update existing key
                bucket.data[i] = insert_item_info;
                let _ = segments.remove_item(current_item_info, true);
                insert_item_info = 0;
            }
        }

        if insert_item_info == 0 {
            bucket.data[0] += 1;
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn try_update_cas(
        &mut self,
        key: &[u8],
        cas: u32,
        segments: &mut Segments,
    ) -> Result<(), SegCacheError> {
        increment_counter!(&Stat::HashLookup);
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);
        let bucket_id = hash & self.mask;
        trace!("hash: {} mask: {} bucket: {}", hash, self.mask, bucket_id);

        let bucket = self.data.get_mut(bucket_id as usize).unwrap();

        if cas != get_cas(bucket.data[0]) {
            return Err(SegCacheError::Exists);
        }

        let chain_len = chain_len(bucket.data[0]);
        let n_item_slot = if chain_len > 0 { 7 } else { 8 };

        // NOTE: this is only valid for the first bucket in a chain, note that
        // we start scanning from 1 not 0. Chaining is currently not implemented
        for i in 1..n_item_slot {
            let current_info = bucket.data[i];

            if get_tag(current_info) == tag {
                let current_item = segments.get_item(current_info).unwrap();
                if current_item.key() != key {
                    increment_counter!(&Stat::HashTagCollision);
                } else {
                    // update item frequency
                    let mut freq = get_freq(current_info);
                    if freq < 127 {
                        let rand: u64 = self.rng.gen();
                        if freq <= 16 || rand % freq == 0 {
                            freq = ((freq + 1) | 0x80) << FREQ_BIT_SHIFT;
                        } else {
                            freq = (freq | 0x80) << FREQ_BIT_SHIFT;
                        }
                        if bucket.data[i] == current_info {
                            bucket.data[i] = (current_info & !FREQ_MASK) | freq;
                        }
                    }

                    if cas == get_cas(bucket.data[0]) {
                        // TODO(bmartin): what is expected on overflow of the cas bits?
                        bucket.data[0] += 1;
                        return Ok(());
                    }
                }
            }
        }

        Err(SegCacheError::NotFound)
    }

    pub fn delete(&mut self, key: &[u8], segments: &mut Segments) -> bool {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);

        let first_bucket_id = hash & self.mask;
        let bucket_id = first_bucket_id;

        let mut deleted = false;

        let chain_len = chain_len(self.data.get(bucket_id as usize).unwrap().data[0]);

        for k in 0..=chain_len {
            let bucket = self.data.get_mut(bucket_id as usize).unwrap();

            let n_item_slot = if k == chain_len { 8 } else { 7 };

            for i in 0..n_item_slot {
                if bucket_id == first_bucket_id && i == 0 {
                    continue;
                }

                let current_item_info = bucket.data[i];

                if get_tag(current_item_info) == tag {
                    let current_item = segments.get_item(current_item_info).unwrap();
                    if current_item.key() != key {
                        increment_counter!(&Stat::HashTagCollision);
                        continue;
                    } else {
                        let _ = segments.remove_item(current_item_info, !deleted);
                        bucket.data[i] = 0;
                        deleted = true;
                    }
                }
            }
        }

        deleted
    }

    pub(crate) fn evict(&mut self, key: &[u8], offset: i32, segment: &mut Segment) -> bool {
        let hash = self.hash(key);
        let tag = tag_from_hash(hash);

        let first_bucket_id = hash & self.mask;
        let bucket_id = first_bucket_id;

        let evict_item_info = build_item_info(tag, segment.id() as u64, offset as u64);

        let mut evicted = false;
        let mut outdated = true;
        let mut first_match = true;

        let chain_len = chain_len(self.data.get(bucket_id as usize).unwrap().data[0]);

        for k in 0..=chain_len {
            let bucket = self.data.get_mut(bucket_id as usize).unwrap();

            let n_item_slot = if k == chain_len { 8 } else { 7 };

            for i in 0..n_item_slot {
                if bucket_id == first_bucket_id && i == 0 {
                    continue;
                }

                let current_item_info = clear_freq(bucket.data[i]);
                if get_tag(current_item_info) != tag {
                    continue;
                }

                if get_seg_id(current_item_info) == segment.id() {
                    let current_item = segment.get_item(current_item_info).unwrap();
                    if current_item.key() != key {
                        increment_counter!(&Stat::HashTagCollision);
                        continue;
                    }

                    if first_match {
                        if evict_item_info == current_item_info {
                            segment.remove_item(current_item_info, false);
                            bucket.data[i] = 0;
                            outdated = false;
                            evicted = true;
                        }
                        first_match = false;
                        continue;
                    } else {
                        if !evicted && current_item_info == evict_item_info {
                            evicted = true;
                        }
                        segment.remove_item(bucket.data[i], !outdated);
                        bucket.data[i] = 0;
                    }
                }
            }
        }

        evicted
    }

    fn hash(&self, key: &[u8]) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        hasher.write(key);
        hasher.finish()
    }
}

#[inline]
const fn tag_from_hash(hash: u64) -> u64 {
    (hash & TAG_MASK) | 0x0010000000000000
}

#[inline]
const fn get_offset(item_info: u64) -> u64 {
    (item_info & OFFSET_MASK) << 3
}

#[inline]
const fn get_seg_id(item_info: u64) -> i32 {
    ((item_info & SEG_ID_MASK) >> 20) as i32
}

#[inline]
const fn get_freq(item_info: u64) -> u64 {
    (item_info & FREQ_MASK) >> 44
}

#[inline]
const fn get_cas(bucket_info: u64) -> u32 {
    (bucket_info & CAS_MASK) as u32
}

#[inline]
const fn get_ts(bucket_info: u64) -> u64 {
    bucket_info & TS_MASK
}

#[inline]
const fn get_tag(item_info: u64) -> u64 {
    item_info & TAG_MASK
}

#[inline]
const fn clear_freq(item_info: u64) -> u64 {
    item_info & !FREQ_MASK
}

#[inline]
const fn chain_len(bucket_info: u64) -> u64 {
    (bucket_info & BUCKET_CHAIN_LEN_MASK) >> 20
}

#[inline]
const fn build_item_info(tag: u64, seg_id: u64, offset: u64) -> u64 {
    tag | (seg_id << 20) | (offset >> 3)
}

#[derive(Copy, Clone)]
pub struct HashBucket {
    data: [u64; 8],
}

impl HashBucket {
    fn new() -> Self {
        Self { data: [0; 8] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::RandomState;

    // Use ahash for testing with fixed seeds. Reproducible testing is important
    fn build_hasher() -> RandomState {
        RandomState::with_seeds(
            0xbb8c484891ec6c86,
            0x0522a25ae9c769f9,
            0xeed2797b9571bc75,
            0x4feb29c1fbbd59d0,
        )
    }

    #[test]
    fn sizes() {
        #[cfg(all(feature = "magic", feature = "precise_freq"))]
        assert_eq!(ITEM_HDR_SIZE, 13);

        #[cfg(all(not(feature = "magic"), feature = "precise_freq"))]
        assert_eq!(ITEM_HDR_SIZE, 9);

        #[cfg(all(feature = "magic", not(feature = "precise_freq")))]
        assert_eq!(ITEM_HDR_SIZE, 9);

        #[cfg(all(not(feature = "magic"), not(feature = "precise_freq")))]
        assert_eq!(ITEM_HDR_SIZE, 5);

        assert_eq!(std::mem::size_of::<Segments>(), 64);

        assert_eq!(std::mem::size_of::<HashBucket>(), 64);
        assert_eq!(std::mem::size_of::<HashTable<ahash::RandomState>>(), 64);

        assert_eq!(std::mem::size_of::<TtlBucket>(), 64);
        assert_eq!(std::mem::size_of::<TtlBuckets>(), 16);
    }

    #[test]
    fn init() {
        let mut cache = SegCache::new(16, build_hasher(), 64, 4 * 1024);
        assert_eq!(cache.items(), 0);
    }

    #[test]
    fn get_free_seg() {
        let mut cache = SegCache::new(16, build_hasher(), 64, 4 * 1024);
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 64);
        let seg = cache.get_new_segment();
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(seg, Some(0));
    }

    #[test]
    fn get() {
        let ttl = CoarseDuration::ZERO;
        let mut cache = SegCache::new(16, build_hasher(), 64, 4 * 1024);
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 64);
        assert!(cache.get(b"coffee").is_none());
        assert!(cache.insert(b"coffee", b"strong", None, ttl).is_ok());
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(cache.items(), 1);
        assert!(cache.get(b"coffee").is_some());

        let item = cache.get(b"coffee").unwrap();
        assert_eq!(item.value(), b"strong", "item is: {:?}", item);
    }

    #[test]
    fn overwrite() {
        let ttl = CoarseDuration::ZERO;
        let mut cache = SegCache::new(16, build_hasher(), 64, 4 * 1024);
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 64);
        assert!(cache.get(b"drink").is_none());

        println!("==== first insert ====");
        assert!(cache.insert(b"drink", b"coffee", None, ttl).is_ok());
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(cache.items(), 1);
        let item = cache.get(b"drink");
        assert!(item.is_some());
        let item = item.unwrap();
        let value = item.value();
        assert_eq!(value, b"coffee", "item is: {:?}", item);

        println!("==== second insert ====");
        assert!(cache.insert(b"drink", b"espresso", None, ttl).is_ok());
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(cache.items(), 1);
        let item = cache.get(b"drink");
        assert!(item.is_some());
        let item = item.unwrap();
        let value = item.value();
        assert_eq!(value, b"espresso", "item is: {:?}", item);

        println!("==== third insert ====");
        assert!(cache.insert(b"drink", b"whisky", None, ttl).is_ok());
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(cache.items(), 1);
        let item = cache.get(b"drink");
        assert!(item.is_some());
        let item = item.unwrap();
        let value = item.value();
        assert_eq!(value, b"whisky", "item is: {:?}", item);
    }

    #[test]
    fn delete() {
        let ttl = CoarseDuration::ZERO;
        let mut cache = SegCache::new(16, build_hasher(), 64, 4 * 1024);
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 64);
        assert!(cache.get(b"drink").is_none());

        assert!(cache.insert(b"drink", b"coffee", None, ttl).is_ok());
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(cache.items(), 1);
        let item = cache.get(b"drink");
        assert!(item.is_some());
        let item = item.unwrap();
        let value = item.value();
        assert_eq!(value, b"coffee", "item is: {:?}", item);

        assert_eq!(cache.delete(b"drink"), true);
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(cache.items(), 0);
    }

    #[test]
    fn collisions_2() {
        let ttl = CoarseDuration::ZERO;
        let mut cache = SegCache::new(3, build_hasher(), 2, 64);
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 2);

        // note: we can only fit 7 because the first bucket in the chain only
        // has 7 slots. since we don't support chaining, we must have a
        // collision on the 8th insert.
        for i in 0..1000 {
            let i = i % 3;
            let v = format!("{:02}", i);
            assert!(cache.insert(v.as_bytes(), v.as_bytes(), None, ttl).is_ok());
            let item = cache.get(v.as_bytes());
            assert!(item.is_some());
        }
    }

    #[test]
    fn collisions() {
        let ttl = CoarseDuration::ZERO;
        let mut cache = SegCache::new(3, build_hasher(), 64, 4 * 1024);
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 64);

        // note: we can only fit 7 because the first bucket in the chain only
        // has 7 slots. since we don't support chaining, we must have a
        // collision on the 8th insert.
        for i in 0..7 {
            let v = format!("{}", i);
            assert!(cache.insert(v.as_bytes(), v.as_bytes(), None, ttl).is_ok());
            let item = cache.get(v.as_bytes());
            assert!(item.is_some());
            assert_eq!(cache.items(), i + 1);
        }
        let v = b"8";
        assert!(cache.insert(v, v, None, ttl).is_err());
        assert_eq!(cache.items(), 7);
        assert_eq!(cache.delete(b"0"), true);
        assert_eq!(cache.items(), 6);
        assert!(cache.insert(v, v, None, ttl).is_ok());
        assert_eq!(cache.items(), 7);
    }

    #[test]
    fn full_cache_long() {
        let ttl = CoarseDuration::ZERO;
        let iters = 1_000_000;
        let segments = 32;
        let seg_size = 1024;
        let key_size = 1;
        let value_size = 512;

        let mut cache = SegCache::new(16, build_hasher(), segments as i32, seg_size);

        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), segments);

        let mut rng = rand::rng();

        let mut key = vec![0; key_size];
        let mut value = vec![0; value_size];

        let mut inserts = 0;

        for _ in 0..iters {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut value);

            if cache.insert(&key, &value, None, ttl).is_ok() {
                inserts += 1;
            };
        }

        assert_eq!(inserts, iters);
    }

    #[test]
    fn full_cache_long_2() {
        let ttl = CoarseDuration::ZERO;
        let iters = 10_000_000;
        let segments = 64;
        let seg_size = 2 * 1024;
        let key_size = 2;
        let value_size = 1;

        let mut cache = SegCache::new(16, build_hasher(), segments as i32, seg_size);

        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), segments);

        let mut rng = rand::rng();

        let mut key = vec![0; key_size];
        let mut value = vec![0; value_size];

        let mut inserts = 0;

        for _ in 0..iters {
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut value);

            if cache.insert(&key, &value, None, ttl).is_ok() {
                inserts += 1;
            };
        }

        // TODO(bmartin): can't check this until we do chaining, there are too
        // many collisions for this config.
        assert_eq!(inserts, 9999721);
    }

    #[test]
    fn expiration() {
        let segments = 64;
        let seg_size = 2 * 1024;

        let mut cache = SegCache::new(16, build_hasher(), segments as i32, seg_size);

        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), segments);

        assert!(cache
            .insert(b"latte", b"", None, CoarseDuration::from_secs(5))
            .is_ok());
        assert!(cache
            .insert(b"espresso", b"", None, CoarseDuration::from_secs(15))
            .is_ok());

        assert!(cache.get(b"latte").is_some());
        assert!(cache.get(b"espresso").is_some());
        assert_eq!(cache.items(), 2);
        assert_eq!(cache.segments.free(), segments - 2);

        // not enough time elapsed, not removed by expire
        cache.expire();
        assert!(cache.get(b"latte").is_some());
        assert!(cache.get(b"espresso").is_some());
        assert_eq!(cache.items(), 2);
        assert_eq!(cache.segments.free(), segments - 2);

        // wait and expire again
        std::thread::sleep(std::time::Duration::from_secs(10));
        cache.expire();

        assert!(cache.get(b"latte").is_none());
        assert!(cache.get(b"espresso").is_some());
        assert_eq!(cache.items(), 1);
        assert_eq!(cache.segments.free(), segments - 1);

        // wait and expire again
        std::thread::sleep(std::time::Duration::from_secs(10));
        cache.expire();

        assert!(cache.get(b"latte").is_none());
        assert!(cache.get(b"espresso").is_none());
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), segments);
    }
}
