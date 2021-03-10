// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate rustcommon_logger;

#[macro_use]
extern crate metrics;

use rustcommon_time::*;

use core::hash::BuildHasher;
use std::convert::TryInto;
use core::hash::Hasher;

mod common;
mod hashtable;
mod item;
mod rand;
mod segments;
mod ttl_buckets;

pub use item::Item;
pub use segments::Policy;

pub(crate) use common::*;
pub(crate) use hashtable::*;
pub(crate) use item::*;
pub(crate) use metrics::Stat;
pub(crate) use crate::rand::*;
pub(crate) use segments::*;
pub(crate) use ttl_buckets::TtlBuckets;

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

pub struct Builder<S: std::hash::BuildHasher> {
    hasher: Option<S>,
    power: u8,
    segments_builder: SegmentsBuilder,
}

impl<S: std::hash::BuildHasher> Default for Builder<S> {
    fn default() -> Self {
        Self {
            hasher: None,
            power: 16,
            segments_builder: SegmentsBuilder::default(),
        }
    }
}

impl<S: std::hash::BuildHasher + Default> Builder<S> {
    pub fn hasher(mut self, hasher: S) -> Self {
        self.hasher = Some(hasher);
        self
    }

    pub fn power(mut self, power: u8) -> Self {
        assert!(power > 0, "power must be greater than zero");
        self.power = power;
        self
    }

    pub fn segments(mut self, count: i32) -> Self {
        self.segments_builder = self.segments_builder.segments(count);
        self
    }

    pub fn seg_size(mut self, size: i32) -> Self {
        self.segments_builder = self.segments_builder.seg_size(size);
        self
    }

    pub fn eviction(mut self, policy: Policy) -> Self {
        self.segments_builder = self.segments_builder.eviction_policy(policy);
        self
    }

    pub fn build(self) -> SegCache<S> {
        let hasher = self.hasher.unwrap_or_default();
        SegCache::new(self.power, hasher, self.segments_builder)
    }
}

impl<S: std::hash::BuildHasher> SegCache<S> {
    pub fn builder() -> Builder<S> {
        Builder::default()
    }

    fn new(power: u8, hash_builder: S, segments_builder: SegmentsBuilder) -> Self {
        let hashtable = HashTable::with_hasher(power, hash_builder);

        let segments = segments_builder.build();
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
            Err(ttl_buckets::Error::ItemOversized) => Err(SegCacheError::ItemOversized),
            Err(ttl_buckets::Error::NoFreeSegments) => Err(SegCacheError::NoFreeSegments),
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


#[cfg(test)]
mod tests {
    use crate::common::ITEM_HDR_SIZE;
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

        #[cfg(feature = "magic")]
        assert_eq!(ITEM_HDR_SIZE, 9);

        #[cfg(not(feature = "magic"))]
        assert_eq!(ITEM_HDR_SIZE, 5);

        assert_eq!(std::mem::size_of::<Segments>(), 64);

        assert_eq!(std::mem::size_of::<HashBucket>(), 64);
        assert_eq!(std::mem::size_of::<HashTable<ahash::RandomState>>(), 64);

        assert_eq!(std::mem::size_of::<crate::ttl_buckets::TtlBucket>(), 64);
        assert_eq!(std::mem::size_of::<TtlBuckets>(), 16);
    }

    #[test]
    fn init() {
        let mut cache = SegCache::builder().seg_size(4096).hasher(build_hasher()).build();
        assert_eq!(cache.items(), 0);
    }

    #[test]
    fn get_free_seg() {
        let mut cache = SegCache::builder().seg_size(4096).hasher(build_hasher()).build();
        assert_eq!(cache.items(), 0);
        assert_eq!(cache.segments.free(), 64);
        let seg = cache.get_new_segment();
        assert_eq!(cache.segments.free(), 63);
        assert_eq!(seg, Some(0));
    }

    #[test]
    fn get() {
        let ttl = CoarseDuration::ZERO;
        let mut cache = SegCache::builder().seg_size(4096).hasher(build_hasher()).build();
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
        let mut cache = SegCache::builder().seg_size(4096).hasher(build_hasher()).build();
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
        let mut cache = SegCache::builder().seg_size(4096).hasher(build_hasher()).build();
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
        let mut cache = SegCache::builder().seg_size(64).segments(2).power(3).hasher(build_hasher()).build();
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
        let mut cache = SegCache::builder().seg_size(4096).power(3).hasher(build_hasher()).build();
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

        let mut cache = SegCache::builder().seg_size(seg_size).segments(segments as i32).power(16).hasher(build_hasher()).build();

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

        let mut cache = SegCache::builder().seg_size(seg_size).segments(segments as i32).power(16).hasher(build_hasher()).build();

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

        #[cfg(not(feature = "magic"))]
        assert_eq!(inserts, 9999721);

        #[cfg(feature = "magic")]
        assert_eq!(inserts, 9999702);
    }

    #[test]
    fn expiration() {
        let segments = 64;
        let seg_size = 2 * 1024;

        let mut cache = SegCache::builder().seg_size(seg_size).segments(segments as i32).power(16).hasher(build_hasher()).build();

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
