// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;

use crate::common::ThinOption;

use thiserror::Error;

const N_BUCKET_PER_STEP_N_BIT: usize = 8;
const N_BUCKET_PER_STEP: usize = 1 << N_BUCKET_PER_STEP_N_BIT;

const TTL_BUCKET_INTERVAL_N_BIT_1: usize = 3;
const TTL_BUCKET_INTERVAL_N_BIT_2: usize = 7;
const TTL_BUCKET_INTERVAL_N_BIT_3: usize = 11;
const TTL_BUCKET_INTERVAL_N_BIT_4: usize = 15;

const TTL_BUCKET_INTERVAL_1: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_1;
const TTL_BUCKET_INTERVAL_2: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_2;
const TTL_BUCKET_INTERVAL_3: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_3;
const TTL_BUCKET_INTERVAL_4: usize = 1 << TTL_BUCKET_INTERVAL_N_BIT_4;

const TTL_BOUNDARY_1: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_1 + N_BUCKET_PER_STEP_N_BIT);
const TTL_BOUNDARY_2: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_2 + N_BUCKET_PER_STEP_N_BIT);
const TTL_BOUNDARY_3: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_3 + N_BUCKET_PER_STEP_N_BIT);
const TTL_BOUNDARY_4: i32 = 1 << (TTL_BUCKET_INTERVAL_N_BIT_4 + N_BUCKET_PER_STEP_N_BIT);

#[derive(Error, Debug)]
pub enum TtlBucketError {
    #[error("item is oversized")]
    ItemOversized,
    #[error("ttl bucket expansion failed, no free segments")]
    NoFreeSegments,
}

pub struct TtlBucket {
    head: i32,
    tail: i32,
    ttl: i32,
    next_expiration: i32,
    segments: i32,
    next_to_merge: i32,
    last_cutoff_freq: i32,
    _pad: u32, // pad to half a cacheline
}

pub struct TtlBuckets {
    buckets: Box<[TtlBucket]>,
}

impl TtlBucket {
    fn new(ttl: i32) -> Self {
        Self {
            head: -1,
            tail: -1,
            ttl,
            next_expiration: 0,
            segments: 0,
            next_to_merge: -1,
            last_cutoff_freq: 0,
            _pad: 0,
        }
    }

    /// Expands the `TtlBucket` by requesting another segment
    fn expand(&mut self, segments: &mut Segments) -> Result<(), TtlBucketError> {
        if let Some(id) = segments.pop_free() {
            {
                if self.tail.is_some() {
                    let tail = segments.get_mut(self.tail).unwrap();
                    tail.header.set_next_seg(id);
                }
            }

            let segment = segments.get_mut(id).unwrap();
            segment.header.set_prev_seg(self.tail);
            segment.header.set_next_seg(-1);
            segment
                .header
                .set_ttl(CoarseDuration::from_secs(self.ttl as u32));
            if self.head.is_none() {
                debug_assert!(self.tail.is_none());
                self.head = id;
            }
            self.tail = id;
            self.segments += 1;
            debug_assert_eq!(segment.header.evictable(), false);
            segment.header.set_evictable(true);
            segment.header.set_accessible(true);
            Ok(())
        } else {
            Err(TtlBucketError::NoFreeSegments)
        }
    }

    pub(crate) fn reserve(
        &mut self,
        size: usize,
        segments: &mut Segments,
    ) -> Result<ReservedItem, TtlBucketError> {
        trace!("reserving: {} bytes for ttl: {}", size, self.ttl);

        let seg_size = segments.segment_size() as usize;

        if size > seg_size {
            debug!("item is oversized");
            return Err(TtlBucketError::ItemOversized);
        }

        loop {
            if let Ok(segment) = segments.get_mut(self.tail) {
                if !segment.accessible() {
                    continue;
                }
                // TODO(bmartin): this handling needs to change for threaded impl
                let offset = segment.header.write_offset() as usize;
                debug!("offset: {}", offset);
                if offset + size <= seg_size {
                    let _ = segment.header.incr_write_offset(size as i32);
                    let _ = segment.header.incr_occupied_size(size as i32);
                    segment.header.incr_n_item();
                    let ptr = unsafe { segment.data.as_mut_ptr().add(offset) };

                    let item = Item::from_ptr(ptr);
                    return Ok(ReservedItem {
                        item,
                        offset,
                        seg: segment.header.id(),
                    });
                }
            }
            self.expand(segments)?;
        }
    }
}

impl Default for TtlBuckets {
    fn default() -> Self {
        Self::new()
    }
}

impl TtlBuckets {
    pub fn new() -> Self {
        let intervals = [
            TTL_BUCKET_INTERVAL_1,
            TTL_BUCKET_INTERVAL_2,
            TTL_BUCKET_INTERVAL_3,
            TTL_BUCKET_INTERVAL_4,
        ];

        let mut buckets = Vec::with_capacity(0);
        buckets.reserve_exact(intervals.len() * N_BUCKET_PER_STEP as usize);

        for interval in &intervals {
            for j in 0..N_BUCKET_PER_STEP {
                let ttl = interval * j + 1;
                let bucket = TtlBucket::new(ttl as i32);
                buckets.push(bucket);
            }
        }

        let buckets = buckets.into_boxed_slice();

        Self { buckets }
    }

    fn get_bucket_index(&self, ttl: CoarseDuration) -> usize {
        let ttl = ttl.as_sec() as i32;
        if ttl <= 0 {
            self.buckets.len() - 1
        } else if ttl & !(TTL_BOUNDARY_1 - 1) == 0 {
            (ttl >> TTL_BUCKET_INTERVAL_N_BIT_1) as usize
        } else if ttl & !(TTL_BOUNDARY_2 - 1) == 0 {
            (ttl >> TTL_BUCKET_INTERVAL_N_BIT_2) as usize + N_BUCKET_PER_STEP
        } else if ttl & !(TTL_BOUNDARY_3 - 1) == 0 {
            (ttl >> TTL_BUCKET_INTERVAL_N_BIT_3) as usize + N_BUCKET_PER_STEP * 2
        } else {
            std::cmp::min(
                (ttl >> TTL_BUCKET_INTERVAL_N_BIT_4) as usize + N_BUCKET_PER_STEP * 3,
                self.buckets.len() - 1,
            )
        }
    }

    // TODO(bmartin): confirm handling for negative TTLs here...
    pub fn get_mut_bucket(&mut self, ttl: CoarseDuration) -> &mut TtlBucket {
        let index = self.get_bucket_index(ttl);

        // NOTE: since get_bucket_index() must return an index within the slice,
        // we do not need to worry about UB here.
        unsafe { self.buckets.get_unchecked_mut(index) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bucket_index() {
        let ttl_buckets = TtlBuckets::new();
        assert_eq!(ttl_buckets.get_bucket_index(CoarseDuration::ZERO), 1023);
        assert_eq!(ttl_buckets.get_bucket_index(CoarseDuration::MAX), 1023);
    }
}
