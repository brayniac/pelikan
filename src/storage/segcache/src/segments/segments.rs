use crate::segments::*;
use core::cmp::max;
use core::cmp::Ordering;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SegmentsError {
    #[error("bad segment id")]
    BadSegmentId,
}

pub struct SegEvict {
    policy: EvictPolicy,
    last_update_time: CoarseInstant,
    ranked_segs: Box<[i32]>,
    index: usize,
    rng: Box<Random>,
}

impl SegEvict {
    fn least_valuable_seg(&mut self) -> Option<i32> {
        let index = self.index;
        self.index += 1;
        if index < self.ranked_segs.len() {
            self.ranked_segs[index].as_option()
        } else {
            None
        }
    }

    fn should_rerank(&mut self) -> bool {
        let now = recent_coarse!();
        if self.ranked_segs[0] == -1
            || (now - self.last_update_time).as_sec() > 1
            || self.ranked_segs.len() < (self.index + 8)
        {
            self.last_update_time = now;
            true
        } else {
            false
        }
    }

    fn rerank(&mut self, mut headers: Vec<SegmentHeader>) {
        match self.policy {
            EvictPolicy::None | EvictPolicy::Random => {
                return;
            }
            EvictPolicy::Fifo => {
                headers.sort_by(|a, b| Self::compare_fifo(a, b));
            }
            EvictPolicy::Cte => {
                headers.sort_by(|a, b| Self::compare_cte(a, b));
            }
            EvictPolicy::Util => {
                headers.sort_by(|a, b| Self::compare_util(a, b));
            }
        }
        for (i, id) in self.ranked_segs.iter_mut().enumerate() {
            *id = headers.get(i).map(|header| header.id()).unwrap_or(-1);
        }
    }

    fn compare_fifo(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else if max(lhs.create_at(), lhs.merge_at()) > max(rhs.create_at(), rhs.merge_at()) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    fn compare_cte(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else if (lhs.create_at() + lhs.ttl()) > (rhs.create_at() + rhs.ttl()) {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    fn compare_util(lhs: &SegmentHeader, rhs: &SegmentHeader) -> Ordering {
        if !lhs.can_evict() {
            Ordering::Greater
        } else if !rhs.can_evict() {
            Ordering::Less
        } else if lhs.occupied_size() > rhs.occupied_size() {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    fn new(nseg: usize, policy: EvictPolicy) -> Self {
        let mut ranked_segs = Vec::with_capacity(0);
        ranked_segs.reserve_exact(nseg);
        ranked_segs.resize(nseg, -1);
        let ranked_segs = ranked_segs.into_boxed_slice();

        Self {
            policy,
            last_update_time: recent_coarse!(),
            ranked_segs,
            index: 0,
            rng: Box::new(rng()),
        }
    }
}

pub struct SegmentsBuilder {
    seg_size: i32,
    segments: i32,
    evict_policy: EvictPolicy,
}

impl Default for SegmentsBuilder {
    fn default() -> Self {
        Self {
            seg_size: 1024 * 1024,
            segments: 64,
            evict_policy: EvictPolicy::Random,
        }
    }
}

impl SegmentsBuilder {
    pub fn seg_size(mut self, bytes: i32) -> Self {
        #[cfg(not(feature = "magic"))]
        assert!(bytes > ITEM_HDR_SIZE as i32);

        #[cfg(feature = "magic")]
        assert!(bytes >= ITEM_HDR_SIZE as i32 + ITEM_MAGIC_SIZE as i32);

        self.seg_size = bytes;
        self
    }

    pub fn segments(mut self, count: i32) -> Self {
        assert!(count > 0);
        self.segments = count;
        self
    }

    pub fn eviction_policy(mut self, policy: EvictPolicy) -> Self {
        self.evict_policy = policy;
        self
    }

    pub fn build(self) -> Segments {
        Segments::from_builder(self)
    }
}

pub struct Segments {
    headers: Box<[SegmentHeader]>, // pointer to slice of headers
    data: Box<[u8]>,               // pointer to raw data
    seg_size: i32,                 // size of segment in bytes
    free: i32,                     // number of segments free
    cap: i32,                      // total number of segments
    free_q: i32,                   // next free segment
    flush_at: CoarseInstant,       // time last flushed
    evict: Box<SegEvict>,          // eviction config and state
}

impl Default for Segments {
    fn default() -> Self {
        Self::from_builder(Default::default())
    }
}

impl Segments {
    pub fn builder() -> SegmentsBuilder {
        SegmentsBuilder::default()
    }

    fn from_builder(builder: SegmentsBuilder) -> Self {
        let seg_size = builder.seg_size;
        let segments = builder.segments as usize;
        let evict_policy = builder.evict_policy;

        let mut headers = Vec::with_capacity(0);
        headers.reserve_exact(segments);
        for id in 0..segments {
            let header = SegmentHeader::new(id as i32);
            headers.push(header);
        }
        let mut headers = headers.into_boxed_slice();

        let heap_size = segments * seg_size as usize;
        let mut data = Vec::with_capacity(0);
        data.reserve_exact(heap_size);
        data.resize(heap_size, 0);
        let mut data = data.into_boxed_slice();

        for id in 0..segments {
            let begin = seg_size as usize * id;
            let end = begin + seg_size as usize;
            let mut segment = Segment {
                header: &mut headers[id],
                data: &mut data[begin..end],
            };
            segment.init();
            if id > 0 {
                segment.header.set_prev_seg(id as i32 - 1);
            }
            if id < (segments - 1) {
                segment.header.set_next_seg(id as i32 + 1);
            }
        }

        Self {
            headers,
            seg_size, // store as a usize because it reduces casting
            cap: segments as i32,
            free: segments as i32,
            free_q: 0,
            data,
            flush_at: recent_coarse!(),
            evict: Box::new(SegEvict::new(segments, evict_policy)),
        }
    }

    // returns the segment size in bytes
    #[inline]
    pub fn segment_size(&self) -> i32 {
        self.seg_size
    }

    // returns the number of segments in the free queue
    pub fn free(&self) -> usize {
        self.free as usize
    }

    pub fn flush_at(&self) -> CoarseInstant {
        self.flush_at
    }

    pub(crate) fn get_item(&mut self, item_info: u64) -> Option<RawItem> {
        let seg_id = get_seg_id(item_info);
        let offset = get_offset(item_info) as usize;
        self.get_item_at(seg_id, offset)
    }

    // returns the item looking it up from the item_info
    // TODO(bmartin): consider changing the return type here and removing asserts?
    pub(crate) fn get_item_at(&mut self, seg_id: i32, offset: usize) -> Option<RawItem> {
        trace!("getting item from: seg: {} offset: {}", seg_id, offset);
        assert!(seg_id < self.cap as i32);

        let seg_begin = self.seg_size as usize * seg_id as usize;
        let seg_end = seg_begin + self.seg_size as usize;
        let mut segment = Segment {
            header: &mut self.headers[seg_id as usize],
            data: &mut self.data[seg_begin..seg_end],
        };

        segment.get_item_at(offset)
    }

    // // looks up the segment by id and returns an immutable view of it
    // pub(crate) fn get(&self, id: i32) -> Result<Segment, SegmentsError> {
    //     if id < 0 {
    //         Err(SegmentsError::BadSegmentId)
    //     } else {
    //         let id = id as usize;
    //         let header = self.headers.get(id).ok_or(SegmentsError::BadSegmentId)?;
    //         // this is safe because we now know the id was within range
    //         let data = unsafe { self.data.as_ptr().add(self.seg_size as usize * id) };
    //         let segment = Segment {
    //             header,
    //             data: unsafe { std::slice::from_raw_parts(data, self.seg_size as usize) },
    //         };
    //         segment.check_magic();
    //         Ok(segment)
    //     }
    // }

    // looks up the segment by id and returns a mutable view of it
    pub(crate) fn get_mut(&mut self, id: i32) -> Result<Segment, SegmentsError> {
        if id < 0 {
            Err(SegmentsError::BadSegmentId)
        } else {
            let id = id as usize;
            let header = self
                .headers
                .get_mut(id)
                .ok_or(SegmentsError::BadSegmentId)?;
            // this is safe because we now know the id was within range
            let data = unsafe { self.data.as_mut_ptr().add(self.seg_size as usize * id) };
            let segment = Segment {
                header,
                data: unsafe { std::slice::from_raw_parts_mut(data, self.seg_size as usize) },
            };
            segment.check_magic();
            Ok(segment)
        }
    }

    // adds a segment to the free queue
    pub(crate) fn push_free(&mut self, id: i32) {
        // unlinks the next segment
        if let Some(next) = self.headers[id as usize].next_seg() {
            self.headers[next as usize].set_prev_seg(-1);
        }

        self.headers[id as usize].set_next_seg(self.free_q);
        self.headers[id as usize].set_prev_seg(-1);

        if self.free_q.is_some() {
            debug_assert!(self.headers[self.free_q as usize].prev_seg().is_none());
            self.headers[self.free_q as usize].set_prev_seg(id);
        }
        self.free_q = id;

        assert!(!self.headers[id as usize].evictable());
        self.headers[id as usize].set_accessible(false);

        self.headers[id as usize].reset();

        self.free += 1;
    }

    // get a segment from the free queue
    pub(crate) fn pop_free(&mut self) -> Option<i32> {
        assert!(self.free >= 0);
        assert!(self.free <= self.cap);

        if self.free == 0 {
            None
        } else {
            self.free -= 1;
            let id = self.free_q;
            assert!(id.is_some());

            if let Some(next) = self.headers[id as usize].next_seg() {
                self.free_q = next;
                // this is not really necessary
                let next = &mut self.headers[next as usize];
                next.set_prev_seg(-1);
            } else {
                self.free_q = -1;
            }

            #[cfg(not(feature = "magic"))]
            assert_eq!(self.headers[id as usize].write_offset(), 0);

            #[cfg(feature = "magic")]
            assert_eq!(
                self.headers[id as usize].write_offset() as usize,
                std::mem::size_of_val(&SEG_MAGIC),
                "segment: ({}) in free queue has write_offset: ({})",
                id,
                self.headers[id as usize].write_offset()
            );

            Some(id)
        }
    }

    // TODO(bmartin): use a result here, not option
    pub(crate) fn least_valuable_seg(&mut self) -> Option<i32> {
        match self.evict.policy {
            EvictPolicy::None => None,
            EvictPolicy::Random => {
                let mut start: i32 = self.evict.rng.gen();
                if start < 0 {
                    start *= -1;
                }

                start %= self.cap;

                for i in 0..self.cap {
                    let id = (start + i) % self.cap;
                    if self.headers[id as usize].can_evict() {
                        return Some(id);
                    }
                }

                None
            }
            _ => {
                if self.evict.should_rerank() {
                    let mut headers = Vec::with_capacity(self.headers.len());
                    headers.extend_from_slice(&self.headers);
                    self.evict.rerank(headers);
                }
                while let Some(id) = self.evict.least_valuable_seg() {
                    if self.headers[id as usize].can_evict() {
                        return Some(id);
                    }
                }
                None
            }
        }
    }

    // remove a single item from a segment based on the item_info, optionally
    // setting tombstone
    pub(crate) fn remove_item(
        &mut self,
        item_info: u64,
        tombstone: bool,
    ) -> Result<(), SegmentsError> {
        let seg_id = get_seg_id(item_info);
        let offset = get_offset(item_info) as usize;
        self.remove_at(seg_id, offset, tombstone)
    }

    pub(crate) fn remove_at(
        &mut self,
        seg_id: i32,
        offset: usize,
        tombstone: bool,
    ) -> Result<(), SegmentsError> {
        let mut segment = self.get_mut(seg_id)?;
        segment.remove_item_at(offset, tombstone);
        Ok(())
    }

    // mostly for testing, probably never want to run this otherwise
    pub(crate) fn items(&mut self) -> usize {
        let mut total = 0;
        for id in 0..self.cap {
            let segment = self.get_mut(id as i32).unwrap();
            segment.check_magic();
            let count = segment.header.n_item();
            debug!(
                "{} items in segment {} header: {:?}",
                count, id, segment.header
            );
            total += segment.header.n_item() as usize;
        }
        total
    }

    pub(crate) fn print_headers(&self) {
        for id in 0..self.cap {
            println!("segment header: {:?}", self.headers[id as usize]);
        }
    }

    pub(crate) fn check_integrity(&mut self) -> bool {
        let mut integrity = true;
        for id in 0..self.cap {
            if !self.get_mut(id).unwrap().check_integrity() {
                integrity = false;
            }
        }
        integrity
    }
}
