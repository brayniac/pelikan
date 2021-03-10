use crate::segments::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SegmentsError {
    #[error("bad segment id")]
    BadSegmentId,
}

pub struct SegmentsBuilder {
    seg_size: i32,
    segments: i32,
    evict_policy: Policy,
}

impl Default for SegmentsBuilder {
    fn default() -> Self {
        Self {
            seg_size: 1024 * 1024,
            segments: 64,
            evict_policy: Policy::Random,
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

    pub fn eviction_policy(mut self, policy: Policy) -> Self {
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
    evict: Box<Eviction>,          // eviction config and state
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
            evict: Box::new(Eviction::new(segments, evict_policy)),
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

    // pub fn evict(&mut self) -> Option<i32> {
    //     // avoid lifetime nonsense by just returning a segment id
    //     if let Some(s) = self.segments.pop_free() {
    //         Some(s)
    //     } else if let Some(id) = self.segments.least_valuable_seg() {
    //         if self.evict_segment(id).is_ok() {
    //             Some(id)
    //         } else {
    //             None
    //         }
    //     } else {
    //         None
    //     }
    // }

    fn unlink(&mut self, id: i32) {
        let id_idx = id as usize;
        if let Some(next) = self.headers[id_idx].next_seg() {
            let prev = self.headers[id_idx].prev_seg().unwrap_or(-1);
            self.headers[next as usize].set_prev_seg(prev);
        }

        if let Some(prev) = self.headers[id_idx].prev_seg() {
            let next = self.headers[id_idx].next_seg().unwrap_or(-1);
            self.headers[prev as usize].set_next_seg(next);
        }
    }

    fn push_front(&mut self, this: i32, head: i32) {
        let this_idx = this as usize;
        self.headers[this_idx].set_next_seg(head);
        self.headers[this_idx].set_prev_seg(-1);

        if head.is_some() {
            let head_idx = head as usize;
            debug_assert!(self.headers[head_idx].prev_seg().is_none());
            self.headers[head_idx].set_prev_seg(this);
        }
    }

    // adds a segment to the free queue
    pub(crate) fn push_free(&mut self, id: i32) {
        // unlinks the next segment
        self.unlink(id);

        // relinks it as the free queue head
        self.push_front(id, self.free_q);
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
        match self.evict.policy() {
            Policy::None => None,
            Policy::Random => {
                let mut start: i32 = self.evict.random();
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
