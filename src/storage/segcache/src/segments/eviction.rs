use crate::segments::*;
use core::cmp::max;
use core::cmp::Ordering;

/// `Policy` defines the eviction strategy to be used.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Policy {
    /// No eviction
    None,
    /// Segment random eviction
    Random,
    /// FIFO segment eviction
    Fifo,
    /// Closest to expiration
    Cte,
    /// Least utilized segment
    Util,
}

/// The `Eviction` struct is used to rank and return segments for eviction. It
/// implements eviction strategies corresponding to the `Policy`.
pub struct Eviction {
    policy: Policy,
    last_update_time: CoarseInstant,
    ranked_segs: Box<[i32]>,
    index: usize,
    rng: Box<Random>,
}

impl Eviction {
    /// Creates a new `Eviction` struct which will handle up to `nseg` segments
    /// using the specified eviction policy.
    pub fn new(nseg: usize, policy: Policy) -> Self {
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

    pub fn policy(&self) -> Policy {
        self.policy
    }

    /// Returns the segment id of the least valuable segment
    pub fn least_valuable_seg(&mut self) -> Option<i32> {
        let index = self.index;
        self.index += 1;
        if index < self.ranked_segs.len() {
            self.ranked_segs[index].as_option()
        } else {
            None
        }
    }

    /// Returns a random segment id
    pub fn random(&mut self) -> i32 {
        self.rng.gen()
    }

    pub fn should_rerank(&mut self) -> bool {
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

    pub fn rerank(&mut self, mut headers: Vec<SegmentHeader>) {
        match self.policy {
            Policy::None | Policy::Random => {
                return;
            }
            Policy::Fifo => {
                headers.sort_by(|a, b| Self::compare_fifo(a, b));
            }
            Policy::Cte => {
                headers.sort_by(|a, b| Self::compare_cte(a, b));
            }
            Policy::Util => {
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

    
}