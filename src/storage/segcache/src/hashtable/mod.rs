use crate::*;

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

                    return Some(Item::new(current_item, get_cas(bucket.data[0])));
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
                    return Some(Item::new(current_item, get_cas(bucket.data[0])));
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

#[derive(Copy, Clone)]
pub struct HashBucket {
    data: [u64; 8],
}

impl HashBucket {
    fn new() -> Self {
        Self { data: [0; 8] }
    }
}