// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::borrow::Borrow;
use std::convert::TryInto;
use std::time::SystemTime;

// use ahash::AHashMap;
// use fxhash::FxHashMap;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("not stored")]
    NotStored,
    #[error("exists")]
    Exists,
    #[error("not found")]
    NotFound,
    #[error("no memory")]
    NoMemory,
}

use rand::prelude::*;
use std::hash::BuildHasher;
use std::hash::Hash;
use std::hash::Hasher;

use ahash::RandomState;

struct Entry<K, V> {
    key: K, // 24 bytes
    value: V, // 24 bytes
    expiry: u64, // 8 byte
    expires: bool, // 1 byte
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V, expiry: Option<u64>) -> Self {
        Self { key, value, expires: expiry.is_some(), expiry: expiry.unwrap_or(0) }
    }
}

pub struct Table<K, V> {
    hashers: [RandomState; 4],
    data: Vec<Option<Entry<K, V>>>,
}

impl<K: PartialEq + Hash + Eq, V> Table<K, V> {
    pub fn with_capacity(items: usize) -> Self {
        let hashers = [
            RandomState::with_seeds(
                0xbb8c484891ec6c86,
                0x0522a25ae9c769f9,
                0xeed2797b9571bc75,
                0x4feb29c1fbbd59d0,
            ),
            RandomState::with_seeds(
                0x8311d8f153515ff4,
                0xd22e51032364b4d3,
                0x09b8db5fb059ee6c,
                0xe24d08dfec3e78f6,
            ),
            RandomState::with_seeds(
                0x1bb782fb90137932,
                0x82bdf5530d94544e,
                0x9db2dee23f7c7a09,
                0xe3cfa85548d2daba,
            ),
            RandomState::with_seeds(
                0xba4b6fc9f600b396,
                0x9579f32609013d9f,
                0xbf73237d0a5cbd3a,
                0xa7ecd88e69029010,
            ),
        ];
        let size = items.next_power_of_two();
        let mut data = Vec::with_capacity(0);
        data.reserve_exact(size);
        for _ in 0..size {
            data.push(None)
        }
        Self { hashers, data }
    }

    pub fn insert(&mut self, key: K, value: V, expiry: Option<u64>) {
        let mut positions = [0; 4];
        let mut now: Option<u64> = None;

        // handle replacing existing K-V pair
        // up to 4 hash operations
        #[allow(clippy::needless_range_loop)]
        for hash_id in 0..4 {
            let mut hasher = self.hashers[hash_id].build_hasher();
            key.hash(&mut hasher);
            let position = (hasher.finish() as usize) & (self.data.len() - 1);
            positions[hash_id] = position;

            // Check each of the 4 positions for a matching key and replace if
            // found. While visiting each position, handle expiry to free up
            // space.
            if self.data[position].is_some() {
                if self.data[position].as_ref().unwrap().key == key {
                    if let Some(current) = &mut self.data[position] {
                        current.value = value;
                        current.expires = expiry.is_some();
                        current.expiry = expiry.unwrap_or(0);
                        return;
                    }
                } else if self.data[position].as_ref().unwrap().expires {
                    if now.is_none() {
                        let epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
                        now = Some(epoch.as_secs());
                    }
                    if self.data[position].as_ref().unwrap().expiry < now.unwrap() {
                        let _ = self.data[position].take().unwrap();
                    }
                }
            }
        }

        // Handle adding entry into an empty position since key is not in the
        // table. We do not check expiry since all 4 positions have been expired
        // already.
        for position in &positions {
            if self.data[*position].is_none() {
                self.data[*position] = Some(Entry::new(key, value, expiry));
                return;
            }
        }

        // Since there were no empty entries, we need to try to move entries
        // around. Up to 16 hash operations here. Preemptively expire entries in
        // those additional positions.
        for position in &positions {
            for hash_id in 0..4 {
                let mut hasher = self.hashers[hash_id].build_hasher();

                // we already know each of the 4 primary positions are occupied,
                // so this unwrap is safe.
                self.data[*position].as_ref().unwrap().key.hash(&mut hasher);

                let candidate_position = (hasher.finish() as usize) & (self.data.len() - 1);

                // Check if the candidate position is free, if it is we move the
                // existing entry into the candidate position and replace the
                // existing entry with the new entry.
                if self.data[candidate_position].is_none() {
                    self.data[candidate_position] = self.data[*position].take();
                    self.data[*position] = Some(Entry::new(key, value, expiry));
                    return;
                }

                // Try to expire entry at candidate position. We know the
                // position is occupied based on the previous check, so this
                // unwrap is safe.
                if self.data[candidate_position].as_ref().unwrap().expires {
                    if now.is_none() {
                        let epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
                        now = Some(epoch.as_secs());
                    }
                    if self.data[candidate_position].as_ref().unwrap().expiry < now.unwrap() {
                        // If we expire the entry at the candidate position, we
                        // can replace it with the existing entry and replace
                        // the existing entry with the new entry.
                        self.data[candidate_position] = self.data[*position].take();
                        self.data[*position] = Some(Entry::new(key, value, expiry));
                        return;
                    }
                }
            }
        }

        // could not make room, randomly evict
        let mut rng = thread_rng();
        let position: usize = rng.gen_range(0..4);
        self.data[position] = Some(Entry::new(key, value, expiry));
    }

    pub fn get<Q: ?Sized>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut now: Option<u64> = None;

        for hash_id in 0..4 {
            let mut hasher = self.hashers[hash_id].build_hasher();
            key.hash(&mut hasher);
            let position = (hasher.finish() as usize) & (self.data.len() - 1);
            if self.data[position].is_some() {
                if self.data[position].as_ref().unwrap().expires {
                    if now.is_none() {
                        let epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
                        now = Some(epoch.as_secs());
                    }

                    if self.data[position].as_ref().unwrap().expiry < now.unwrap() {
                        let current = self.data[position].take().unwrap();
                        if *current.key.borrow() == *key {
                            return None;
                        }
                    }
                } else if *self.data[position].as_ref().unwrap().key.borrow() == *key {
                    return Some(&self.data[position].as_ref().unwrap().value);
                }
            }
        }
        None
    }

    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let mut now: Option<u64> = None;

        for hash_id in 0..4 {
            let mut hasher = self.hashers[hash_id].build_hasher();
            key.hash(&mut hasher);
            let position = (hasher.finish() as usize) & (self.data.len() - 1);
            if self.data[position].is_some() {
                if self.data[position].as_ref().unwrap().expires {
                    if now.is_none() {
                        let epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
                        now = Some(epoch.as_secs());
                    }

                    if self.data[position].as_ref().unwrap().expiry < now.unwrap() {
                        let current = self.data[position].take().unwrap();
                        if *current.key.borrow() == *key {
                            return Some(current.value);
                        }
                    }
                } else if *self.data[position].as_ref().unwrap().key.borrow() == *key {
                    let current = self.data[position].take().unwrap();
                    return Some(current.value);
                    // return Some(&self.data[position].as_ref().unwrap().value)
                }
            }
        }
        None
    }

    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.get(key).is_some()
    }
}

// TODO(bmartin): this is just a placeholder, we probably would benefit from
// taking a generic type implementing some storage trait to allow for pluggable
// storage backends.

// TODO(bmartin): this needs to be replaced before using for production
// workloads.
pub struct HashTable {
    inner: Table<Vec<u8>, Item>,
    size_current: u64,
    size_limit: u64,
}

pub struct Item {
    value: Vec<u8>,
    flags: u32,
    cas: Option<u64>,
}

impl Item {
    pub fn new(value: &[u8], flags: u32, cas: Option<u64>) -> Self {
        Self {
            value: value.to_vec(),
            flags,
            cas,
        }
    }

    fn size(&self) -> u64 {
        (std::mem::size_of::<Item>() + self.value.len() + std::mem::size_of::<Option<SystemTime>>())
            .try_into()
            .unwrap()
    }

    pub fn value_len(&self) -> usize {
        self.value.len()
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn cas(&self) -> Option<u64> {
        self.cas
    }
}

impl HashTable {
    pub fn new() -> Self {
        Self {
            inner: Table::with_capacity(16_777_216), // 2^24
            size_current: 0,
            size_limit: 4 * 1024_u64.pow(3),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<&Item, Error> {
        if let Some(entry) = self.inner.get(key) {
            Ok(entry)
        } else {
            Err(Error::NotFound)
        }
    }

    pub fn cas(&mut self, key: &[u8], item: Item, expiry: Option<u64>) -> Result<(), Error> {
        if let Ok(existing) = self.get(key) {
            if existing.cas() != item.cas() {
                return Err(Error::Exists);
            }
        }
        self.set(key, item, expiry)
    }

    pub fn set(&mut self, key: &[u8], item: Item, expiry: Option<u64>) -> Result<(), Error> {
        let existing_size = if let Ok(existing) = self.get(key) {
            existing.size() + key.len() as u64
        } else {
            0
        };

        let current_size = self.size_current - existing_size;
        let new_size = current_size + item.size() + key.len() as u64;

        if new_size < self.size_limit {
            self.inner.insert(key.to_vec(), item, expiry);
            self.size_current = new_size;
            Ok(())
        } else {
            // TODO(bmartin): eviction needs to be handled here
            Err(Error::NoMemory)
        }
    }

    pub fn add(&mut self, key: &[u8], item: Item, expiry: Option<u64>) -> Result<(), Error> {
        if self.inner.contains_key(key) {
            Err(Error::NotStored)
        } else {
            self.set(key, item, expiry)
        }
    }

    pub fn replace(
        &mut self,
        key: &[u8],
        item: Item,
        expiry: Option<u64>,
    ) -> Result<(), Error> {
        if self.inner.contains_key(key) {
            self.set(key, item, expiry)
        } else {
            Err(Error::NotStored)
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Result<Item, Error> {
        match self.inner.remove(key) {
            Some(entry) => {
                self.size_current -= entry.size() + key.len() as u64;
                Ok(entry)
            }
            None => Err(Error::NotFound),
        }
    }
}
