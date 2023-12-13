use crate::*;

#[derive(Clone)]
pub struct Message {
    pub data: Vec<u8>,
}

impl Message {
    pub fn new(hash_builder: &RandomState, len: u32) -> Self {
        let now = SystemTime::now();

        let unix_ns = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let mut data = vec![0; len as usize];

        data[0..4].copy_from_slice(&len.to_be_bytes());
        data[8..16].copy_from_slice(&[0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21]);
        data[24..32].copy_from_slice(&unix_ns.to_be_bytes());

        let checksum = hash_builder.hash_one(&data[8..len as usize]).to_be_bytes();
        data[16..24].copy_from_slice(&checksum);

        Self { data }
    }
}
