use crate::RawItem;

#[derive(Debug)]
pub struct ReservedItem {
    item: RawItem,
    seg: i32,
    offset: usize,
}

impl ReservedItem {
	pub fn new(item: RawItem, seg: i32, offset: usize) -> Self {
		Self {
			item,
			seg,
			offset
		}
	}
	
	pub fn check_magic(&self) {
		self.item.check_magic()
	}

	pub fn define(&mut self, key: &[u8], value: &[u8], optional: &[u8]) {
		self.item.define(key, value, optional)
	}

	pub fn item(&self) -> RawItem {
		self.item
	}

	pub fn offset(&self) -> usize {
		self.offset
	}

	pub fn seg(&self) -> i32 {
		self.seg
	}	
}