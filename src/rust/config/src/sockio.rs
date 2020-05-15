const BUFSOCK_POOLSIZE: usize = 0;

pub struct SockioConfig {
	buf_sock_poolsize: usize,
}

impl Default for SockioConfig {
	buf_sock_poolsize: BUFSOCK_POOLSIZE,
}

impl SockioConfig {
	pub fn buf_sock_poolsize(&self) -> usize {
		self.buf_sock_poolsize
	}
}