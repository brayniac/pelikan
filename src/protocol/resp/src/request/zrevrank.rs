// Copyright 2025 Pelikan Foundation LLC.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::io::Error;
use std::sync::Arc;

use super::*;

#[metric(name = "zrevrank")]
pub static ZREVRANK: Counter = Counter::new();

#[metric(name = "zrevrank_ex")]
pub static ZREVRANK_EX: Counter = Counter::new();

#[metric(name = "zrevrank_hit")]
pub static ZREVRANK_HIT: Counter = Counter::new();

#[metric(name = "zrevrank_miss")]
pub static ZREVRANK_MISS: Counter = Counter::new();

#[derive(Debug, PartialEq, Eq)]
pub struct SortedSetReverseRank {
    key: Arc<[u8]>,
    member: Arc<[u8]>,
    with_score: bool,
}

impl TryFrom<Message> for SortedSetReverseRank {
    type Error = Error;

    fn try_from(other: Message) -> Result<Self, Error> {
        let array = match other {
            Message::Array(array) => array,
            _ => return Err(Error::new(ErrorKind::Other, "malformed command")),
        };

        if array.inner.is_none() {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let mut array = array.inner.unwrap();

        if !(3..=4).contains(&array.len()) {
            return Err(Error::new(ErrorKind::Other, "malformed command"));
        }

        let with_score = array.len() == 4;
        let _command = take_bulk_string(&mut array)?;
        let key = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;
        let member = take_bulk_string(&mut array)?
            .ok_or_else(|| Error::new(ErrorKind::Other, "malformed command"))?;

        Ok(Self {
            key,
            member,
            with_score,
        })
    }
}

impl SortedSetReverseRank {
    pub fn new(key: &[u8], member: &[u8], with_score: bool) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
            with_score,
        }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn member(&self) -> &[u8] {
        &self.member
    }

    pub fn with_score(&self) -> bool {
        self.with_score
    }
}

impl From<&SortedSetReverseRank> for Message {
    fn from(value: &SortedSetReverseRank) -> Message {
        Message::Array(Array {
            inner: Some(vec![
                Message::BulkString(BulkString::new(b"ZREVRANK")),
                Message::BulkString(BulkString::new(value.key())),
                Message::BulkString(BulkString::new(value.member())),
                if value.with_score {
                    Message::BulkString(BulkString::new(b"WITHSCORE"))
                } else {
                    Message::BulkString(BulkString::new(b""))
                },
            ]),
        })
    }
}

impl Compose for SortedSetReverseRank {
    fn compose(&self, buf: &mut dyn BufMut) -> usize {
        Message::from(self).compose(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser() {
        let parser = RequestParser::new();
        assert_eq!(
            parser.parse(b"ZREVRANK z a\r\n").unwrap().into_inner(),
            Request::SortedSetReverseRank(SortedSetReverseRank::new(b"z", b"a", false))
        );

        assert_eq!(
            parser
                .parse(b"ZREVRANK z a WITHSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetReverseRank(SortedSetReverseRank::new(b"z", b"a", true))
        );

        assert_eq!(
            parser
                .parse(b"*4\r\n$8\r\nZREVRANK\r\n$1\r\nz\r\n$1\r\na\r\n$9\r\nWITHSCORE\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetReverseRank(SortedSetReverseRank::new(b"z", b"a", true))
        );

        assert_eq!(
            parser
                .parse(b"*3\r\n$8\r\nZREVRANK\r\n$1\r\nz\r\n$1\r\na\r\n")
                .unwrap()
                .into_inner(),
            Request::SortedSetReverseRank(SortedSetReverseRank::new(b"z", b"a", false))
        );
    }
}
