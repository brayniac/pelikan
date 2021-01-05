// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use bytes::BytesMut;
use std::borrow::Borrow;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Verb {
    Get,
    Set,
    Cas,
    Add,
    Replace,
    Delete,
}

// TODO(bmartin): evaluate if we take any performance hit from using a request
// trait here instead. Trying to avoid a Box<>. Another option might be a
// unified type, but that moves some of the complexity back into the request
// handling.
#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    Get(GetRequest),
    Set(SetRequest),
    Cas(CasRequest),
    Add(AddRequest),
    Replace(ReplaceRequest),
    Delete(DeleteRequest),
}

#[derive(PartialEq, Eq, Debug)]
pub enum ParseError {
    Incomplete,
    Invalid,
    UnknownCommand,
}

#[derive(PartialEq, Eq, Debug)]
pub struct GetRequest {
    data: BytesMut,
    key_indices: Vec<(usize, usize)>,
}

impl GetRequest {
    pub fn keys(&self) -> Vec<&[u8]> {
        let data: &[u8] = self.data.borrow();
        let mut keys = Vec::new();
        for key_index in &self.key_indices {
            keys.push(&data[key_index.0..key_index.1])
        }
        keys
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct DeleteRequest {
    data: BytesMut,
    key_index: (usize, usize),
    noreply: bool,
}

impl DeleteRequest {
    pub fn key(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.key_index.0..self.key_index.1]
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct SetRequest {
    data: BytesMut,
    key_index: (usize, usize),
    expiry: u32,
    flags: u32,
    noreply: bool,
    value_index: (usize, usize),
}

impl SetRequest {
    pub fn key(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.key_index.0..self.key_index.1]
    }

    pub fn value(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.value_index.0..self.value_index.1]
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }

    pub fn expiry(&self) -> u32 {
        self.expiry
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct CasRequest {
    data: BytesMut,
    key_index: (usize, usize),
    expiry: u32,
    flags: u32,
    cas: u64,
    noreply: bool,
    value_index: (usize, usize),
}

impl CasRequest {
    pub fn key(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.key_index.0..self.key_index.1]
    }

    pub fn value(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.value_index.0..self.value_index.1]
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }

    pub fn expiry(&self) -> u32 {
        self.expiry
    }

    pub fn cas(&self) -> u64 {
        self.cas
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct AddRequest {
    data: BytesMut,
    key_index: (usize, usize),
    expiry: u32,
    flags: u32,
    noreply: bool,
    value_index: (usize, usize),
}

impl AddRequest {
    pub fn key(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.key_index.0..self.key_index.1]
    }

    pub fn value(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.value_index.0..self.value_index.1]
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }

    pub fn expiry(&self) -> u32 {
        self.expiry
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct ReplaceRequest {
    data: BytesMut,
    key_index: (usize, usize),
    expiry: u32,
    flags: u32,
    noreply: bool,
    value_index: (usize, usize),
}

impl ReplaceRequest {
    pub fn key(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.key_index.0..self.key_index.1]
    }

    pub fn value(&self) -> &[u8] {
        let data: &[u8] = self.data.borrow();
        &data[self.value_index.0..self.value_index.1]
    }

    pub fn flags(&self) -> u32 {
        self.flags
    }

    pub fn noreply(&self) -> bool {
        self.noreply
    }

    pub fn expiry(&self) -> u32 {
        self.expiry
    }
}

// TODO(bmartin): consider splitting the parsing functions up, cognitive
// complexity is a little high for this function. Currently this way to re-use
// the window iterators and avoid re-parsing the buffer once into command
// specific parsing.
pub fn parse(buffer: &mut BytesMut) -> Result<Request, ParseError> {
    // no-copy borrow as a slice
    let buf: &[u8] = (*buffer).borrow();

    // check if we got a CRLF
    let mut double_byte_windows = buf.windows(2);
    if let Some(command_end) = double_byte_windows.position(|w| w == b"\r\n") {
        // single-byte windowing to find spaces
        let mut single_byte_windows = buf.windows(1);
        if let Some(command_verb_end) = single_byte_windows.position(|w| w == b" ") {
            let verb = match &buf[0..command_verb_end] {
                b"get" => Verb::Get,
                b"set" => Verb::Set,
                b"cas" => Verb::Cas,
                b"add" => Verb::Add,
                b"replace" => Verb::Replace,
                b"delete" => Verb::Delete,
                _ => {
                    return Err(ParseError::UnknownCommand);
                }
            };
            match verb {
                Verb::Get => {
                    let mut previous = command_verb_end + 1;
                    let mut keys = Vec::new();

                    // command may have multiple keys, we need to loop until we hit
                    // a CRLF
                    loop {
                        if let Some(key_end) = single_byte_windows.position(|w| w == b" ") {
                            if key_end < command_end {
                                keys.push((previous, key_end));
                                previous = key_end + 1;
                            } else {
                                keys.push((previous, command_end));
                                break;
                            }
                        } else {
                            keys.push((previous, command_end));
                            break;
                        }
                    }

                    let data = buffer.split_to(command_end + 2);
                    Ok(Request::Get(GetRequest {
                        data,
                        key_indices: keys,
                    }))
                }
                Verb::Cas => {
                    let key_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + command_verb_end
                        + 1;

                    let flags_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + key_end
                        + 1;
                    let flags_str = std::str::from_utf8(&buf[(key_end + 1)..flags_end])
                        .map_err(|_| ParseError::Invalid)?;
                    let flags = flags_str.parse().map_err(|_| ParseError::Invalid)?;

                    let expiry_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + flags_end
                        + 1;
                    let expiry_str = std::str::from_utf8(&buf[(flags_end + 1)..expiry_end])
                        .map_err(|_| ParseError::Invalid)?;
                    let expiry = expiry_str.parse().map_err(|_| ParseError::Invalid)?;

                    let bytes_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + expiry_end
                        + 1;
                    let bytes_str = std::str::from_utf8(&buf[(expiry_end + 1)..bytes_end])
                        .map_err(|_| ParseError::Invalid)?;
                    let bytes = bytes_str
                        .parse::<usize>()
                        .map_err(|_| ParseError::Invalid)?;

                    // now it gets tricky, we either have "[bytes] noreply\r\n" or "[bytes]\r\n"
                    let mut double_byte_windows = buf.windows(2);
                    let mut noreply = false;

                    // get the position of the next space and first CRLF
                    let next_space = single_byte_windows
                        .position(|w| w == b" ")
                        .map(|v| v + expiry_end + 1);
                    let first_crlf = double_byte_windows
                        .position(|w| w == b"\r\n")
                        .ok_or(ParseError::Incomplete)?;

                    let cas_end = if let Some(next_space) = next_space {
                        // if we have both, bytes_end is before the earlier of the two
                        if next_space < first_crlf {
                            // validate that noreply isn't malformed
                            if &buf[(next_space + 1)..(first_crlf)] == b"noreply" {
                                noreply = true;
                                next_space
                            } else {
                                return Err(ParseError::Invalid);
                            }
                        } else {
                            first_crlf
                        }
                    } else {
                        first_crlf
                    };

                    if let Ok(Ok(cas)) = std::str::from_utf8(&buf[(bytes_end + 1)..cas_end])
                        .map(|v| v.parse::<u64>())
                    {
                        let data_end = first_crlf + 2 + bytes + 2;
                        if buf.len() >= data_end {
                            let data = buffer.split_to(data_end);
                            Ok(Request::Cas(CasRequest {
                                data,
                                key_index: ((command_verb_end + 1), key_end),
                                flags,
                                expiry,
                                noreply,
                                cas,
                                value_index: ((first_crlf + 2), (first_crlf + 2 + bytes)),
                            }))
                        } else {
                            Err(ParseError::Incomplete)
                        }
                    } else {
                        Err(ParseError::Invalid)
                    }
                }
                Verb::Set | Verb::Add | Verb::Replace => {
                    let key_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + command_verb_end
                        + 1;

                    let flags_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + key_end
                        + 1;
                    let flags_str = std::str::from_utf8(&buf[(key_end + 1)..flags_end])
                        .map_err(|_| ParseError::Invalid)?;
                    let flags = flags_str.parse().map_err(|_| ParseError::Invalid)?;

                    let expiry_end = single_byte_windows
                        .position(|w| w == b" ")
                        .ok_or(ParseError::Incomplete)?
                        + flags_end
                        + 1;
                    let expiry_str = std::str::from_utf8(&buf[(flags_end + 1)..expiry_end])
                        .map_err(|_| ParseError::Invalid)?;
                    let expiry = expiry_str.parse().map_err(|_| ParseError::Invalid)?;

                    // now it gets tricky, we either have "[bytes] noreply\r\n" or "[bytes]\r\n"
                    let mut double_byte_windows = buf.windows(2);
                    let mut noreply = false;

                    // get the position of the next space and first CRLF
                    let next_space = single_byte_windows
                        .position(|w| w == b" ")
                        .map(|v| v + expiry_end + 1);
                    let first_crlf = double_byte_windows
                        .position(|w| w == b"\r\n")
                        .ok_or(ParseError::Incomplete)?;

                    let bytes_end = if let Some(next_space) = next_space {
                        // if we have both, bytes_end is before the earlier of the two
                        if next_space < first_crlf {
                            // validate that noreply isn't malformed
                            if &buf[(next_space + 1)..(first_crlf)] == b"noreply" {
                                noreply = true;
                                next_space
                            } else {
                                return Err(ParseError::Invalid);
                            }
                        } else {
                            first_crlf
                        }
                    } else {
                        first_crlf
                    };

                    if let Ok(Ok(bytes)) = std::str::from_utf8(&buf[(expiry_end + 1)..bytes_end])
                        .map(|v| v.parse::<usize>())
                    {
                        let data_end = first_crlf + 2 + bytes + 2;
                        if buf.len() >= data_end {
                            let data = buffer.split_to(data_end);

                            Ok(match verb {
                                Verb::Set => Request::Set(SetRequest {
                                    data,
                                    key_index: ((command_verb_end + 1), key_end),
                                    flags,
                                    expiry,
                                    noreply,
                                    value_index: ((first_crlf + 2), (first_crlf + 2 + bytes)),
                                }),
                                Verb::Add => Request::Add(AddRequest {
                                    data,
                                    key_index: ((command_verb_end + 1), key_end),
                                    flags,
                                    expiry,
                                    noreply,
                                    value_index: ((first_crlf + 2), (first_crlf + 2 + bytes)),
                                }),
                                Verb::Replace => Request::Replace(ReplaceRequest {
                                    data,
                                    key_index: ((command_verb_end + 1), key_end),
                                    flags,
                                    expiry,
                                    noreply,
                                    value_index: ((first_crlf + 2), (first_crlf + 2 + bytes)),
                                }),
                                _ => {
                                    // we already matched on the verb before parsing to restrict cases handled
                                    // anything not covered in this match is unreachable
                                    unreachable!()
                                }
                            })
                        } else {
                            Err(ParseError::Incomplete)
                        }
                    } else {
                        Err(ParseError::Invalid)
                    }
                }
                Verb::Delete => {
                    let mut noreply = false;
                    let mut double_byte_windows = buf.windows(2);
                    // get the position of the next space and first CRLF
                    let next_space = single_byte_windows
                        .position(|w| w == b" ")
                        .map(|v| v + command_verb_end + 1);
                    let first_crlf = double_byte_windows
                        .position(|w| w == b"\r\n")
                        .ok_or(ParseError::Incomplete)?;

                    let key_end = if let Some(next_space) = next_space {
                        // if we have both, bytes_end is before the earlier of the two
                        if next_space < first_crlf {
                            // validate that noreply isn't malformed
                            if &buf[(next_space + 1)..(first_crlf)] == b"noreply" {
                                noreply = true;
                                next_space
                            } else {
                                return Err(ParseError::Invalid);
                            }
                        } else {
                            first_crlf
                        }
                    } else {
                        first_crlf
                    };

                    let command_end = if noreply { key_end + 9 } else { key_end + 2 };

                    let data = buffer.split_to(command_end);

                    Ok(Request::Delete(DeleteRequest {
                        data,
                        key_index: ((command_verb_end + 1), key_end),
                        noreply,
                    }))
                }
            }
        } else {
            Err(ParseError::UnknownCommand)
        }
    } else {
        Err(ParseError::Incomplete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Vec<&'static [u8]> {
        vec![b"0", b"1", b"0123456789", b"A"]
    }

    fn values() -> Vec<&'static [u8]> {
        vec![b"0", b"1", b"0123456789", b"A"]
    }

    fn flags() -> Vec<u32> {
        vec![0, 1, u32::MAX]
    }

    #[test]
    fn parse_incomplete() {
        let buffers: Vec<&[u8]> = vec![
            b"",
            b"get",
            b"get ",
            b"get 0",
            b"get 0\r",
            b"set 0",
            b"set 0 0 0 1",
            b"set 0 0 0 1\r\n",
            b"set 0 0 0 1\r\n1",
            b"set 0 0 0 1\r\n1\r",
            b"set 0 0 0 3\r\n1\r\n\r",
        ];
        for mut buffer in buffers.iter().map(|v| BytesMut::from(&v[..])) {
            assert_eq!(parse(&mut buffer), Err(ParseError::Incomplete));
        }
    }

    #[test]
    fn parse_get() {
        for key in keys() {
            let mut buffer = BytesMut::new();
            buffer.extend_from_slice(b"get ");
            buffer.extend_from_slice(key);
            buffer.extend_from_slice(b"\r\n");
            let parsed = parse(&mut buffer);
            assert!(parsed.is_ok());
            if let Ok(Request::Get(get_request)) = parsed {
                assert_eq!(get_request.keys(), vec![key]);
            } else {
                panic!("incorrectly parsed");
            }
        }
    }

    // TODO(bmartin): test multi-get

    #[test]
    fn parse_set() {
        for key in keys() {
            for value in values() {
                for flag in flags() {
                    let mut buffer = BytesMut::new();
                    buffer.extend_from_slice(b"set ");
                    buffer.extend_from_slice(key);
                    buffer.extend_from_slice(format!(" {} 0 {}\r\n", flag, value.len()).as_bytes());
                    buffer.extend_from_slice(value);
                    buffer.extend_from_slice(b"\r\n");
                    let parsed = parse(&mut buffer);
                    assert!(parsed.is_ok());
                    if let Ok(Request::Set(set_request)) = parsed {
                        assert_eq!(set_request.key(), key);
                        assert_eq!(set_request.value(), value);
                        assert_eq!(set_request.flags(), flag);
                    } else {
                        panic!("incorrectly parsed");
                    }
                }
            }
        }
    }

    // TODO(bmartin): add test for add / replace / delete
}
