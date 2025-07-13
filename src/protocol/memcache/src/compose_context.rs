// Copyright 2022 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

/// Minimal context needed for composing responses
/// This replaces the need to pass the entire Request to compose_response
#[derive(Debug, Clone, Copy)]
pub struct ComposeContext {
    /// For binary protocol: opaque field to echo back
    pub opaque: Option<u32>,
    
    /// For GET requests: whether CAS was requested
    pub cas: bool,
    
    /// For binary GET: whether to include key in response
    pub include_key: bool,
    
    /// For metrics: number of keys requested (for miss counting)
    pub key_count: usize,
    
    /// Request type hint for metrics
    pub request_type: RequestType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestType {
    Get,
    Gets,
    Set,
    Add,
    Replace,
    Append,
    Prepend,
    Delete,
    Incr,
    Decr,
    Cas,
    FlushAll,
    Quit,
}

impl ComposeContext {
    /// Create a simple context for requests that don't need special handling
    pub fn simple(request_type: RequestType) -> Self {
        Self {
            opaque: None,
            cas: false,
            include_key: false,
            key_count: 0,
            request_type,
        }
    }
    
    /// Create context for GET/GETS requests
    pub fn for_get(key_count: usize, cas: bool, opaque: Option<u32>, include_key: bool) -> Self {
        Self {
            opaque,
            cas,
            include_key,
            key_count,
            request_type: if cas { RequestType::Gets } else { RequestType::Get },
        }
    }
    
    /// Create context with just opaque for binary protocol
    pub fn with_opaque(request_type: RequestType, opaque: Option<u32>) -> Self {
        Self {
            opaque,
            cas: false,
            include_key: false,
            key_count: 0,
            request_type,
        }
    }
}

/// Trait for responses that can be composed with just a context
pub trait ComposeWithContext {
    /// Compose the response using minimal context instead of full request
    fn compose_with_context(
        &self,
        context: &ComposeContext,
        buffer: &mut dyn protocol_common::BufMut,
    ) -> usize;
}

/// Extension trait to create compose context from requests
impl From<&crate::Request> for ComposeContext {
    fn from(request: &crate::Request) -> Self {
        match request {
            crate::Request::Get(get) => ComposeContext::for_get(
                get.keys().len(),
                get.cas(),
                get.opaque,
                get.key,
            ),
            crate::Request::Set(set) => ComposeContext::with_opaque(RequestType::Set, set.opaque),
            crate::Request::Add(_) => ComposeContext::simple(RequestType::Add),
            crate::Request::Replace(_) => ComposeContext::simple(RequestType::Replace),
            crate::Request::Append(_) => ComposeContext::simple(RequestType::Append),
            crate::Request::Prepend(_) => ComposeContext::simple(RequestType::Prepend),
            crate::Request::Delete(delete) => ComposeContext::with_opaque(RequestType::Delete, delete.opaque),
            crate::Request::Incr(_) => ComposeContext::simple(RequestType::Incr),
            crate::Request::Decr(_) => ComposeContext::simple(RequestType::Decr),
            crate::Request::Cas(_) => ComposeContext::simple(RequestType::Cas),
            crate::Request::FlushAll(_) => ComposeContext::simple(RequestType::FlushAll),
            crate::Request::Quit(_) => ComposeContext::simple(RequestType::Quit),
        }
    }
}