use crate::*;
use std::sync::Mutex;

use crate::common::ThinOption;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("item is oversized")]
    ItemOversized,
    #[error("ttl bucket expansion failed, no free segments")]
    NoFreeSegments,
}
