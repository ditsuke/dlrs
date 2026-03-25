use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

pub(crate) type ByteCount = u64;
