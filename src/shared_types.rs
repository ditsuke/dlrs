#[derive(Clone, Copy, Debug)]
pub(crate) struct ChunkRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

pub(crate) type ByteCount = u64;
