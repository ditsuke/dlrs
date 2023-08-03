#[derive(Clone, Copy, Debug)]
pub(crate) struct ChunkRange {
    pub(crate) start: u32,
    pub(crate) end: u32,
}

pub(crate) type ByteCount = u64;
