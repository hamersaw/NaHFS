use std::collections::HashMap;

mod processor;
pub use processor::BlockProcessor;

pub struct BlockIndex {
    start_timestamp: u64,
    end_timestamp: u64,
    geohashes: Vec<(String, u32, u32)>,
}

pub struct Block {
    block_id: u64,
    index: Option<BlockIndex>
}

impl Block {
    pub fn new(block_id: u64) -> Block {
        Block {
            block_id: block_id,
            index: None,
        }
    }
}

pub struct BlockStore {
    map: HashMap<u64, Block>,
}

impl BlockStore {
    pub fn new() -> BlockStore {
        BlockStore {
            map: HashMap::new(),
        }
    }
}
