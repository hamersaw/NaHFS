use std::collections::HashMap;

mod processor;
pub use processor::BlockProcessor;

pub struct Block {
    block_id: u64,
    index: Vec<(String, u32, u32)>
}

impl Block {
    pub fn new(block_id: u64) -> Block {
        Block {
            block_id: block_id,
            index: Vec::new(),
        }
    }
}

pub struct BlockIndex {
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
