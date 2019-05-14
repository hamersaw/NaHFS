use std::collections::HashMap;

mod processor;
pub use processor::BlockProcessor;

pub struct Block {
    block_id: u64,
    data: Vec<u8>,
    index: Option<BlockIndex>,
}

impl Block {
    pub fn new(block_id: u64, data: Vec<u8>) -> Block {
        Block {
            block_id: block_id,
            data: data,
            index: None,
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
