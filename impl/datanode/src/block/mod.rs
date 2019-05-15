use std::collections::HashMap;

mod processor;
pub use processor::BlockProcessor;

use std::collections::BTreeMap;

pub enum Operation {
    INDEX,
    WRITE,
    TRANSFER,
}

pub struct BlockOperation {
    operation: Operation,
    block_id: u64,
    data: Vec<u8>, 
    index: Option<BTreeMap<String, Vec<(usize, usize)>>>,
}

impl BlockOperation {
    pub fn new(operation: Operation, block_id: u64,
            data: Vec<u8>) -> BlockOperation {
        BlockOperation {
            operation: operation,
            block_id: block_id,
            data: data,
            index: None,
        }
    }
}
