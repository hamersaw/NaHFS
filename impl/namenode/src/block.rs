use std::collections::HashMap;

pub struct Block {
    id: u64,
    generation_stamp: u64,
    length: u64,
    locations: Vec<String>,
    storage_ids: Vec<String>,
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

    pub fn update(&mut self, id: u64, generation_stamp: u64,
            length: u64, datanode_id: &str, storage_id: &str) {
        // get block, creating if it doesn't exist
        let mut block = self.map.entry(id).or_insert(
            Block {
                id: id,
                generation_stamp: generation_stamp,
                length: 0,
                locations: Vec::new(),
                storage_ids: Vec::new(),
            });
 
        // return if datanode is already registered
        for value in block.locations.iter() {
            if value == &datanode_id {
                return;
            }
        }

        // update block
        debug!("updated block '{}' with location {}:{} and length {}",
            block.id, &datanode_id, &storage_id, length);
        block.locations.push(datanode_id.to_owned());
        block.storage_ids.push(storage_id.to_owned());
        block.length = length;
    }
}
