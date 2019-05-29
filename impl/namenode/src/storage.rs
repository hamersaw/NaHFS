use std::collections::HashMap;

pub struct Storage {
    pub id: String,
    pub states: Vec<StorageState>,
}

pub struct StorageState {
    pub capacity: Option<u64>,
    pub dfs_used: Option<u64>,
    pub remaining: Option<u64>,
    pub block_pool_used: Option<u64>,
    pub non_dfs_used: Option<u64>,
    pub update_timestamp: u64,
}

pub struct StorageStore {
    map: HashMap<String, Storage>,
    state_queue_length: usize,
}

impl StorageStore {
    pub fn new(state_queue_length: usize) -> StorageStore {
        StorageStore {
            map: HashMap::new(),
            state_queue_length: state_queue_length,
        }
    }

    pub fn get_storage(&self, id: &str) -> Option<&Storage> {
        Some(self.map.get(id).unwrap())
    }

    pub fn update(&mut self, id: &str, capacity: Option<u64>,
            dfs_used: Option<u64>, remaining: Option<u64>,
            block_pool_used: Option<u64>, non_dfs_used: Option<u64>,
            update_timestamp: u64) {
        // get storage, creating if it doesn't exist
        let storage = self.map.entry(id.to_string()).or_insert(
            Storage {
                id: id.to_string(),
                states: Vec::new(),
            });

        // create and add state
        let state = StorageState {
            capacity: capacity,
            dfs_used: dfs_used,
            remaining: remaining,
            block_pool_used: block_pool_used,
            non_dfs_used: non_dfs_used,
            update_timestamp: update_timestamp,
        };

        storage.states.push(state);

        // remove old states
        while storage.states.len() > self.state_queue_length {
            storage.states.remove(0);
        }
    }
}
