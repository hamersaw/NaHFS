use std::collections::HashMap;

pub struct Storage {
    id: String,
    states: Vec<StorageState>,
}

pub struct StorageState {
    capacity: Option<u64>,
    dfs_used: Option<u64>,
    remaining: Option<u64>,
    update_timestamp: u64,
}

pub struct StorageStore {
    map: HashMap<String, Storage>,
}

impl StorageStore {
    pub fn new() -> StorageStore {
        StorageStore {
            map: HashMap::new(),
        }
    }

    pub fn update(&mut self, id: String, capacity: Option<u64>,
            dfs_used: Option<u64>, remaining: Option<u64>,
            update_timestamp: u64) {
        // get storage, creating if it doesn't exist
        let mut storage = self.map.entry(id.clone()).or_insert(
            Storage {
                id: id,
                states: Vec::new(),
            });

        // create and add state
        let state = StorageState {
            capacity: capacity,
            dfs_used: dfs_used,
            remaining: remaining,
            update_timestamp: update_timestamp,
        };

        storage.states.push(state);

        // remove old states
        while storage.states.len() > 10 {
            storage.states.remove(0);
        }
    }
}
