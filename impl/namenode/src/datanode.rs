use std::collections::HashMap;

pub struct Datanode {
    pub id: String,
    pub ip_address: String,
    pub xfer_port: u32,
    pub storage_ids: Vec<String>,
    pub states: Vec<DatanodeState>,
}

pub struct DatanodeState {
    pub cache_capacity: Option<u64>,
    pub cache_used: Option<u64>,
    pub update_timestamp: u64,
    pub xmits_in_progress: Option<u32>,
    pub xceiver_count: Option<u32>,
}

pub struct DatanodeStore {
    map: HashMap<String, Datanode>,
}

impl DatanodeStore {
    pub fn new() -> DatanodeStore {
        DatanodeStore {
            map: HashMap::new(),
        }
    }

    pub fn add_storage(&mut self, id: &str, storage_id: &str) {
        if let Some(mut datanode) = self.map.get_mut(id) {
            for value in datanode.storage_ids.iter() {
                if value == storage_id {
                    return;
                }
            }

            datanode.storage_ids.push(storage_id.to_string());
        }
    }

    pub fn register(&mut self, id: String,
            ip_address: String, xfer_port: u32) {
        info!("registering datanode '{}' as {}:{}", id, ip_address, xfer_port);

        let id_clone = id.clone();
        let datanode = Datanode {
            id: id,
            ip_address: ip_address,
            xfer_port: xfer_port,
            storage_ids: Vec::new(),
            states: Vec::new(),
        };
        self.map.insert(id_clone, datanode);
    }

    pub fn get_datanode(&self, id: &str) -> Option<&Datanode> {
        self.map.get(id)
    }

    pub fn get_random_ids(&self, count: u32) -> Vec<&String> {
        let mut ids = Vec::new();
        'a: while (ids.len() as u32) < count
                && ids.len() != self.map.len() {
            // find random id
            let index = rand::random::<usize>() % self.map.len();
            let id = self.map.keys().nth(index).unwrap();

            // check if id is already added
            for value in ids.iter() {
                if value == &id {
                    continue 'a;
                }
            }

            // add id
            ids.push(id);
        }

        ids
    }

    pub fn update(&mut self, id: &str, cache_capacity: Option<u64>,
            cache_used: Option<u64>, update_timestamp: u64,
            xmits_in_progress: Option<u32>, xceiver_count: Option<u32>) {
        if let Some(mut datanode) = self.map.get_mut(id) {
            // create and add state
            let state = DatanodeState {
                cache_capacity: cache_capacity,
                cache_used: cache_used,
                update_timestamp: update_timestamp,
                xmits_in_progress: xmits_in_progress,
                xceiver_count: xceiver_count,
            };

            datanode.states.push(state);

            // remove old states
            while datanode.states.len() > 10 {
                datanode.states.remove(0);
            }
        }
    }
}
