use std::collections::HashMap;

pub struct Datanode {

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
}
