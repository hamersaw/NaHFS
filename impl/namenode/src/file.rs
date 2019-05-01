use std::collections::HashMap;

pub struct File {
    inode: u64,
    name: String,
}

pub struct FileStore {
    inodes: HashMap<u64, File>,
    children: HashMap<u64, Vec<u64>>,
    parents: HashMap<u64, u64>,
}

impl FileStore {
    pub fn new() -> FileStore {
        // TODO - create root inode

        FileStore {
            inodes: HashMap::new(),
            children: HashMap::new(),
            parents: HashMap::new(),
        }
    }

    pub fn path(inode: u64) -> String {
        unimplemented!();
    }
}
