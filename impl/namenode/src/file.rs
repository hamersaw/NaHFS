use std::collections::HashMap;

pub struct File {
    pub inode: u64,
    pub name: String,

    pub file_type: i32, // 1 -> dir, 2 -> file
    pub permissions: u32,
    pub owner: String,
    pub group: String,

    pub blocks: Vec<u64>,
    pub block_replication: u32,
    pub block_size: u64,
}

impl File {
    pub fn directory(inode: u64, name: String, permissions: u32,
            owner: String, group: String) -> File {
        File {
            inode: inode,
            name: name,
            file_type: 1,
            permissions: permissions,
            owner: owner,
            group: group,
            blocks: Vec::new(),
            block_replication: 0,
            block_size: 0,
        }
    }

    pub fn regular(inode: u64, name: String, permissions: u32,
            owner: String, group: String, replication: u32,
            block_size: u64) -> File {
        File {
            inode: inode,
            name: name,
            file_type: 2,
            permissions: permissions,
            owner: owner,
            group: group,
            blocks: Vec::new(),
            block_replication: replication,
            block_size: block_size,
        }
    }
}

pub struct FileStore {
    inodes: HashMap<u64, File>,
    children: HashMap<u64, Vec<u64>>,
    parents: HashMap<u64, u64>,
}

impl FileStore {
    pub fn new() -> FileStore {
        // create variables
        let mut inodes = HashMap::new();
        let mut children = HashMap::new();

        // create root node
        inodes.insert(2, File::directory(2, "".to_string(), 0,
            "root".to_owned(), "root".to_owned()));
        children.insert(2, Vec::new());

        FileStore {
            inodes: inodes,
            children: children,
            parents: HashMap::new(),
        }
    }

    pub fn create(&mut self, path: &str, permissions: u32, owner: &str,
            group: &str, block_replication: u32, block_size: u64) {
        // find longest path match
        let components = parse_path(path);
        let (inode, match_length) = self.get_longest_match(&components);

        // check if directories are valid
        if match_length == components.len() {
            return; // directory already exists
        } else if components.len() >= 1
                && match_length < components.len() - 1 { 
            return; // need to, and unable to create parents
        }

        // create file
        let child_inode = rand::random::<u64>();
        let filename = components[components.len() - 1].to_string();
        let child_file = File::regular(child_inode, filename, 
            permissions, owner.to_string(), group.to_string(),
            block_replication, block_size);

        // update data inode data structures
        self.inodes.insert(child_inode, child_file);
        self.parents.insert(child_inode, inode);
        self.children.get_mut(&inode).unwrap().push(child_inode);
        self.children.insert(child_inode, Vec::new());
    }

    pub fn compute_path(&self, inode: u64) -> String {
        let mut path = String::new();
        let mut current_inode = inode;
        loop {
            // append inode to path
            let file = self.inodes.get(&current_inode).unwrap();
            path.insert_str(0, &format!("{}/", file.name));

            // get parent
            if self.parents.contains_key(&current_inode) {
                current_inode = *self.parents.get(&current_inode).unwrap();
            } else {
                break;
            }
        }

        path
    }

    pub fn get_file(&self, path: &str) -> Option<&File> {
        let components = parse_path(path);
        let (inode, match_length) = self.get_longest_match(&components);
        if match_length != components.len() {
            return None;
        }

        Some(self.inodes.get(&inode).unwrap())
    }

    pub fn get_file_mut(&self, path: &str) -> Option<&File> {
        unimplemented!();
    }

    pub fn get_children(&self, inode: u64) -> Option<Vec<&File>> {
        if !self.children.contains_key(&inode) {
            return None;
        }

        let mut children = Vec::new();
        for child_inode in self.children.get(&inode).unwrap() {
            children.push(self.inodes.get(child_inode).unwrap());
        }

        Some(children)
    }

    fn get_longest_match(&self, components: &Vec<&str>) -> (u64, usize) {
        let (mut inode, mut match_length) = (2, 0);
        for i in 0..components.len() {
            for child_inode in self.children.get(&inode).unwrap() {
                let child_file = self.inodes.get(child_inode).unwrap();
                if &child_file.name == components[i] {
                    inode = child_file.inode;
                    match_length += 1;
                }
            }

            if match_length != i + 1 {
                break;
            }
        }

        (inode, match_length)
    }

    pub fn mkdirs(&mut self, directory: &str, permissions: u32,
            owner: &str, group: &str, create_parent: bool) {
        // find longest path match
        let components = parse_path(directory);
        let (mut inode, match_length) = self.get_longest_match(&components);

        // check if directories are valid
        if match_length == components.len() {
            return; // directory already exists
        } else if components.len() >= 1 && match_length < components.len() - 1
                && !create_parent {
            return; // need to, and unable to create parents
        }

        // create directories
        for i in match_length..components.len() {
            // initialize child file
            let child_inode = rand::random::<u64>();
            let child_file = File::directory(child_inode,
                components[i].to_string(), permissions,
                owner.to_string(), group.to_string());

            // update data inode data structures
            self.inodes.insert(child_inode, child_file);
            self.parents.insert(child_inode, inode);
            self.children.get_mut(&inode).unwrap().push(child_inode);
            self.children.insert(child_inode, Vec::new());

            inode = child_inode;
        }
    }

    pub fn rename(src_path: &str, dst_path: &str) {
        unimplemented!();
    }
}

fn parse_path(path: &str) -> Vec<&str> {
    let mut components: Vec<&str> = path.split("/").collect();
    while components.len() > 0 && components[0] == "" {
        components.remove(0);
    }

    components
}
