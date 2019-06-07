use serde::{Deserialize, Serialize};

use crate::file::{File, FileType};

use std::collections::HashMap;

#[derive(Deserialize, Serialize)]
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
        let root_inode = File::new(2, FileType::directory(),
            "".to_string(), "root".to_string(),
            "root".to_string(), 0, None);
        inodes.insert(2, root_inode);
        children.insert(2, Vec::new());

        FileStore {
            inodes: inodes,
            children: children,
            parents: HashMap::new(),
        }
    }

    pub fn create(&mut self, path: &str, permissions: u32, owner: &str,
            group: &str, replication: u32, block_size: u64) {
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
        let child_file = File::new(child_inode,
            FileType::regular(replication, block_size), filename, 
            owner.to_string(), group.to_string(), permissions, None);

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

        if path.len() > 1 {
            let _ = path.pop();
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

    pub fn get_file_mut(&mut self, path: &str) -> Option<&mut File> {
        let components = parse_path(path);
        let (inode, match_length) = self.get_longest_match(&components);
        if match_length != components.len() {
            return None;
        }

        Some(self.inodes.get_mut(&inode).unwrap())
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

    pub fn get_storage_policy(&self, inode: &u64) -> Option<&str> {
        let mut current_inode = inode;
        loop {
            if let Some(file) = self.inodes.get(current_inode) {
                if let Some(storage_policy) = file.get_storage_policy() {
                    return Some(storage_policy);
                }
            }
 
            // set current inode to parent
            if self.parents.contains_key(current_inode) {
                current_inode = self.parents.get(current_inode).unwrap();
            } else {
                break;
            }
        }

        None
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
            let child_file = File::new(child_inode,
                FileType::directory(), components[i].to_string(),
                owner.to_string(), group.to_string(), permissions, None);

            // update data inode data structures
            self.inodes.insert(child_inode, child_file);
            self.parents.insert(child_inode, inode);
            self.children.get_mut(&inode).unwrap().push(child_inode);
            self.children.insert(child_inode, Vec::new());

            inode = child_inode;
        }
    }

    pub fn rename(&mut self, src_path: &str, dst_path: &str) {
        // compute src path components
        let src_components = parse_path(src_path);
        let (src_inode, src_match_length) =
            self.get_longest_match(&src_components);

        if src_match_length != src_components.len() {
            return; // file does not exist
        }

        // compute dst path components
        let dst_components = parse_path(dst_path);
        let (dst_inode, dst_match_length) =
            self.get_longest_match(&dst_components);

        if dst_match_length != dst_components.len() - 1 {
            return; // destination directory does not exist
        }

        // remove src file from children and parents
        let parent_inode = self.parents
            .get(&src_inode).unwrap().to_owned();
        self.parents.remove(&src_inode);
        let children = self.children.get_mut(&parent_inode).unwrap();
        let mut index = 0;
        for (i, value) in children.iter().enumerate() {
            if value == &src_inode {
                index = i;
            }
        }

        children.remove(index);
        
        // add dst file to children and parents
        self.children.get_mut(&dst_inode).unwrap().push(src_inode);
        self.parents.insert(src_inode, dst_inode);

        // change file name
        let mut file = self.inodes.get_mut(&src_inode).unwrap();
        file.name = dst_components.last().unwrap().to_string();
    }

    pub fn set_storage_policy(&mut self, path: &str, storage_policy: &str) {
        let components = parse_path(path);
        let (inode, match_length) = self.get_longest_match(&components);
        if match_length != components.len() {
            return;
        }

        let mut file = self.inodes.get_mut(&inode).unwrap();
        file.storage_policy = Some(storage_policy.to_string());
    }
}

fn parse_path(path: &str) -> Vec<&str> {
    let mut components: Vec<&str> = path.split("/").collect();
    while components.len() > 0 && components[0] == "" {
        components.remove(0);
    }

    components
}
