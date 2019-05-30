use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use shared::NahError;

use std::collections::HashMap;
use std::io::{Read, Write};

pub struct File {
    pub inode: u64,
    pub name: String,

    pub file_type: i32, // 1 -> dir, 2 -> file
    pub permissions: u32,
    pub owner: String,
    pub group: String,
    pub storage_policy: Option<String>,

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
            storage_policy: None,
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
            storage_policy: None,
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

    pub fn from(mut reader: impl Read) -> Result<FileStore, NahError> {
        // read inodes
        let mut inodes = HashMap::new();
        for _ in 0..reader.read_u32::<BigEndian>()? {
            // read file
            let inode = reader.read_u64::<BigEndian>()?;
            let mut name_buf = vec![0u8; reader.read_u8()? as usize];
            reader.read_exact(&mut name_buf)?;
            let name = String::from_utf8_lossy(&name_buf);

            let file_type = reader.read_i32::<BigEndian>()?;
            let permissions = reader.read_u32::<BigEndian>()?;
            let mut owner_buf = vec![0u8; reader.read_u8()? as usize];
            reader.read_exact(&mut owner_buf)?;
            let owner = String::from_utf8_lossy(&owner_buf);
            let mut group_buf = vec![0u8; reader.read_u8()? as usize];
            reader.read_exact(&mut group_buf)?;
            let group = String::from_utf8_lossy(&group_buf);
            let mut storage_policy_buf
                = vec![0u8; reader.read_u8()? as usize];
            let storage_policy = match storage_policy_buf.len() {
                0 => None,
                _ => {
                    reader.read_exact(&mut storage_policy_buf)?;
                    Some(String::from_utf8_lossy(&storage_policy_buf).to_string())
                },
            };

            let mut blocks = Vec::new();
            for _ in 0..reader.read_u32::<BigEndian>()? {
                blocks.push(reader.read_u64::<BigEndian>()?);
            }
            let block_replication = reader.read_u32::<BigEndian>()?;
            let block_size = reader.read_u64::<BigEndian>()?;

            let file = File {
                inode: inode,
                name: name.to_string(),
                file_type: file_type,
                permissions: permissions,
                owner: owner.to_string(),
                group: group.to_string(),
                storage_policy: storage_policy,
                blocks: blocks,
                block_replication: block_replication,
                block_size: block_size,
            };

            inodes.insert(inode, file);
        }

        // read children
        let mut children = HashMap::new();
        for _ in 0..reader.read_u32::<BigEndian>()? {
            let key = reader.read_u64::<BigEndian>()?;
            let mut vec = Vec::new();
            for _ in 0..reader.read_u32::<BigEndian>()? {
                let value = reader.read_u64::<BigEndian>()?;
                vec.push(value);
            }

            children.insert(key, vec);
        }

        // read parents
        let mut parents = HashMap::new();
        for _ in 0..reader.read_u32::<BigEndian>()? {
            let key = reader.read_u64::<BigEndian>()?;
            let value = reader.read_u64::<BigEndian>()?;
            parents.insert(key, value);
        }

        Ok(
            FileStore {
                inodes: inodes,
                children: children,
                parents: parents,
            }
        )
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
            // check current files storage policy
            if let Some(policy) = 
                    &self.inodes.get(current_inode).unwrap().storage_policy {
                return Some(&policy);
            }

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

    pub fn write_to(&self, mut writer: impl Write)
            -> Result<(), NahError> {
        // write inodes      
        writer.write_u32::<BigEndian>(self.inodes.len() as u32)?;
        for (_, file) in self.inodes.iter() {
            // write file
            writer.write_u64::<BigEndian>(file.inode)?;
            writer.write_u8(file.name.len() as u8)?;
            writer.write_all(file.name.as_bytes())?;

            writer.write_i32::<BigEndian>(file.file_type)?;
            writer.write_u32::<BigEndian>(file.permissions)?;
            writer.write_u8(file.owner.len() as u8)?;
            writer.write_all(file.owner.as_bytes())?;
            writer.write_u8(file.group.len() as u8)?;
            writer.write_all(file.group.as_bytes())?;
            match &file.storage_policy {
                Some(storage_policy) => {
                    writer.write_u8(storage_policy.len() as u8)?;
                    writer.write_all(storage_policy.as_bytes())?;
                },
                None => writer.write_u8(0)?,
            }

            writer.write_u32::<BigEndian>(file.blocks.len() as u32)?;
            for block in file.blocks.iter() {
                writer.write_u64::<BigEndian>(*block)?;
            }
            writer.write_u32::<BigEndian>(file.block_replication)?;
            writer.write_u64::<BigEndian>(file.block_size)?;
        }

        // write children
        writer.write_u32::<BigEndian>(self.children.len() as u32)?;
        for (key, vec) in self.children.iter() {
            writer.write_u64::<BigEndian>(*key)?;
            writer.write_u32::<BigEndian>(vec.len() as u32)?;
            for value in vec.iter() {
                writer.write_u64::<BigEndian>(*value)?;
            }
        }

        // write parents
        writer.write_u32::<BigEndian>(self.parents.len() as u32)?;
        for (key, value) in self.parents.iter() {
            writer.write_u64::<BigEndian>(*key)?;
            writer.write_u64::<BigEndian>(*value)?;
        }

        Ok(())
    }
}

fn parse_path(path: &str) -> Vec<&str> {
    let mut components: Vec<&str> = path.split("/").collect();
    while components.len() > 0 && components[0] == "" {
        components.remove(0);
    }

    components
}
