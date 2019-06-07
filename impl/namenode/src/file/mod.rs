use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use serde::{Deserialize, Serialize};
use shared::AtlasError;

mod store;
pub use store::FileStore;

use std::io::{Read, Write};

#[derive(Deserialize, Serialize)]
pub enum FileType {
    Directory,
    Regular { blocks: Vec<u64>, replication: u32, block_size: u64 },
}

impl FileType {
    pub fn directory() -> FileType {
        FileType::Directory {
        }
    }

    pub fn regular(replication: u32, block_size: u64) -> FileType {
        FileType::Regular {
            blocks: Vec::new(),
            replication: replication,
            block_size: block_size,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct File {
    inode: u64,
    file_type: FileType,
    name: String,
    owner: String,
    group: String,
    permissions: u32,
    storage_policy: Option<String>,
}

impl File {
    pub fn new(inode: u64, file_type: FileType, name: String,
            owner: String, group: String, permissions: u32,
            storage_policy: Option<String>) -> File {
        File {
            inode: inode,
            file_type: file_type,
            name: name,
            owner: owner,
            group: group,
            permissions: permissions,
            storage_policy: storage_policy,
        }
    }

    pub fn get_file_type(&self) -> &FileType {
        &self.file_type
    }

    pub fn get_file_type_mut(&mut self) -> &mut FileType {
        &mut self.file_type
    }

    pub fn get_file_type_code(&self) -> i32 {
        match &self.file_type {
            FileType::Directory => 1,
            FileType::Regular {blocks, replication, block_size} => 2,
        }
    }

    pub fn get_group(&self) -> &str {
        &self.group
    }

    pub fn get_inode(&self) -> u64 {
        self.inode
    }

    pub fn get_owner(&self) -> &str {
        &self.owner
    }

    pub fn get_permissions(&self) -> u32 {
        self.permissions
    }

    pub fn get_storage_policy(&self) -> &Option<String> {
        &self.storage_policy
    }
}
