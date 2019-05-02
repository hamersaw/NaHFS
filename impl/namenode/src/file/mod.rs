mod store;
pub use store::FileStore;

pub struct File {
    pub inode: u32,
    pub name: String,
    pub file_type: i32, // 1 -> dir, 2 -> file
}

impl File {
    pub fn new(inode: u32, name: String, file_type: i32) -> File {
        File {
            inode: inode,
            name: name,
            file_type: file_type,
        }
    }
}
