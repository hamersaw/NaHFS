use hdfs_comm::rpc::Protocol;
use hdfs_protos::hadoop::hdfs::{DirectoryListingProto, GetFileInfoResponseProto, GetFileInfoRequestProto, GetListingRequestProto, GetListingResponseProto, HdfsFileStatusProto, MkdirsRequestProto, MkdirsResponseProto};
use prost::Message;

use crate::file::{File, FileStore};

use std::sync::{Arc, RwLock};

pub struct ClientNamenodeProtocol {
    file_store: Arc<RwLock<FileStore>>,
}

impl ClientNamenodeProtocol {
    pub fn new(file_store: Arc<RwLock<FileStore>>) -> ClientNamenodeProtocol {
        ClientNamenodeProtocol {
            file_store: file_store,
        }
    }

    fn get_file_info(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = GetFileInfoRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = GetFileInfoResponseProto::default();

        // get file
        debug!("getFileInfo({:?})", request);
        let file_store = self.file_store.read().unwrap();
        if let Some(file) = file_store.get_file(&request.src) {
            response.fs = Some(to_proto(file, &file_store));
        }

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn get_listing(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = GetListingRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = GetListingResponseProto::default();

        // get listing
        // TODO - handle start_after, need_location
        debug!("getListing({:?})", request);
        let file_store = self.file_store.read().unwrap();
        if let Some(file) = file_store.get_file(&request.src) {
            let mut partial_listing = Vec::new();
            match file.file_type {
                1 => {
                    for child_file in file_store
                            .get_children(file.inode).unwrap() {
                        partial_listing.push(
                            to_proto(child_file, &file_store));
                    }
                },
                2 => partial_listing.push(to_proto(file, &file_store)),
                _ => unimplemented!(),
            }

            // create DirectoryListingProto
            let mut directory_listing = DirectoryListingProto::default();
            directory_listing.partial_listing = partial_listing;
            directory_listing.remaining_entries = 0;

            response.dir_list = Some(directory_listing);
        }
        
        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn mkdirs(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = MkdirsRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = MkdirsResponseProto::default();

        // create directories
        debug!("mkdirs({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.mkdirs(&request.src, request.create_parent);

        response.result = true;
        response.encode_length_delimited(resp_buf).unwrap();
    }
}

impl Protocol for ClientNamenodeProtocol {
    fn process(&self, method: &str, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) {
        match method {
            "getFileInfo" => self.get_file_info(req_buf, resp_buf),
            "getListing" => self.get_listing(req_buf, resp_buf),
            "mkdirs" => self.mkdirs(req_buf, resp_buf),
            _ => error!("unimplemented method '{}'", method),
        }
    }
}

fn to_proto(file: &File, file_store: &FileStore) -> HdfsFileStatusProto {
    let mut proto = HdfsFileStatusProto::default();
    let path = file_store.compute_path(file.inode);
    proto.path = path.into_bytes();

    proto
}
