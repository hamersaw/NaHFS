use hdfs_comm::rpc::Protocol;
use hdfs_protos::hadoop::hdfs::{AddBlockResponseProto, AddBlockRequestProto, CompleteResponseProto, CompleteRequestProto, CreateResponseProto, CreateRequestProto, DirectoryListingProto, GetFileInfoResponseProto, GetFileInfoRequestProto, GetListingResponseProto, GetListingRequestProto, GetServerDefaultsResponseProto, GetServerDefaultsRequestProto, MkdirsResponseProto, MkdirsRequestProto, RenameResponseProto, RenameRequestProto, SetStoragePolicyResponseProto, SetStoragePolicyRequestProto};
use prost::Message;

use crate::datanode::{Datanode, DatanodeStore};
use crate::file::{File, FileStore};

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

static FIRST_BIT: u64 = 9223372036854775808;
static INDEXED_MASK: u64 = 9223372032559808512;
static NON_INDEXED_MASK: u64 = 9223372036854775807;

pub struct ClientNamenodeProtocol {
    datanode_store: Arc<RwLock<DatanodeStore>>,
    file_store: Arc<RwLock<FileStore>>,
}

impl ClientNamenodeProtocol {
    pub fn new(datanode_store: Arc<RwLock<DatanodeStore>>,
            file_store: Arc<RwLock<FileStore>>) -> ClientNamenodeProtocol {
        ClientNamenodeProtocol {
            datanode_store: datanode_store,
            file_store: file_store,
        }
    }

    fn add_block(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = AddBlockRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = AddBlockResponseProto::default();

        // add block
        debug!("addBlock({:?})", request);
        let mut block_id = rand::random::<u64>();
        let mut file_store = self.file_store.write().unwrap();
        if let Some(file) = file_store.get_file(&request.src) {
            let mut lb_proto = &mut response.block;

            // compute block id
            if let Some("INDEXED") =
                    file_store.get_storage_policy(&file.inode) {
                block_id = (block_id & INDEXED_MASK) | FIRST_BIT;
            } else {
                block_id = block_id & NON_INDEXED_MASK;
            }

            // populate random DatanodeInfoProto locations
            let datanode_store = self.datanode_store.read().unwrap();
            let ids = datanode_store.get_random_ids(file.block_replication);

            for id in ids {
                let datanode = datanode_store.get_datanode(id).unwrap();
                lb_proto.locs.push(super::to_datanode_info_proto(datanode));
            }

            // populate ExtendedBlockProto
            let mut ex_proto = &mut lb_proto.b;
            ex_proto.block_id = block_id;
            ex_proto.generation_stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH).unwrap().as_secs() * 1000;
        }

        // add blockid to file
        if let Some(mut file) = file_store.get_file_mut(&request.src) {
            file.blocks.push(block_id);
        }

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn complete(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = CompleteRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = CompleteResponseProto::default();

        // complete file
        debug!("complete({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        if let Some(file) = file_store.get_file(&request.src) {
            // TODO complete
        }

        response.result = true;
        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn create(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = CreateRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = CreateResponseProto::default();

        // create file
        debug!("create({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.create(&request.src, request.masked.perm,
            "TODO", "TODO", request.replication, request.block_size);

        // get file
        if let Some(file) = file_store.get_file(&request.src) {
            response.fs = Some(crate::protocol
                ::to_hdfs_file_status_proto(file, &file_store));
        }

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn get_file_info(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = GetFileInfoRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = GetFileInfoResponseProto::default();

        // get file
        debug!("getFileInfo({:?})", request);
        let file_store = self.file_store.read().unwrap();
        if let Some(file) = file_store.get_file(&request.src) {
            response.fs = Some(crate::protocol
                ::to_hdfs_file_status_proto(file, &file_store));
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
                        partial_listing.push(crate::protocol
                            ::to_hdfs_file_status_proto(child_file,
                                &file_store));
                    }
                },
                2 => partial_listing.push(crate::protocol
                    ::to_hdfs_file_status_proto(file, &file_store)),
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

    fn get_server_defaults(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = GetServerDefaultsRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = GetServerDefaultsResponseProto::default();
        let mut fsd_proto = &mut response.server_defaults;

        // populate server defaults
        debug!("getServerDefaults({:?})", request);
        fsd_proto.block_size = 65536;
        fsd_proto.bytes_per_checksum = 512;
        fsd_proto.write_packet_size = 5000;
        fsd_proto.replication = 3;
        fsd_proto.file_buffer_size = 5000;
        // TODO - parameterize server defaults values

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn mkdirs(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = MkdirsRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = MkdirsResponseProto::default();

        // create directories
        debug!("mkdirs({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.mkdirs(&request.src, request.masked.perm,
            "TODO", "TODO", request.create_parent);

        response.result = true;
        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn rename(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = RenameRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = RenameResponseProto::default();

        // create directories
        debug!("rename({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.rename(&request.src, &request.dst);

        response.result = true;
        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn set_storage_policy(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = SetStoragePolicyRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = SetStoragePolicyResponseProto::default();

        // create directories
        debug!("setStoragePolicy({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.set_storage_policy(&request.src, &request.policy_name);

        response.encode_length_delimited(resp_buf).unwrap();
    }
}

impl Protocol for ClientNamenodeProtocol {
    fn process(&self, method: &str, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) {
        match method {
            "addBlock" => self.add_block(req_buf, resp_buf),
            "complete" => self.complete(req_buf, resp_buf),
            "create" => self.create(req_buf, resp_buf),
            "getFileInfo" => self.get_file_info(req_buf, resp_buf),
            "getListing" => self.get_listing(req_buf, resp_buf),
            "getServerDefaults" => self.get_server_defaults(req_buf, resp_buf),
            "mkdirs" => self.mkdirs(req_buf, resp_buf),
            "rename" => self.rename(req_buf, resp_buf),
            "setStoragePolicy" => self.set_storage_policy(req_buf, resp_buf),
            _ => error!("unimplemented method '{}'", method),
        }
    }
}
