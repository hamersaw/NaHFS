use hdfs_comm::rpc::Protocol;
use hdfs_protos::hadoop::hdfs::{AddBlockResponseProto, AddBlockRequestProto, CompleteResponseProto, CompleteRequestProto, CreateResponseProto, CreateRequestProto, DirectoryListingProto, GetBlockLocationsResponseProto, GetBlockLocationsRequestProto, GetFileInfoResponseProto, GetFileInfoRequestProto, GetListingResponseProto, GetListingRequestProto, GetServerDefaultsResponseProto, GetServerDefaultsRequestProto, MkdirsResponseProto, MkdirsRequestProto, RenameResponseProto, RenameRequestProto, RenewLeaseResponseProto, RenewLeaseRequestProto, SetStoragePolicyResponseProto, SetStoragePolicyRequestProto};
use prost::Message;
use query::BooleanExpression;
use radix::RadixQuery;
use shared::AtlasError;

use crate::block::BlockStore;
use crate::datanode::DatanodeStore;
use crate::file::{FileStore, FileType};
use crate::index::Index;
use crate::storage::StorageStore;

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

static FIRST_BIT: u64 = 9223372036854775808;
static INDEXED_MASK: u64 = 9223371968135299072;
static NON_INDEXED_MASK: u64 = 9223372036854775807;

pub struct ClientNamenodeProtocol {
    block_store: Arc<RwLock<BlockStore>>,
    datanode_store: Arc<RwLock<DatanodeStore>>,
    file_store: Arc<RwLock<FileStore>>,
    index: Arc<RwLock<Index>>,
    storage_store: Arc<RwLock<StorageStore>>,
}

impl ClientNamenodeProtocol {
    pub fn new(block_store: Arc<RwLock<BlockStore>>, 
            datanode_store: Arc<RwLock<DatanodeStore>>,
            file_store: Arc<RwLock<FileStore>>,
            index: Arc<RwLock<Index>>,
            storage_store: Arc<RwLock<StorageStore>>)
            -> ClientNamenodeProtocol {
        ClientNamenodeProtocol {
            block_store: block_store,
            datanode_store: datanode_store,
            file_store: file_store,
            index: index,
            storage_store: storage_store,
        }
    }

    fn add_block(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = AddBlockRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = AddBlockResponseProto::default();

        // add block
        debug!("addBlock({:?})", request);
        let mut block_id = rand::random::<u64>();
        let mut file_store = self.file_store.write().unwrap();
        if let Some(file) = file_store.get_file(&request.src) {
            if let FileType::Regular {blocks: _, replication, block_size: _} =
                    file.get_file_type() {
                let lb_proto = &mut response.block;

                // compute block id 
                let mask = if let Some(storage_policy_id) = file_store
                        .get_storage_policy_id(&file.get_inode()) {
                    block_id = (block_id & INDEXED_MASK) | FIRST_BIT;
                    storage_policy_id
                } else {
                    block_id = block_id & NON_INDEXED_MASK;
                    0
                };

                // populate random DatanodeInfoProto locations
                let datanode_store = self.datanode_store.read().unwrap();
                let ids = datanode_store.get_random_ids(*replication);

                for id in ids {
                    let datanode = datanode_store.get_datanode(id).unwrap();
                    lb_proto.locs.push(super
                        ::to_datanode_info_proto(datanode, None));
                }

                // populate ExtendedBlockProto
                let mut ex_proto = &mut lb_proto.b;
                ex_proto.block_id = block_id | mask as u64;
                ex_proto.generation_stamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH).unwrap().as_secs() * 1000;
            }
        }

        // add blockid to file
        if let Some(file) = file_store.get_file_mut(&request.src) {
            if let FileType::Regular {blocks, replication: _, block_size: _} =
                    file.get_file_type_mut() {
                blocks.push(block_id);
            }
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn complete(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = CompleteRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = CompleteResponseProto::default();

        // complete file
        debug!("complete({:?})", request);
        let file_store = self.file_store.write().unwrap();
        if let Some(_) = file_store.get_file(&request.src) {
            // TODO complete
        }

        response.result = true;
        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn create(&self, user: &str, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = CreateRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = CreateResponseProto::default();

        // create file
        debug!("create({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.create(&request.src, request.masked.perm,
            user, user, request.replication, request.block_size);

        // get file
        if let Some(file) = file_store.get_file(&request.src) {
            let block_store = self.block_store.read().unwrap();
            let index = self.index.read().unwrap();
            response.fs = Some(crate::protocol
                ::to_hdfs_file_status_proto(file, &None,
                    &block_store, &file_store, &index));
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn get_block_locations(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = GetBlockLocationsRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = GetBlockLocationsResponseProto::default();

        // get block locations
        debug!("getBlockLocations({:?})", request);
        let (path, query) = parse_embedded_query_path(&request.src)?;

        let file_store = self.file_store.read().unwrap();
        if let Some(file) = file_store.get_file(path) {
            let block_store = self.block_store.read().unwrap();
            let datanode_store = self.datanode_store.read().unwrap();
            let index = self.index.read().unwrap();
            let storage_store = self.storage_store.read().unwrap();

            response.locations = Some(crate::protocol
                ::to_located_blocks_proto(file, &query, &block_store,
                    &datanode_store, &index, &storage_store));
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn get_file_info(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = GetFileInfoRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = GetFileInfoResponseProto::default();

        // get file
        debug!("getFileInfo({:?})", request);
        let (path, query) = parse_embedded_query_path(&request.src)?;

        let file_store = self.file_store.read().unwrap();
        if let Some(file) = file_store.get_file(path) {
            let block_store = self.block_store.read().unwrap();
            let index = self.index.read().unwrap();
            response.fs = Some(crate::protocol
                ::to_hdfs_file_status_proto(file, &query,
                    &block_store, &file_store, &index));
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn get_listing(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = GetListingRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = GetListingResponseProto::default();

        // get listing
        // TODO - handle need_location
        debug!("getListing({:?})", request);
        let (path, query) =
            parse_embedded_query_path(&request.src).unwrap();

        let file_store = self.file_store.read().unwrap();
        let mut remaining_entries = 0;
        if let Some(file) = file_store.get_file(path) {
            let block_store = self.block_store.read().unwrap();
            let index = self.index.read().unwrap();

            let mut partial_listing = Vec::new();
            match file.get_file_type_code() {
                1 => {
                    // parse start_after from request
                    let start_after =
                        String::from_utf8_lossy(&request.start_after);
                    let mut process = 
                            match request.start_after.len() {
                        0 => true,
                        _ => false,
                    };

                    let mut byte_count = 0;
                    let children = file_store
                        .get_children(file.get_inode()).unwrap();
                    for (i, child_file) in children.iter().enumerate() {
                        // check if we process this file
                        if !process {
                            if start_after == file_store.compute_path(
                                    child_file.get_inode()) {
                                process = true;
                            }

                            continue;
                        }

                        // process this file
                        let mut hfs_proto = crate::protocol
                            ::to_hdfs_file_status_proto(child_file,
                                &query, &block_store, &file_store, &index);

                        // add locations if necessary
                        if request.need_location {
                            let datanode_store =
                                self.datanode_store.read().unwrap();
                            let storage_store =
                                self.storage_store.read().unwrap();

                            hfs_proto.locations = Some(crate::protocol
                                ::to_located_blocks_proto(child_file,
                                    &query, &block_store, &datanode_store,
                                    &index, &storage_store));
                        }

                        byte_count += hfs_proto.encoded_len();
                        partial_listing.push(hfs_proto);

                        // check if message is too large
                        if byte_count >= 65536 {
                            remaining_entries = children.len() - 1 - i;
                            break;
                        }
                    }
                },
                2 => {
                    let mut hfs_proto = crate::protocol
                        ::to_hdfs_file_status_proto(file, &query,
                            &block_store, &file_store, &index);

                    // add locations if necessary
                    if request.need_location {
                        let datanode_store =
                            self.datanode_store.read().unwrap();
                        let storage_store =
                            self.storage_store.read().unwrap();

                        hfs_proto.locations = Some(crate::protocol
                            ::to_located_blocks_proto(file, &query,
                                &block_store, &datanode_store,
                                &index, &storage_store));
                    }

                    partial_listing.push(hfs_proto);
                }
                _ => unreachable!(),
            }

            // create DirectoryListingProto
            let mut directory_listing = DirectoryListingProto::default();
            directory_listing.partial_listing = partial_listing;
            directory_listing.remaining_entries =
                remaining_entries as u32;

            response.dir_list = Some(directory_listing);
        }

        println!("GET_LISTING: {:?}", response);
        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn get_server_defaults(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = GetServerDefaultsRequestProto
            ::decode_length_delimited(req_buf)?;
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

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn mkdirs(&self, user: &str, req_buf: &[u8],
              resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = MkdirsRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = MkdirsResponseProto::default();

        // create directories
        debug!("mkdirs({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.mkdirs(&request.src, request.masked.perm,
            user, user, request.create_parent);

        response.result = true;
        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn rename(&self, req_buf: &[u8],
              resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = RenameRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = RenameResponseProto::default();

        // create directories
        debug!("rename({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.rename(&request.src, &request.dst);

        response.result = true;
        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn renew_lease(&self, req_buf: &[u8],
              resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = RenewLeaseRequestProto
            ::decode_length_delimited(req_buf)?;
        let response = RenewLeaseResponseProto::default();

        debug!("renewLease({:?})", request);
        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn set_storage_policy(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = SetStoragePolicyRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let response = SetStoragePolicyResponseProto::default();

        // create directories
        debug!("setStoragePolicy({:?})", request);
        let mut file_store = self.file_store.write().unwrap();
        file_store.set_storage_policy(&request.src, &request.policy_name);

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }
}

impl Protocol for ClientNamenodeProtocol {
    fn process(&self, user: &Option<String>, method: &str,
            req_buf: &[u8], resp_buf: &mut Vec<u8>) -> std::io::Result<()> {
        let user = match user {
            Some(user) => user,
            None => "default",
        };

        match method {
            "addBlock" => self.add_block(req_buf, resp_buf)?,
            "complete" => self.complete(req_buf, resp_buf)?,
            "create" => self.create(user, req_buf, resp_buf)?,
            "getBlockLocations" => self.get_block_locations(req_buf, resp_buf)?,
            "getFileInfo" => self.get_file_info(req_buf, resp_buf)?,
            "getListing" => self.get_listing(req_buf, resp_buf)?,
            "getServerDefaults" => self.get_server_defaults(req_buf, resp_buf)?,
            "mkdirs" => self.mkdirs(user, req_buf, resp_buf)?,
            "rename" => self.rename(req_buf, resp_buf)?,
            "renewLease" => self.renew_lease(req_buf, resp_buf)?,
            "setStoragePolicy" => self.set_storage_policy(req_buf, resp_buf)?,
            _ => error!("unimplemented method '{}'", method),
        }

        Ok(())
    }
}

fn parse_embedded_query_path(path: &str) -> Result<(&str,
        Option<(&str, (BooleanExpression<u64>, RadixQuery))>), AtlasError> {
    let fields: Vec<&str> = path.split("+").collect();
    let query = match fields.len() {
        1 => None,
        2 => Some((fields[1], crate::index::parse_query(fields[1])?)),
        _ => return Err(AtlasError::from("invalid embedded query path")),
    };

    Ok((fields[0], query))
}
