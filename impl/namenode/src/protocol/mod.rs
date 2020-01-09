use hdfs_protos::hadoop::hdfs::{DatanodeIdProto, DatanodeInfoProto, HdfsFileStatusProto, LocatedBlockProto, LocatedBlocksProto};

use crate::block::BlockStore;
use crate::datanode::{Datanode, DatanodeStore};
use crate::file::{File, FileStore, FileType};
use crate::index::{Index, SpatialQuery, TemporalQuery};
use crate::storage::StorageStore;

mod client_namenode;
mod datanode;
mod nahfs;

pub use client_namenode::ClientNamenodeProtocol;
pub use datanode::DatanodeProtocol;
pub use nahfs::NahFSProtocol;

use std::collections::HashMap;
use std::cmp::Ordering;

fn get_datanode_usage(datanode_store: &DatanodeStore,
        storage_store: &StorageStore) -> Vec<(String, u64)> {
    let mut datanodes = Vec::new();

    // compute storage usage for each registered datanode
    for datanode in datanode_store.get_datanodes() {
        let mut byte_count = 0;
        for storage_id in datanode.storage_ids.iter() {
            if let Some(storage) =
                    storage_store.get_storage(storage_id) {
                if let Some(state) = storage.states.last() {
                    byte_count += state.dfs_used.unwrap_or(0);
                }
            }
        }

        datanodes.push((datanode.id.to_string(), byte_count));
    }

    // sort datanodes by storage usage
    datanodes.sort_by(|a, b| a.1.partial_cmp(&b.1)
        .unwrap_or(Ordering::Equal));
    datanodes
}

fn get_spatiotemporal_datanode_usage(block_store: &BlockStore,
        datanode_store: &DatanodeStore, index: &Index,
        geohashes: &Vec<String>) -> Vec<(String, u64)> {
    // TODO - extend to process temporal attributes as well

    // initialize datanodes
    let mut map = HashMap::new();
    for datanode in datanode_store.get_datanodes() {
        map.insert(datanode.id.to_string(), 0);  
    }

    // iterate over spatial attributes
    for geohash in geohashes.iter() {
        for (block_id, size) in index.spatial_blocks_query(geohash) {
            // update datanodes for each block replica
            let block = block_store.get_block(&block_id).unwrap();
            for datanode_id in block.locations.iter() {
                let byte_count = map.get_mut(datanode_id).unwrap();
                *byte_count += size as u64;
            }
        }
    }
    
    let mut datanodes: Vec<(String, u64)> = map.drain().collect();
    datanodes.sort_by(|a, b| a.1.partial_cmp(&b.1)
        .unwrap_or(Ordering::Equal));
    datanodes
}

fn select_block_replica(vec: &Vec<(String, u64)>) -> usize {
    // use a logarithmic function to favor nodes with lower utilization
    let log_base = (vec.len() + 1) as f64;
    let replica_token = rand::random::<f64>();
    for i in 0..vec.len() {
        if replica_token <= ((i + 2) as f64).log(log_base) {
            return i;
        }
    }

    return vec.len() - 1;
}

fn to_datanode_id_proto(datanode: &Datanode) -> DatanodeIdProto {
    // initialize DatanodeIdProto
    let mut di_proto = DatanodeIdProto::default();
    di_proto.ip_addr = datanode.ip_address.clone();
    di_proto.datanode_uuid = datanode.id.clone();
    di_proto.xfer_port = datanode.xfer_port;

    di_proto
}

fn to_datanode_info_proto(datanode: &Datanode,
        storage_store: Option<&StorageStore>) -> DatanodeInfoProto {
    let mut last_update = 0;

    // initialize DatanodeInfoProto
    let mut din_proto = DatanodeInfoProto::default();
    din_proto.admin_state = Some(0); // NORMAL
    din_proto.location = Some("/default-rack".to_string());

    // populate DatanodeIdProto
    let mut di_proto = &mut din_proto.id;
    di_proto.ip_addr = datanode.ip_address.clone();
    di_proto.datanode_uuid = datanode.id.clone();
    di_proto.xfer_port = datanode.xfer_port;

    // populate storage state variables
    let (mut capacity, mut dfs_used, mut remaining,
         mut block_pool_used, mut non_dfs_used) = (0, 0, 0, 0, 0);
    if let Some(storage_store) = storage_store {
        for storage_id in &datanode.storage_ids {
            if let Some(storage) =
                    storage_store.get_storage(storage_id) {
                if let Some(state) = storage.states.last() {
                    capacity += state.capacity.unwrap_or(0);
                    dfs_used += state.dfs_used.unwrap_or(0);
                    remaining += state.remaining.unwrap_or(0);
                    block_pool_used += state.block_pool_used.unwrap_or(0);
                    non_dfs_used += state.non_dfs_used.unwrap_or(0);
                    last_update = std::cmp::max(last_update,
                        state.update_timestamp);
                }
            }
        }
    }

    din_proto.capacity = Some(capacity);
    din_proto.dfs_used = Some(dfs_used);
    din_proto.remaining = Some(remaining);
    din_proto.block_pool_used = Some(block_pool_used);
    din_proto.non_dfs_used = Some(non_dfs_used);

    // populate datanode state variables
    if let Some(state) = datanode.states.last() {
        din_proto.cache_capacity = state.cache_capacity;
        din_proto.cache_used = state.cache_used;
        din_proto.xceiver_count = state.xceiver_count;
        last_update = std::cmp::max(last_update,
            state.update_timestamp);
    }

    // last updated = max of most recent datanode and storage states
    if last_update != 0 {
        din_proto.last_update = Some(last_update);
    }
 
    din_proto
}

fn to_hdfs_file_status_proto(file: &File,
        query: &Option<(&str, (Option<SpatialQuery>, Option<TemporalQuery>))>, 
        block_store: &BlockStore, file_store: &FileStore,
        index: &Index) -> HdfsFileStatusProto {
    let mut hfs_proto = HdfsFileStatusProto::default();
    hfs_proto.file_type = file.get_file_type_code();
    hfs_proto.path =
        file_store.compute_path(file.get_inode()).into_bytes();
    if let Some((query_string, _)) = query {
        hfs_proto.path.push('+' as u8);

        for value in query_string.as_bytes() {
            hfs_proto.path.push(*value);
        }
    }

    // iterate over blocks to compute file length
    hfs_proto.length = 0;
    if let FileType::Regular{blocks, replication: _, block_size: _} =
            file.get_file_type() {
        for (block_id, query_result) in query_blocks(blocks, index, query) {
            if let Some(block) = block_store.get_block(&block_id) {
                match query_result {
                    Some((_, length)) => hfs_proto.length += length as u64,
                    None => hfs_proto.length += block.length,
                }
            }
        }
    }

    let fp_proto = &mut hfs_proto.permission;
    fp_proto.perm = file.get_permissions();

    hfs_proto.owner = file.get_owner().to_string();
    hfs_proto.group = file.get_group().to_string();

    match file.get_file_type() {
        FileType::Directory =>  {
            if let Some(children) = file_store.get_children(file.get_inode()) {
                hfs_proto.children_num = Some(children.len() as i32);
            }
        },
        FileType::Regular{blocks: _, replication, block_size} => {
            hfs_proto.block_replication = Some(*replication);
            hfs_proto.blocksize = Some(*block_size);
        },
    }

    hfs_proto.file_id = Some(file.get_inode());

    hfs_proto.storage_policy =
        file_store.get_storage_policy_id(&file.get_inode());

    hfs_proto
}

fn to_located_blocks_proto(file: &File,
        query: &Option<(&str, (Option<SpatialQuery>, Option<TemporalQuery>))>, 
        block_store: &BlockStore, datanode_store: &DatanodeStore,
        index: &Index, storage_store: &StorageStore) -> LocatedBlocksProto {
    let mut lbs_proto = LocatedBlocksProto::default();
    let lb_proto_blocks = &mut lbs_proto.blocks;

    let (mut length, mut complete) = (0, true);
    if let FileType::Regular{blocks, replication: _, block_size: _} =
            file.get_file_type() {
        for (block_id, query_result) in query_blocks(blocks, index, query) {
            if let Some(block) = block_store.get_block(&block_id) {
                // populate LocatedBlockProto
                let mut lb_proto = LocatedBlockProto::default();
                let eb_proto = &mut lb_proto.b;

                // populate ExtendedBlockProto
                match query_result {
                    Some((query_block_id, length)) => {
                        eb_proto.block_id = query_block_id;
                        eb_proto.num_bytes = Some(length as u64);
                    },
                    None => {
                        eb_proto.block_id = block.id;
                        eb_proto.num_bytes = Some(block.length);
                    },
                }

                // populate LocatedBlockProto
                lb_proto.offset = length;
                lb_proto.corrupt = false;

                // populate locs
                for datanode_id in &block.locations {
                    if let Some(datanode) =
                            datanode_store.get_datanode(datanode_id) {
                        lb_proto.locs.push(to_datanode_info_proto(
                            datanode, Some(storage_store)));
                    }
                }

                // populate storages
                for storage_id in &block.storage_ids {
                    lb_proto.storage_types.push(0);
                    lb_proto.storage_i_ds.push(storage_id.to_string());
                    lb_proto.is_cached.push(false);
                }

                // increment file length
                length += eb_proto.num_bytes.unwrap();
                lb_proto_blocks.push(lb_proto);
            } else {
                // block_id not found -> file not complete
                complete = false;
            }
        }
    }

    lbs_proto.file_length = length;
    lbs_proto.under_construction = !complete;
    lbs_proto.is_last_block_complete = complete;
    lbs_proto
}

fn query_blocks(block_ids: &Vec<u64>, index: &Index, 
        query: &Option<(&str, (Option<SpatialQuery>, Option<TemporalQuery>))>)
        -> Vec<(u64, Option<(u64, u32)>)> {
    let mut blocks = Vec::new();

    match query {
        Some((_, (spatial_query, temporal_query))) => {
            'block: for block_id in block_ids {
                // evaluate temporal query
                if let Some(query) = temporal_query {
                    if !index.temporal_query(block_id, query) {
                        // temporal query fails -> skip block
                        continue 'block;
                    }
                }

                // evaluate spatial query
                if let Some(query) = spatial_query {
                    if let Some(geohashes) =
                            index.spatial_query(block_id, query) {
                        if geohashes.len() == 0 {
                            // no spatial index found -> use whole block
                            blocks.push((*block_id, None));
                        } else {
                            // compute block_id
                            let geohash_vec = geohashes.keys()
                                .map(|x| *x).collect();
                            let query_block_id = shared::block
                                ::encode_block_id(&block_id, &geohash_vec);

                            // compute block length
                            let mut query_block_length = 0;
                            for (_, length) in geohashes.iter() {
                                query_block_length += length;
                            }

                            blocks.push((*block_id,
                                Some((query_block_id, query_block_length))));
                        }
                    }
                } else {
                    // no spatial query -> use whole block
                    blocks.push((*block_id, None));
                }
            }
        },
        None => {
            // if no query -> return blocks that exist in BlockStore
            for block_id in block_ids {
                blocks.push((*block_id, None));
            }
        },
    }

    blocks
}
