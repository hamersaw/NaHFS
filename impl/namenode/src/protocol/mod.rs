use hdfs_protos::hadoop::hdfs::{DatanodeIdProto, DatanodeInfoProto, FsPermissionProto, HdfsFileStatusProto, LocatedBlockProto};

use crate::block::{Block, BlockStore};
use crate::datanode::{Datanode, DatanodeStore};
use crate::file::{File, FileStore};
use crate::storage::StorageStore;

mod client_namenode;
mod datanode;

pub use client_namenode::ClientNamenodeProtocol;
pub use datanode::DatanodeProtocol;

fn to_datanode_info_proto(datanode: &Datanode) -> DatanodeInfoProto {
    // iniitalize DatanodeInfoProto
    let mut din_proto = DatanodeInfoProto::default();

    // populate DatanodeIdProto
    let mut di_proto = &mut din_proto.id;
    di_proto.ip_addr = datanode.ip_address.clone();
    di_proto.datanode_uuid = datanode.id.clone();
    di_proto.xfer_port = datanode.xfer_port;

    // TODO - compute storage values

    // populate state variables
    if let Some(state) = datanode.states.last() {
        din_proto.cache_capacity = state.cache_capacity;
        din_proto.cache_used = state.cache_used;
        din_proto.last_update = Some(state.update_timestamp);
        din_proto.xceiver_count = state.xceiver_count;
    }
    
    din_proto
}

fn to_hdfs_file_status_proto(file: &File,
        file_store: &FileStore) -> HdfsFileStatusProto {
    let mut hfs_proto = HdfsFileStatusProto::default();
    hfs_proto.file_type = file.file_type;
    hfs_proto.path = file_store.compute_path(file.inode).into_bytes();
    //hfs_proto.length = 0; // TODO - compute length from blocks

    let mut fp_proto = FsPermissionProto::default();
    fp_proto.perm = file.permissions;
    hfs_proto.permission = fp_proto;

    hfs_proto.owner = file.owner.clone();
    hfs_proto.group = file.group.clone();

    match file.file_type {
        1 =>  {
            if let Some(children) = file_store.get_children(file.inode) {
                hfs_proto.children_num = Some(children.len() as i32);
            }
        },
        2 => {
            hfs_proto.block_replication = Some(file.block_replication);
            hfs_proto.blocksize = Some(file.block_size);
        },
        _ => unimplemented!(),
    }

    // TODO - add locations if necessary
    hfs_proto.file_id = Some(file.inode);

    hfs_proto
}

fn to_located_block_proto(block: &Block) -> LocatedBlockProto {
    unimplemented!();
}
