mod namenode;
mod transfer;

pub use namenode::NamenodeProtocol;
pub use transfer::TransferStreamHandler;

use hdfs_comm::protos::hdfs::{DatanodeIdProto, DatanodeStorageProto, ExportedBlockKeysProto, StorageInfoProto};
use hdfs_comm::protos::hdfs::datanode::DatanodeRegistrationProto;

use crate::Config;

pub fn to_datanode_storage_proto(config: &Config)
        -> DatanodeStorageProto {
    let mut ds_proto = DatanodeStorageProto::default();
    ds_proto.storage_uuid = config.storage_id.to_owned();

    ds_proto
}

pub fn to_datanode_registration_proto(config: &Config)
        -> DatanodeRegistrationProto {
    let mut di_proto = DatanodeIdProto::default();
    di_proto.ip_addr = config.ip_address.to_owned();
    di_proto.datanode_uuid = config.id.to_owned();
    di_proto.xfer_port = config.port;

    let si_proto = StorageInfoProto::default();
    // TODO - complete

    let ebk_proto = ExportedBlockKeysProto::default();
    // TODO - complete

    let mut dr_proto = DatanodeRegistrationProto::default();
    dr_proto.datanode_id = di_proto;
    dr_proto.storage_info = si_proto;
    dr_proto.keys = ebk_proto;
    dr_proto.software_version = "2.8.2".to_string();
    dr_proto
}
