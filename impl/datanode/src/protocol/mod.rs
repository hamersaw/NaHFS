mod namenode;
pub use namenode::NamenodeProtocol;

use hdfs_protos::hadoop::hdfs::{BlockKeyProto, DatanodeIdProto, ExportedBlockKeysProto, StorageInfoProto};
use hdfs_protos::hadoop::hdfs::datanode::{DatanodeRegistrationProto};

fn to_datanode_registration_proto() -> DatanodeRegistrationProto {
    let dr_proto = DatanodeRegistrationProto::default();
    dr_proto
}
