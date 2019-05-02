use hdfs_comm::rpc::Protocol;
//use hdfs_protos::hadoop::hdfs::{DirectoryListingProto, GetFileInfoResponseProto, GetFileInfoRequestProto, GetListingRequestProto, GetListingResponseProto, HdfsFileStatusProto, MkdirsRequestProto, MkdirsResponseProto};
use prost::Message;

use std::sync::{Arc, RwLock};

pub struct DatanodeProtocol {
}

impl DatanodeProtocol {
    pub fn new() -> DatanodeProtocol {
        DatanodeProtocol {
        }
    }
}

impl Protocol for DatanodeProtocol {
    fn process(&self, method: &str, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) {
        match method {
            "registerDatanode" => self.register_datanode(req_buf, resp_buf),
            _ => error!("unimplemented method '{}'", method),
        }
    }
}
