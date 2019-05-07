use hdfs_comm::rpc::Protocol;
use hdfs_protos::hadoop::hdfs::datanode::{BlockReportResponseProto, BlockReportRequestProto, HeartbeatResponseProto, HeartbeatRequestProto, RegisterDatanodeResponseProto, RegisterDatanodeRequestProto};
use prost::Message;

use crate::datanode::{Datanode, DatanodeStore};

use std::sync::{Arc, RwLock};

pub struct DatanodeProtocol {
    datanode_store: Arc<RwLock<DatanodeStore>>,
}

impl DatanodeProtocol {
    pub fn new(datanode_store: Arc<RwLock<DatanodeStore>>)
            -> DatanodeProtocol {
        DatanodeProtocol {
            datanode_store: datanode_store,
        }
    }

    fn block_report(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = BlockReportRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = BlockReportResponseProto::default();

        // process block report
        debug!("blockReport({:?})", request);
        // TODO - process block report

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn heartbeat(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = HeartbeatRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = HeartbeatResponseProto::default();

        // process heartbeat
        debug!("heartbeat({:?})", request);
        // TODO - process heartbeat

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn register_datanode(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = RegisterDatanodeRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = RegisterDatanodeResponseProto::default();

        // register datanode
        debug!("registerDatanode({:?})", request);
        // TODO - register datanode

        response.encode_length_delimited(resp_buf).unwrap();
    }
}

impl Protocol for DatanodeProtocol {
    fn process(&self, method: &str, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) {
        match method {
            "blockReport" => self.block_report(req_buf, resp_buf),
            "heartbeat" => self.heartbeat(req_buf, resp_buf),
            "registerDatanode" => self.register_datanode(req_buf, resp_buf),
            _ => error!("unimplemented method '{}'", method),
        }
    }
}
