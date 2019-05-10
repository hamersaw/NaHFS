use hdfs_comm::rpc::Protocol;
use hdfs_protos::hadoop::hdfs::datanode::{BlockReportResponseProto, BlockReportRequestProto, HeartbeatResponseProto, HeartbeatRequestProto, RegisterDatanodeResponseProto, RegisterDatanodeRequestProto};
use prost::Message;

use crate::block::BlockStore;
use crate::datanode::{Datanode, DatanodeStore};
use crate::storage::StorageStore;

use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct DatanodeProtocol {
    block_store: Arc<RwLock<BlockStore>>,
    datanode_store: Arc<RwLock<DatanodeStore>>,
    storage_store: Arc<RwLock<StorageStore>>,
}

impl DatanodeProtocol {
    pub fn new(block_store: Arc<RwLock<BlockStore>>,
            datanode_store: Arc<RwLock<DatanodeStore>>,
            storage_store: Arc<RwLock<StorageStore>>) -> DatanodeProtocol {
        DatanodeProtocol {
            block_store: block_store,
            datanode_store: datanode_store,
            storage_store: storage_store,
        }
    }

    fn block_report(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = BlockReportRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = BlockReportResponseProto::default();

        // process block report
        trace!("blockReport({:?})", request);
        let mut block_store = self.block_store.write().unwrap();

        let datanode_id = request.registration.datanode_id.datanode_uuid;
        for sbr_proto in request.reports {
            let storage_id = sbr_proto.storage.storage_uuid;
            let mut index = 0;
            while index < sbr_proto.blocks.len() {
                let block_id = sbr_proto.blocks[index];
                let length = sbr_proto.blocks[index+1];
                let generation_stamp = sbr_proto.blocks[index+2];

                block_store.update(block_id, generation_stamp,
                    length, &datanode_id, &storage_id);
                index += 4;
            }
        }

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn heartbeat(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = HeartbeatRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = HeartbeatResponseProto::default();

        // process heartbeat
        trace!("heartbeat({:?})", request);
        let time = SystemTime::now().duration_since(UNIX_EPOCH)
            .unwrap().as_secs() * 1000;

        // process datanode report
        let mut datanode_store = self.datanode_store.write().unwrap();
        let datanode_id = &request.registration.datanode_id.datanode_uuid;
        datanode_store.update(datanode_id, request.cache_capacity,
            request.cache_used, time, request.xmits_in_progress,
            request.xceiver_count);
 
        // process storage reports
        let mut storage_store = self.storage_store.write().unwrap();
        for sr_proto in request.reports {
            let storage_id = &sr_proto.storage_uuid;
            storage_store.update(storage_id, sr_proto.capacity,
                sr_proto.dfs_used, sr_proto.remaining, time);

            datanode_store.add_storage(datanode_id, storage_id);
        }

        response.encode_length_delimited(resp_buf).unwrap();
    }

    fn register_datanode(&self, req_buf: &[u8], resp_buf: &mut Vec<u8>) {
        let request = RegisterDatanodeRequestProto
            ::decode_length_delimited(req_buf).unwrap();
        let mut response = RegisterDatanodeResponseProto::default();

        // register datanode
        trace!("registerDatanode({:?})", request);
        let mut datanode_store = self.datanode_store.write().unwrap();
        let di_proto = request.registration.datanode_id;
        datanode_store.register(di_proto.datanode_uuid,
            di_proto.ip_addr, di_proto.xfer_port);

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
