use hdfs_comm::rpc::Protocol;
use prost::Message;
use shared::AtlasError;
use shared::protos::{GetStoragePolicyResponseProto, GetStoragePolicyRequestProto, IndexReportResponseProto, IndexReportRequestProto, InodePersistResponseProto, InodePersistRequestProto};

use crate::file::FileStore;
use crate::index::Index;

use std::fs::File;
use std::io::{Write};
use std::sync::{Arc, RwLock};

pub struct AtlasProtocol {
    file_store: Arc<RwLock<FileStore>>,
    index: Arc<RwLock<Index>>,
    persist_path: String,
}

impl AtlasProtocol {
    pub fn new(file_store: Arc<RwLock<FileStore>>,
            index: Arc<RwLock<Index>>, persist_path: &str) -> AtlasProtocol {
        AtlasProtocol {
            file_store: file_store,
            index: index,
            persist_path: persist_path.to_string(),
        }
    }

    fn get_storage_policy(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = GetStoragePolicyRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = GetStoragePolicyResponseProto::default();

        // retrieve storage policy
        debug!("getStoragePolicy({:?})", request);
        let file_store: &FileStore = &self.file_store.read().unwrap();
        match file_store.get_storage_policy_str(&request.id) {
            Some(storage_policy) =>
                response.storage_policy = storage_policy.to_owned(),
            None => return Err(AtlasError::from(
                format!("storage policy {} not found", request.id))),
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn index_report(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = IndexReportRequestProto
            ::decode_length_delimited(req_buf)?;
        let response = IndexReportResponseProto::default();

        // process index report
        trace!("indexReport({:?})", request);
        let mut index = self.index.write().unwrap();
        for i in 0..request.block_ids.len() {
            let block_id = &request.block_ids[i];
            let bi_proto = &request.block_indices[i];

            // add geohashes
            if let Some(si_proto) = &bi_proto.spatial_index {
                for j in 0..si_proto.geohashes.len() {
                    index.add_spatial_index(*block_id, &si_proto.geohashes[j],
                        si_proto.end_indices[j] - si_proto.start_indices[j])?;
                }
            }

            // add time range
            if let Some(ti_proto) = &bi_proto.temporal_index {
                index.add_temporal_index(*block_id,
                    ti_proto.start_timestamp, ti_proto.end_timestamp)?;
            }
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn inode_persist(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), AtlasError> {
        let request = InodePersistRequestProto
            ::decode_length_delimited(req_buf)?;
        let response = InodePersistResponseProto::default();

        // process inode persist
        debug!("inodePersist({:?})", request);
        let file_store: &FileStore = &self.file_store.read().unwrap();
        let buf: Vec<u8> = bincode::serialize(file_store)?;

        let mut file = File::create(&self.persist_path)?;
        file.write_all(&buf)?;

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }
}

impl Protocol for AtlasProtocol {
    fn process(&self, _user: &Option<String>, method: &str,
            req_buf: &[u8], resp_buf: &mut Vec<u8>) -> std::io::Result<()> {
        match method {
            "getStoragePolicy" =>
                self.get_storage_policy(req_buf, resp_buf)?,
            "indexReport" => self.index_report(req_buf, resp_buf)?,
            "inodePersist" => self.inode_persist(req_buf, resp_buf)?,
            _ => error!("unimplemented method '{}'", method),
        }

        Ok(())
    }
}
