use hdfs_comm::rpc::Protocol;
use prost::Message;
use shared::AtlasError;
use shared::protos::{IndexReportResponseProto, IndexReportRequestProto, InodePersistResponseProto, InodePersistRequestProto};

use crate::file::FileStore;
use crate::index::Index;

use std::fs::File;
use std::io::{BufWriter};
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
            let block_index = &request.block_indices[i];

            // add geohashes
            for j in 0..block_index.geohashes.len() {
                index.add_geohash(&block_index.geohashes[j], block_id,
                    block_index.end_indices[j]
                        - block_index.start_indices[j])?;
            }

            // add time range
            index.add_time_range(block_index.start_timestamp,
                block_index.end_timestamp, block_id)?;
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
        let file_store = self.file_store.read().unwrap();

        let file = File::create(&self.persist_path)?;
        let buf_writer = BufWriter::new(file);
        file_store.write_to(buf_writer)?;

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }
}

impl Protocol for AtlasProtocol {
    fn process(&self, _user: &Option<String>, method: &str,
            req_buf: &[u8], resp_buf: &mut Vec<u8>) -> std::io::Result<()> {
        match method {
            "indexReport" => self.index_report(req_buf, resp_buf)?,
            "inodePersist" => self.inode_persist(req_buf, resp_buf)?,
            _ => error!("unimplemented method '{}'", method),
        }

        Ok(())
    }
}
