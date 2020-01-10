use hdfs_comm::rpc::Protocol;
use prost::Message;
use shared::NahFSError;
use shared::protos::{BlockIndexProto, GetIndexReplicasRequestProto, GetIndexReplicasResponseProto, GetStoragePolicyResponseProto, GetStoragePolicyRequestProto, IndexReportResponseProto, IndexReportRequestProto, IndexViewResponseProto, IndexViewRequestProto, InodePersistResponseProto, InodePersistRequestProto, SpatialIndexProto, TemporalIndexProto};

use crate::{BlockStore, DatanodeStore};
use crate::file::FileStore;
use crate::index::Index;

use std::fs::File;
use std::io::{Write};
use std::sync::{Arc, RwLock};

pub struct NahFSProtocol {
    block_store: Arc<RwLock<BlockStore>>,
    datanode_store: Arc<RwLock<DatanodeStore>>,
    file_store: Arc<RwLock<FileStore>>,
    index: Arc<RwLock<Index>>,
    persist_path: String,
}

impl NahFSProtocol {
    pub fn new(block_store: Arc<RwLock<BlockStore>>,
            datanode_store: Arc<RwLock<DatanodeStore>>,
            file_store: Arc<RwLock<FileStore>>,
            index: Arc<RwLock<Index>>, persist_path: &str) -> NahFSProtocol {
        NahFSProtocol {
            block_store: block_store,
            datanode_store: datanode_store,
            file_store: file_store,
            index: index,
            persist_path: persist_path.to_string(),
        }
    }

    fn get_index_replicas(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), NahFSError> {
        let request = GetIndexReplicasRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = GetIndexReplicasResponseProto::default();

        debug!("getIndexReplicas({:?})", request);

        // retrieve block spatial attributes
        let geohashes = match request.block_index.spatial_index {
            Some(spatial_index) => spatial_index.geohashes,
            None => Vec::new(),
        };

        // compute spatiotemporal datanode storage usage
        let block_store = self.block_store.read().unwrap();
        let datanode_store = self.datanode_store.read().unwrap();
        let index = self.index.read().unwrap();
        let mut datanodes = super::get_spatiotemporal_datanode_usage(
            &block_store, &datanode_store, &index, &geohashes);

        // place a replica on the most used datanode
        if request.datanode_id != datanodes.last().unwrap().0 {
            let datanode = datanode_store
                .get_datanode(&datanodes.last().unwrap().0).unwrap();

            // serialize DatanodeIdProto
            let di_proto = super::to_datanode_id_proto(datanode);
            let mut buf = Vec::new();
            di_proto.encode_length_delimited(&mut buf)?;

            response.datanode_id_protos.push(buf);

            // remove datanode from list
            datanodes.remove(datanodes.len() - 1);
        }

        // choose subsequent replicas based on storage usage
        while response.datanode_id_protos.len()
                < request.replication as usize {
           let index = super::select_block_replica(&datanodes);    
           let datanode = datanode_store
               .get_datanode(&datanodes[index].0).unwrap();

            // serialize DatanodeIdProto
            let di_proto = super::to_datanode_id_proto(datanode);
            let mut buf = Vec::new();
            di_proto.encode_length_delimited(&mut buf)?;

            response.datanode_id_protos.push(buf);

            // remove datanode from list
            datanodes.remove(index);
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn get_storage_policy(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), NahFSError> {
        let request = GetStoragePolicyRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = GetStoragePolicyResponseProto::default();

        // retrieve storage policy
        debug!("getStoragePolicy({:?})", request);
        let file_store: &FileStore = &self.file_store.read().unwrap();
        match file_store.get_storage_policy_str(&request.id) {
            Some(storage_policy) =>
                response.storage_policy = storage_policy.to_owned(),
            None => return Err(NahFSError::from(
                format!("storage policy {} not found", request.id))),
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn index_report(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), NahFSError> {
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
                    index.update_spatial(*block_id, &si_proto.geohashes[j],
                        si_proto.end_indices[j] - si_proto.start_indices[j])?;
                }
            }

            // add time range
            if let Some(ti_proto) = &bi_proto.temporal_index {
                index.update_temporal(*block_id,
                    ti_proto.start_timestamp, ti_proto.end_timestamp)?;
            }
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn index_view(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), NahFSError> {
        let request = IndexViewRequestProto
            ::decode_length_delimited(req_buf)?;
        let mut response = IndexViewResponseProto::default();

        // process index view
        debug!("indexView({:?})", request);
        let index = self.index.read().unwrap();
        let map = &mut response.blocks;

        // process spatial index
        for (block_id, geohash_map) in index.spatial_iter() {
            let mut si_proto = SpatialIndexProto::default();
            for (geohash, length) in geohash_map {
                si_proto.geohashes.push(geohash.to_string());
                si_proto.start_indices.push(0);
                si_proto.end_indices.push(*length);
            }

            let mut bi_proto = BlockIndexProto::default();
            bi_proto.spatial_index = Some(si_proto);
            map.insert(*block_id, bi_proto);
        }

        // process temporal index
        for (block_id, (start_timestamp, end_timestamp))
                in index.temporal_iter() {
            let mut ti_proto = TemporalIndexProto::default();
            ti_proto.start_timestamp = *start_timestamp;
            ti_proto.end_timestamp = *end_timestamp;

            let mut bi_proto = map.entry(*block_id)
                .or_insert(BlockIndexProto::default());
            bi_proto.temporal_index = Some(ti_proto);
        }

        response.encode_length_delimited(resp_buf)?;
        Ok(())
    }

    fn inode_persist(&self, req_buf: &[u8],
            resp_buf: &mut Vec<u8>) -> Result<(), NahFSError> {
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

impl Protocol for NahFSProtocol {
    fn process(&self, _user: &Option<String>, method: &str,
            req_buf: &[u8], resp_buf: &mut Vec<u8>) -> std::io::Result<()> {
        match method {
            "getIndexReplicas" =>
                self.get_index_replicas(req_buf, resp_buf)?,
            "getStoragePolicy" =>
                self.get_storage_policy(req_buf, resp_buf)?,
            "indexReport" => self.index_report(req_buf, resp_buf)?,
            "indexView" => self.index_view(req_buf, resp_buf)?,
            "inodePersist" => self.inode_persist(req_buf, resp_buf)?,
            _ => error!("unimplemented method '{}'", method),
        }

        Ok(())
    }
}
