use crossbeam_channel::{self, Receiver, Sender, SendError};
use hdfs_comm::protos::hdfs::DatanodeIdProto;
use shared::NahFSError;
use shared::protos::BlockMetadataProto;

use crate::index::IndexStore;

use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

static INDEXED_MASK: u64 = 18446744004990074880;

pub enum Operation {
    INDEX,
    WRITE,
    TRANSFER,
}

pub struct BlockOperation {
    operation: Operation,
    pub bm_proto: BlockMetadataProto,
    pub data: Vec<u8>, 
    pub replicas: Vec<DatanodeIdProto>,
}

impl BlockOperation {
    pub fn new(operation: Operation, bm_proto: BlockMetadataProto,
            data: Vec<u8>, replicas: Vec<DatanodeIdProto>) -> BlockOperation {
        BlockOperation {
            operation: operation,
            bm_proto: bm_proto,
            data: data,
            replicas: replicas,
        }
    }
}

pub struct BlockProcessor {
    index_store: Arc<RwLock<IndexStore>>,
    thread_count: u8,
    data_directory: String,
    datanode_id: String,
    namenode_ip_address: String,
    namenode_port: u16,
    operation_channel: (Sender<BlockOperation>,
        Receiver<BlockOperation>),
    shutdown_channel: (Sender<bool>, Receiver<bool>),
    join_handles: Vec<JoinHandle<()>>,
}

impl BlockProcessor {
    pub fn new(index_store: Arc<RwLock<IndexStore>>, thread_count: u8,
            queue_length: u8, data_directory: String, datanode_id: String,
            namenode_ip_address: String, namenode_port: u16) -> BlockProcessor {
        BlockProcessor {
            index_store: index_store,
            thread_count: thread_count,
            data_directory: data_directory,
            datanode_id: datanode_id,
            namenode_ip_address: namenode_ip_address,
            namenode_port: namenode_port,
            operation_channel: crossbeam_channel
                ::bounded(queue_length as usize),
            shutdown_channel: crossbeam_channel::unbounded(),
            join_handles: Vec::new(),
        }
    }

    pub fn add_index(&self, bm_proto: BlockMetadataProto, data: Vec<u8>,
            replicas: Vec<DatanodeIdProto>)
            -> Result<(), SendError<BlockOperation>> {
        let block_op = BlockOperation::new(Operation::INDEX,
            bm_proto, data, replicas);
        self.operation_channel.0.send(block_op)
    }

    pub fn add_write(&self, bm_proto: BlockMetadataProto, data: Vec<u8>,
            replicas: Vec<DatanodeIdProto>)
            -> Result<(), SendError<BlockOperation>> {
        let block_op = BlockOperation::new(Operation::WRITE,
            bm_proto, data, replicas);
        self.operation_channel.0.send(block_op)
    }

    pub fn read(&self, block_id: u64, offset: u64,
            buf: &mut [u8]) -> Result<(), NahFSError> {
        super::read_block(block_id, offset, &self.data_directory, buf)
    }

    pub fn read_indexed(&self, block_id: u64, geohashes: &Vec<u8>,
            offset: u64, buf: &mut [u8]) -> Result<(), NahFSError> {
        super::read_indexed_block(block_id,
            geohashes, offset, &self.data_directory, buf)
    }

    pub fn start(&mut self) -> Result<(), NahFSError> {
        for _ in 0..self.thread_count {
            // clone variables
            let index_store_clone = self.index_store.clone();
            let data_directory_clone = self.data_directory.clone();
            let datanode_id_clone = self.datanode_id.clone();
            let namenode_ip_address_clone = self.namenode_ip_address.clone();
            let namenode_port_clone = self.namenode_port.clone();
            let operation_sender = self.operation_channel.0.clone();
            let operation_receiver = self.operation_channel.1.clone();
            let shutdown_receiver = self.shutdown_channel.1.clone();

            let join_handle = std::thread::spawn(move || {
                process_loop(index_store_clone,
                    &operation_sender, &operation_receiver,
                    &shutdown_receiver, &data_directory_clone, 
                    &datanode_id_clone, &namenode_ip_address_clone,
                    namenode_port_clone);
            });

            self.join_handles.push(join_handle);
        }

        Ok(())
    }

    /*
    // TODO - unused
    pub fn stop(mut self) {
        // send shutdown messages
        for _ in 0..self.join_handles.len() {
            self.shutdown_channel.0.send(true).unwrap();
        }

	// join threads
        while self.join_handles.len() != 0 {
            let join_handle = self.join_handles.pop().unwrap();
            join_handle.join().unwrap();
        }
    }*/
}

fn process_loop(index_store: Arc<RwLock<IndexStore>>,
        operation_sender: &Sender<BlockOperation>,
        operation_receiver: &Receiver<BlockOperation>,
        shutdown_receiver: &Receiver<bool>, data_directory: &str,
        datanode_id: &str, namenode_ip_address: &str, namenode_port: u16) {
    loop {
        select! {
            recv(operation_receiver) -> result => {
                // read block operation
                if let Err(e) = result {
                    error!("recv block operation: {}", e);
                    continue;
                }

                // process block operation
                let mut block_op = result.unwrap();
                let process_result = match (&block_op.operation,
                        &block_op.bm_proto.index) {
                    (Operation::INDEX, _) =>
                        index_block(&index_store, &mut block_op),
                    (Operation::WRITE, _) => 
                        super::write_block(&block_op.data,
                            &block_op.bm_proto, &data_directory),
                    (Operation::TRANSFER, None) =>
                        super::transfer_block(&block_op.data,
                            &block_op.replicas, &block_op.bm_proto),
                    (Operation::TRANSFER, Some(_)) =>
                        super::transfer_indexed_block(&block_op.data,
                            &block_op.bm_proto, &datanode_id,
                            block_op.replicas.len() as u32,
                            &namenode_ip_address, namenode_port),
                };

                // check for error
                if let Err(e) = process_result {
                    error!("processing block: {}", e);
                    continue;
                }

                // send block operation to next stage
                let send_result = match block_op.operation {
                    Operation::INDEX => {
                        block_op.operation = Operation::WRITE;
                        operation_sender.send(block_op)
                    },
                    Operation::WRITE => {
                        if block_op.replicas.len() != 0 {
                            block_op.operation = Operation::TRANSFER;
                            operation_sender.send(block_op)
                        } else {
                            Ok(())
                        }
                    },
                    Operation::TRANSFER => Ok(()),
                };

                // check for error
                if let Err(e) = send_result {
                    error!("sending processed block: {}", e);
                    continue;
                }
            },
            recv(shutdown_receiver) -> _ => break,
        }
    }
}

fn index_block(index_store: &Arc<RwLock<IndexStore>>,
        block_op: &mut BlockOperation) -> Result<(), NahFSError> {
    // parse storage_policy_id and get Indexer
    let storage_policy_id = block_op.bm_proto.block_id as u32;

    let available = {
        let index_store = index_store.read().unwrap();
        index_store.contains_index(&storage_policy_id)
    };

    if !available {
        let mut index_store = index_store.write().unwrap();
        index_store.retrieve_index(&storage_policy_id)?;
    }

    // retrieve indexer
    let index_store = index_store.read().unwrap();
    let indexer = index_store.get_index(&storage_policy_id).unwrap();

    // index block
    let (indexed_data, bi_proto) =
        indexer.process(&block_op.data, &block_op.bm_proto)?;

    // update BlockOperation
    block_op.bm_proto.block_id =
        block_op.bm_proto.block_id & INDEXED_MASK;
    block_op.bm_proto.index = Some(bi_proto);
    block_op.bm_proto.length = indexed_data.len() as u64;
    block_op.data = indexed_data;

    Ok(())
}
