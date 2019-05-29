use crossbeam_channel::{self, Receiver, Sender, SendError};
use shared::NahError;

use super::{BlockOperation, Operation};

use std::thread::JoinHandle;

pub struct BlockProcessor {
    thread_count: u8,
    data_directory: String,
    operation_channel: (Sender<BlockOperation>,
        Receiver<BlockOperation>),
    shutdown_channel: (Sender<bool>, Receiver<bool>),
    join_handles: Vec<JoinHandle<()>>,
}

impl BlockProcessor {
    pub fn new(thread_count: u8, queue_length:u8,
            data_directory: String) -> BlockProcessor {
        BlockProcessor {
            thread_count: thread_count,
            data_directory: data_directory,
            operation_channel: crossbeam_channel
                ::bounded(queue_length as usize),
            shutdown_channel: crossbeam_channel::unbounded(),
            join_handles: Vec::new(),
        }
    }

    pub fn add_index(&self, block_id: u64, data: Vec<u8>)
            -> Result<(), SendError<BlockOperation>> {
        let block_op = BlockOperation::new(Operation::INDEX, block_id, data);
        self.operation_channel.0.send(block_op)
    }

    pub fn add_write(&self, block_id: u64, data: Vec<u8>)
            -> Result<(), SendError<BlockOperation>> {
        let block_op = BlockOperation::new(Operation::WRITE, block_id, data);
        self.operation_channel.0.send(block_op)
    }

    pub fn read(&self, block_id: u64, offset: u64,
            buf: &mut [u8]) -> Result<(), NahError> {
        super::read_block(block_id, offset, &self.data_directory, buf)
    }

    pub fn read_indexed(&self, block_id: u64, geohashes: &Vec<u8>,
            offset: u64, buf: &mut [u8]) -> Result<(), NahError> {
        super::read_indexed_block(block_id,
            geohashes, offset, &self.data_directory, buf)
    }

    pub fn start(&mut self) -> Result<(), NahError> {
        for _ in 0..self.thread_count {
            // clone variables
            let data_directory_clone = self.data_directory.clone();
            let operation_sender = self.operation_channel.0.clone();
            let operation_receiver = self.operation_channel.1.clone();
            let shutdown_receiver = self.shutdown_channel.1.clone();

            let join_handle = std::thread::spawn(move || {
                process_loop(&operation_sender, &operation_receiver,
                    &shutdown_receiver, &data_directory_clone);
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

fn process_loop(operation_sender: &Sender<BlockOperation>,
        operation_receiver: &Receiver<BlockOperation>,
        shutdown_receiver: &Receiver<bool>, data_directory: &str) {
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
                let process_result = match block_op.operation {
                    Operation::INDEX =>
                        super::index_block(&mut block_op),
                    Operation::WRITE => 
                        super::write_block(&block_op, &data_directory),
                    Operation::TRANSFER =>
                        super::transfer_block(&block_op),
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
                        block_op.operation = Operation::TRANSFER;
                        operation_sender.send(block_op)
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
