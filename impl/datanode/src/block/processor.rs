use crossbeam_channel::{self, Receiver, Sender, SendError};
use shared::NahError;

use super::Block;

use std::thread::JoinHandle;

pub enum Operation {
    INDEX,
    WRITE,
    TRANSFER,
}

pub struct BlockOperation {
    operation: Operation,
    block: Block,
    data: Vec<u8>, 
    write_indices: Vec<(u32, u32)>,
}

impl BlockOperation {
    pub fn new(operation: Operation, block: Block,
            data: Vec<u8>) -> BlockOperation {
        BlockOperation {
            operation: operation,
            block: block,
            data: data,
            write_indices: Vec::new(),
        }
    }
}

pub struct BlockProcessor {
    thread_count: u8,
    operation_channel: (Sender<BlockOperation>,
        Receiver<BlockOperation>),
    shutdown_channel: (Sender<bool>, Receiver<bool>),
    join_handles: Vec<JoinHandle<()>>,
}

impl BlockProcessor {
    pub fn new(thread_count: u8) -> BlockProcessor {
        BlockProcessor {
            thread_count: thread_count,
            // TODO - bound channels to alleviate memory contention
            operation_channel: crossbeam_channel::unbounded(),
            shutdown_channel: crossbeam_channel::unbounded(),
            join_handles: Vec::new(),
        }
    }

    pub fn add_index(&self, block_id: u64, data: Vec<u8>)
            -> Result<(), SendError<BlockOperation>> {
        let block = Block::new(block_id);
        let block_op = BlockOperation::new(Operation::INDEX, block, data);
        self.operation_channel.0.send(block_op)
    }

    pub fn add_write(&self, block_id: u64, data: Vec<u8>)
            -> Result<(), SendError<BlockOperation>> {
        let block = Block::new(block_id);
        let block_op = BlockOperation::new(Operation::WRITE, block, data);
        self.operation_channel.0.send(block_op)
    }

    pub fn start(&mut self) -> Result<(), NahError> {
        for _ in 0..self.thread_count {
            // clone variables
            let operation_receiver = self.operation_channel.1.clone();
            let shutdown_receiver = self.shutdown_channel.1.clone();

            let join_handle = std::thread::spawn(move || {
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
                            let result = match block_op.operation {
                                Operation::INDEX =>
                                    index_block(&mut block_op),
                                Operation::WRITE =>
                                    write_block(&block_op),
                                Operation::TRANSFER =>
                                    transfer_block(&block_op),
                            };

                            // check for error
                            if let Err(e) = result {
                                error!("processing block: {}", e);
                            }
                        },
                        recv(shutdown_receiver) -> _ => break,
                    }
                }
            });

            self.join_handles.push(join_handle);
        }

        Ok(())
    }

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
    }
}

fn index_block(block_op: &mut BlockOperation) -> Result<(), NahError> {
    println!("TODO - INDEX BLOCK: {}", block_op.block.block_id);
    Ok(())
}

fn write_block(block_op: &BlockOperation) -> Result<(), NahError> {
    println!("TODO - WRITE BLOCK: {}", block_op.block.block_id);
    Ok(())
}

fn transfer_block(block_op: &BlockOperation) -> Result<(), NahError> {
    println!("TODO - TRANSFER BLOCK: {}", block_op.block.block_id);
    Ok(())
}
