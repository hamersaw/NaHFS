use crossbeam_channel::{self, Receiver, Sender, SendError};
use shared::NahError;

use super::Block;

use std::thread::JoinHandle;

pub struct BlockProcessor {
    thread_count: u8,
    index_channel: (Sender<Block>, Receiver<Block>),
    write_channel: (Sender<Block>, Receiver<Block>),
    transfer_channel: (Sender<Block>, Receiver<Block>),
    shutdown_channel: (Sender<bool>, Receiver<bool>),
    join_handles: Vec<JoinHandle<()>>,
}

impl BlockProcessor {
    pub fn new(thread_count: u8) -> BlockProcessor {
        BlockProcessor {
            thread_count: thread_count,
            // TODO - bound channels to alleviate memory contention
            index_channel: crossbeam_channel::unbounded(),
            write_channel: crossbeam_channel::unbounded(),
            transfer_channel: crossbeam_channel::unbounded(),
            shutdown_channel: crossbeam_channel::unbounded(),
            join_handles: Vec::new(),
        }
    }

    pub fn add_index(&self, block: Block)
            -> Result<(), SendError<Block>> {
        self.index_channel.0.send(block)
    }

    pub fn add_write(&self, block: Block)
            -> Result<(), SendError<Block>> {
        self.write_channel.0.send(block)
    }

    pub fn start(&mut self) -> Result<(), NahError> {
        for _ in 0..self.thread_count {
            // clone variables
            let index_receiver = self.index_channel.1.clone();
            let write_receiver = self.write_channel.1.clone();
            let transfer_receiver = self.transfer_channel.1.clone();
            let shutdown_receiver = self.shutdown_channel.1.clone();

            let join_handle = std::thread::spawn(move || {
                loop {
                    select! {
                        recv(write_receiver) -> result => {
                            if let Err(e) = result {
                                error!("recv write block: {}", e);
                            } else if let Err(e) =
                                    write_block(&result.unwrap()) {
                                error!("write block: {}", e);
                            }
                        },
                        recv(index_receiver) -> result => {
                            if let Err(e) = result {
                                error!("recv idnex block: {}", e);
                            } else if let Err(e) =
                                    index_block(&mut result.unwrap()) {
                                error!("index block: {}", e);
                            }
                        },
                        recv(transfer_receiver) -> result => {
                            if let Err(e) = result {
                                error!("recv transfer block: {}", e);
                            } else if let Err(e) =
                                    transfer_block(&result.unwrap()) {
                                error!("transfer block: {}", e);
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

fn index_block(block: &mut Block) -> Result<(), NahError> {
    println!("TODO - INDEX BLOCK: {}", block.block_id);
    Ok(())
}

fn write_block(block: &Block) -> Result<(), NahError> {
    println!("TODO - WRITE BLOCK: {}", block.block_id);
    Ok(())
}

fn transfer_block(block: &Block) -> Result<(), NahError> {
    println!("TODO - TRANSFER BLOCK: {}", block.block_id);
    Ok(())
}
