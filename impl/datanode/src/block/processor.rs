use crossbeam_channel::{self, Receiver, Sender, SendError};
use shared::NahError;

use std::collections::BTreeMap;
use std::thread::JoinHandle;
use std::time::SystemTime;

pub enum Operation {
    INDEX,
    WRITE,
    TRANSFER,
}

pub struct BlockOperation {
    operation: Operation,
    block_id: u64,
    data: Vec<u8>, 
    index: Option<BTreeMap<String, Vec<(usize, usize)>>>,
}

impl BlockOperation {
    pub fn new(operation: Operation, block_id: u64,
            data: Vec<u8>) -> BlockOperation {
        BlockOperation {
            operation: operation,
            block_id: block_id,
            data: data,
            index: None,
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
        let block_op = BlockOperation::new(Operation::INDEX, block_id, data);
        self.operation_channel.0.send(block_op)
    }

    pub fn add_write(&self, block_id: u64, data: Vec<u8>)
            -> Result<(), SendError<BlockOperation>> {
        let block_op = BlockOperation::new(Operation::WRITE, block_id, data);
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
    let now = SystemTime::now();
    let mut geohashes: BTreeMap<String, Vec<(usize, usize)>> =
        BTreeMap::new();
    let (mut min_timestamp, mut max_timestamp) =
        (std::u64::MAX, std::u64::MIN);

    // iterate over each observation (separated by NEWLINE)
    let mut start_index = 0;
    let mut end_index = 0;
    let mut commas = Vec::new();
    while end_index < block_op.data.len() {
        // find end_index of current observation
        while end_index != block_op.data.len() {
            match block_op.data[end_index] {
                44 => commas.push(end_index - start_index), // COMMA
                10 => break, // NEWLINE
                _ => (),
            }

            end_index += 1;
        }

        // parse metadata
        match parse_metadata(&block_op.data[start_index..end_index], &commas) {
            Ok((geohash, timestamp)) => {
                // index observation
                let mut indicies = geohashes.entry(geohash)
                    .or_insert(Vec::new());
                indicies.push((start_index, end_index));

                // process timestamps
                if timestamp < min_timestamp {
                    min_timestamp = timestamp;
                }

                if timestamp > max_timestamp {
                    max_timestamp = timestamp;
                }
            },
            Err(e) => warn!("parse observation metadata: {}", e),
        }

        // set start_index 
        end_index += 1;
        start_index = end_index;
        commas.clear();
    }

    let elapsed = now.elapsed().unwrap();
    debug!("indexed block {} in {}.{}", block_op.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    // TODO - add timestamps to block index
    block_op.index = Some(geohashes);
    Ok(())
}

fn parse_metadata(data: &[u8], commas: &Vec<usize>)
        -> Result<(String, u64), NahError> {
    let latitude_str = String::from_utf8_lossy(&data[0..commas[0]]);
    let longitude_str = String::from_utf8_lossy(&data[commas[0]+1..commas[1]]);
    let timestamp_str = String::from_utf8_lossy(&data[commas[2]+1..commas[3]-2]);

    let latitude = latitude_str.parse::<f32>()?;
    let longitude = longitude_str.parse::<f32>()?;
    let timestamp = timestamp_str.parse::<u64>()?;

    let geohash = geohash::encode_16(latitude, longitude, 4)?;
    Ok((geohash, timestamp))
}

fn write_block(block_op: &BlockOperation) -> Result<(), NahError> {
    println!("TODO - WRITE BLOCK: {}", block_op.block_id);

    Ok(())
}

fn transfer_block(block_op: &BlockOperation) -> Result<(), NahError> {
    println!("TODO - TRANSFER BLOCK: {}", block_op.block_id);
    Ok(())
}
