use crossbeam_channel::{self, Receiver, Sender, SendError};
use prost::Message;
use shared::NahError;
use shared::protos::{BlockIndex, BlockMetadata, GeohashIndex};

use super::{BlockOperation, Operation};

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::thread::JoinHandle;
use std::time::SystemTime;

pub struct BlockProcessor {
    thread_count: u8,
    data_directory: String,
    operation_channel: (Sender<BlockOperation>,
        Receiver<BlockOperation>),
    shutdown_channel: (Sender<bool>, Receiver<bool>),
    join_handles: Vec<JoinHandle<()>>,
}

impl BlockProcessor {
    pub fn new(thread_count: u8, data_directory: String)
            -> BlockProcessor {
        BlockProcessor {
            thread_count: thread_count,
            data_directory: data_directory,
            operation_channel: crossbeam_channel::unbounded(), // TODO - bound channels to alleviate memory contention
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
            let data_directory_clone = self.data_directory.clone();
            let operation_sender = self.operation_channel.0.clone();
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
                                Operation::INDEX => index_block(
                                    block_op, &operation_sender),
                                Operation::WRITE => write_block(block_op,
                                    &operation_sender, &data_directory_clone),
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

fn index_block(mut block_op: BlockOperation,
        sender: &Sender<BlockOperation>) -> Result<(), NahError> {
    let now = SystemTime::now();
    let mut geohashes: BTreeMap<String, Vec<(usize, usize)>> =
        BTreeMap::new();
    let (mut min_timestamp, mut max_timestamp) =
        (std::u64::MAX, std::u64::MIN);

    let mut start_index;
    let mut end_index = 0;
    let mut delimiter_indices = Vec::new();
    let mut feature_count = 0;

    while end_index < block_op.data.len() - 1 {
        // initialize iteration variables
        start_index = end_index;
        delimiter_indices.clear();

        // compute observation boundaries
        while end_index < block_op.data.len() - 1 {
            end_index += 1;
            match block_op.data[end_index] {
                44 => delimiter_indices.push(end_index - start_index),
                10 => break, // NEWLINE
                _ => (),
            }
        }

        if block_op.data[end_index] == 10 {
            end_index += 1; // if currently on NEWLINE -> increment
        }

        // check if this is a valid observation
        if feature_count == 0 && start_index == 0 {
            continue; // first observation
        } else if feature_count == 0 {
            feature_count = delimiter_indices.len() + 1;
        } else if delimiter_indices.len() + 1 != feature_count {
            continue; // observations differing in feature counts
        }

        // process observation
        let observation = &block_op.data[start_index..end_index];
        match parse_metadata(observation, &delimiter_indices) {
            Ok((geohash, timestamp)) => {
                // index observation
                let mut indices = geohashes.entry(geohash)
                    .or_insert(Vec::new());
                indices.push((start_index, end_index));

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
    }

    let elapsed = now.elapsed().unwrap();
    debug!("indexed block {} in {}.{}s", block_op.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    // update block_op and write
    block_op.operation = Operation::WRITE;
    block_op.timestamps = Some((min_timestamp, max_timestamp));
    block_op.index = Some(geohashes);
    sender.send(block_op); // TODO - handle error

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

fn write_block(mut block_op: BlockOperation, 
        sender: &Sender<BlockOperation>, 
        data_directory: &str) -> Result<(), NahError> {
    let now = SystemTime::now();

    // write block and compute block metadata
    let mut file = File::create(format!("{}/blk_{}", 
        data_directory, block_op.block_id))?;
    let mut buf_writer = BufWriter::new(file);

    let mut block_metadata = BlockMetadata::default();
    block_metadata.block_id = block_op.block_id;
    if let Some(geohashes) = &block_op.index {
        // write indexed data
        let mut geohash_indices = Vec::new();
        let mut current_index = 0;
        for (geohash, indices) in geohashes {
            let mut geohash_index = GeohashIndex::default();
            geohash_index.geohash = geohash.to_string();
            geohash_index.start_index = current_index as u32;

            for (start_index, end_index) in indices {
                buf_writer.write_all(
                    &block_op.data[*start_index..*end_index])?;
                current_index += end_index - start_index;
            }

            geohash_index.end_index = current_index as u32;
            geohash_indices.push(geohash_index);
        }

        let mut block_index = BlockIndex::default();
        block_index.geohash_indices = geohash_indices;

        block_metadata.length = current_index as u64;
        block_metadata.index = Some(block_index);
    } else {
        // write data
        buf_writer.write_all(&block_op.data)?;
        
        block_metadata.length = block_op.data.len() as u64;
    }

    // write block metadata
    let mut buf = Vec::new();
    block_metadata.encode_length_delimited(&mut buf);

    let mut meta_file = File::create(format!("{}/blk_{}.meta", 
        data_directory, block_op.block_id))?;
    let mut meta_buf_writer = BufWriter::new(meta_file);
    meta_buf_writer.write_all(&buf)?;

    let elapsed = now.elapsed().unwrap();
    debug!("wrote block {} in {}.{}s", block_op.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    // update block_op and transfer
    block_op.operation = Operation::TRANSFER;
    sender.send(block_op); // TODO - handle error

    Ok(())
}

fn transfer_block(block_op: &BlockOperation) -> Result<(), NahError> {
    println!("TODO - TRANSFER BLOCK: {}", block_op.block_id);
    Ok(())
}
