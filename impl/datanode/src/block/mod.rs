use prost::Message;
use shared::NahError;

mod processor;
pub use processor::BlockProcessor;

use shared::protos::{BlockIndexProto, BlockMetadataProto};

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Read, SeekFrom, Write};
use std::io::prelude::*;
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
    timestamps: Option<(u64, u64)>,
    index: Option<BTreeMap<String, Vec<(usize, usize)>>>,
}

impl BlockOperation {
    pub fn new(operation: Operation, block_id: u64,
            data: Vec<u8>) -> BlockOperation {
        BlockOperation {
            operation: operation,
            block_id: block_id,
            data: data,
            timestamps: None,
            index: None,
        }
    }
}

fn index_block(block_op: &mut BlockOperation) -> Result<(), NahError> {
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
                let indices = geohashes.entry(geohash)
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

    // update block_op index
    block_op.timestamps = Some((min_timestamp, max_timestamp));
    block_op.index = Some(geohashes);

    let elapsed = now.elapsed().unwrap();
    debug!("indexed block {} in {}.{}s", block_op.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

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

fn read_block(block_id: u64, offset: u64, data_directory: &str,
        buf: &mut [u8]) -> Result<(), NahError> {
    // open file
    let mut file = File::open(&format!("{}/blk_{}",
        data_directory, block_id))?;
    file.seek(SeekFrom::Start(offset))?;

    // read contents
    file.read_exact(buf)?;

    Ok(())
}

fn read_indexed_block(block_id: u64, geohashes: &Vec<u8>, offset: u64,
        data_directory: &str, buf: &mut [u8]) -> Result<(), NahError> {
    // read block metadata
    let mut metadata_buf = Vec::new();
    let mut meta_file = File::open(format!("{}/blk_{}.meta", 
        data_directory, block_id))?;
    meta_file.read_to_end(&mut metadata_buf)?;

    let mut bm_proto = BlockMetadataProto
        ::decode_length_delimited(&metadata_buf)?;

    // open file
    let mut file = File::open(&format!("{}/blk_{}",
        data_directory, block_id))?;

    let mut buf_index = 0;
    let mut remaining_offset = offset;
    if let Some(bi_proto) = &mut bm_proto.index {
        println!("bi_proto geohash {:?}", bi_proto.geohashes);
        for i in 0..bi_proto.geohashes.len() {
            // get last character of geohash
            let last_char = bi_proto.geohashes[i].pop().unwrap_or('x');
            let key = match last_char as u8 {
                x if x >= 48 && x <= 58 => x - 48,
                x if x >= 97 && x <= 102 => x - 87,
                _ => {
                    warn!("invalid geohash character {}", last_char);
                    continue;
                },
            };

            println!("geohash check: {} {:?}", &key, geohashes);
            if geohashes.contains(&key) {
                println!("processing geohash: {}", &key);
                // process geohash 
                let mut start_index = bi_proto.start_indices[i] as u64;
                let end_index = bi_proto.end_indices[i] as u64;
                println!("start,end: {}, {}", start_index, end_index);

                while start_index < end_index {
                    let index_length = end_index - start_index;

                    if remaining_offset > 0 {
                        // skip byte_count bytes for block offset
                        let byte_count = std::cmp
                            ::min(remaining_offset, index_length);

                        start_index += byte_count;
                        remaining_offset -= byte_count;
                        println!("offset bytes: {}", byte_count);
                    } else {
                        // read index_length bytes into buf
                        file.seek(SeekFrom::Start(start_index))?;
                        file.read_exact(&mut buf[buf_index..
                            buf_index + (index_length as usize)])?;

                        println!("read bytes: {} to index {}", index_length, buf_index);
                        buf_index += index_length as usize;
                        start_index += index_length;
                    }
                }
            }
        }
    }

    Ok(())
}

fn transfer_block(block_op: &BlockOperation) -> Result<(), NahError> {
    println!("TODO - TRANSFER BLOCK: {}", block_op.block_id);
    Ok(())
}

fn write_block(block_op: &BlockOperation,
        data_directory: &str) -> Result<(), NahError> {
    let now = SystemTime::now();

    // write block and compute block metadata
    let file = File::create(format!("{}/blk_{}", 
        data_directory, block_op.block_id))?;
    let mut buf_writer = BufWriter::new(file);

    let mut bm_proto = BlockMetadataProto::default();
    bm_proto.block_id = block_op.block_id;

    if let (Some(block_index), Some((start_timestamp, end_timestamp)))
            = (&block_op.index, block_op.timestamps) {
        // write indexed data
        let mut bi_proto = BlockIndexProto::default();
        let geohashes = &mut bi_proto.geohashes;
        let start_indices = &mut bi_proto.start_indices;
        let end_indices = &mut bi_proto.end_indices;

        let mut current_index = 0;
        for (geohash, indices) in block_index {
            geohashes.push(geohash.to_string());
            start_indices.push(current_index as u32); 

            for (start_index, end_index) in indices {
                buf_writer.write_all(
                    &block_op.data[*start_index..*end_index])?;
                current_index += end_index - start_index;
            }

            end_indices.push(current_index as u32);
        }

        bi_proto.start_timestamp = start_timestamp;
        bi_proto.end_timestamp = end_timestamp;

        bm_proto.length = current_index as u64;
        bm_proto.index = Some(bi_proto);
    } else {
        // write data
        buf_writer.write_all(&block_op.data)?;

        bm_proto.length = block_op.data.len() as u64;
    }

    // write block metadata
    let mut buf = Vec::new();
    bm_proto.encode_length_delimited(&mut buf)?;

    let meta_file = File::create(format!("{}/blk_{}.meta", 
        data_directory, block_op.block_id))?;
    let mut meta_buf_writer = BufWriter::new(meta_file);
    meta_buf_writer.write_all(&buf)?;

    let elapsed = now.elapsed().unwrap();
    debug!("wrote block {} in {}.{}s", block_op.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    Ok(())
}
