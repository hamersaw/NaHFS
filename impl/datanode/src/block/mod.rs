use byteorder::{BigEndian, WriteBytesExt};
use hdfs_comm::rpc::Client;
use hdfs_comm::protos::hdfs::DatanodeIdProto;
use prost::Message;
use shared::{self, NahFSError};
use shared::protos::{BlockMetadataProto, GetIndexReplicasRequestProto, GetIndexReplicasResponseProto};

mod processor;
pub use processor::BlockProcessor;

use std::fs::File;
use std::io::{BufWriter, Read, SeekFrom, Write};
use std::io::prelude::*;
use std::net::TcpStream;
use std::time::SystemTime;

fn read_block(block_id: u64, offset: u64, data_directory: &str,
        buf: &mut [u8]) -> Result<(), NahFSError> {
    // open file
    let mut file = File::open(&format!("{}/blk_{}",
        data_directory, block_id))?;
    file.seek(SeekFrom::Start(offset))?;

    // read contents
    file.read_exact(buf)?;

    Ok(())
}

fn read_indexed_block(block_id: u64, geohashes: &Vec<u8>, offset: u64,
        data_directory: &str, buf: &mut [u8]) -> Result<(), NahFSError> {
    // read block metadata
    let mut metadata_buf = Vec::new();
    let mut meta_file = File::open(format!("{}/blk_{}.meta", 
        data_directory, block_id))?;
    meta_file.read_to_end(&mut metadata_buf)?;

    let bm_proto = BlockMetadataProto
        ::decode_length_delimited(&metadata_buf)?;

    // open file
    let mut file = File::open(&format!("{}/blk_{}",
        data_directory, block_id))?;

    if let Some(mut bi_proto) = bm_proto.index {
        if let Some(si_proto) = &mut bi_proto.spatial_index {
            let mut buf_index = 0;
            let mut remaining_offset = offset;

            for i in 0..si_proto.geohashes.len() {
                // compute geohash key for last character in geohash
                let c = si_proto.geohashes[i].pop().unwrap_or('x');
                let geohash_key = match shared::geohash_char_to_value(c) {
                    Ok(geohash_key) => geohash_key,
                    Err(e) => {
                        warn!("failed to parse geohash: {}", e);
                        continue;
                    },
                };

                if geohashes.contains(&geohash_key) {
                    // if valid geohash -> process geohash 
                    let mut start_index = si_proto.start_indices[i] as u64;
                    let end_index = si_proto.end_indices[i] as u64;

                    while start_index < end_index {
                        // read length is minimum of index 
                        // length and remaining buffer
                        let read_length = std::cmp::min(
                            end_index - start_index,
                            (buf.len() - buf_index) as u64);

                        if remaining_offset > 0 {
                            // skip byte_count bytes for block offset
                            let byte_count = std::cmp
                                ::min(remaining_offset, read_length);

                            start_index += byte_count;
                            remaining_offset -= byte_count;
                        } else {
                            // read index_length bytes into buf
                            file.seek(SeekFrom::Start(start_index))?;
                            file.read_exact(&mut buf[buf_index..
                                buf_index + (read_length as usize)])?;

                            buf_index += read_length as usize;
                            start_index += read_length;
                        }

                        // check if buffer is full
                        if buf_index == buf.len() {
                            break;
                        }
                    }
                }
            }
        } else {
            // SpatialIndexProto does not exist
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(buf)?;
        }
    } else {
        // BlockIndexProto does not exist
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;
    }

    Ok(())
}

fn transfer_block(data: &Vec<u8>, replicas: &Vec<DatanodeIdProto>,
        bm_proto: &BlockMetadataProto) -> Result<(), NahFSError> {
    let now = SystemTime::now();

    // iterate over replicas
    for di_proto in replicas.iter() {
        // open socket
        match TcpStream::connect(&format!("{}:{}",
                di_proto.ip_addr, di_proto.xfer_port)) {
            Ok(stream) => {
                // write version and op
                let mut buf_writer =
                    BufWriter::new(stream.try_clone().unwrap());
                buf_writer.write_u16::<BigEndian>(28)?;
                buf_writer.write_u8(82)?;

                // write block metadata
                let mut buf = Vec::new();
                bm_proto.encode_length_delimited(&mut buf)?;
                buf_writer.write_all(&buf)?;
                buf_writer.flush()?;

                // write block
                buf_writer.write_all(&data)?;
                buf_writer.flush()?;
            },
            Err(e) => warn!("replicate block {} to node {} {}:{}: {}",
                bm_proto.block_id, di_proto.datanode_uuid,
                di_proto.ip_addr, di_proto.xfer_port, e),
        }
    }

    let elapsed = now.elapsed().unwrap();
    debug!("transfered block {} in {}.{}s", bm_proto.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    Ok(())
}

fn transfer_indexed_block(data: &Vec<u8>, bm_proto: &BlockMetadataProto,
        datanode_id: &str, replication: u32, namenode_ip_address: &str,
        namenode_port: u16) -> Result<(), NahFSError> {
    // if no replicas -> transfer is successful
    if replication == 0 {
        return Ok(());
    }

    // initialize GetIndexReplicasRequestProto
    let mut req_proto = GetIndexReplicasRequestProto::default();
    req_proto.datanode_id = datanode_id.to_string();
    req_proto.replication = replication;
    match &bm_proto.index {
        Some(index) => req_proto.block_index = index.clone(),
        None => return Err(NahFSError::from("unable to transfer_indexed_block with 'None' index")),
    }

    debug!("writing GetIndexReplicasRequestProto to {}:{}",
        namenode_ip_address, namenode_port);

    // send GetIndexReplicasRequestProto
    let mut client = Client::new(&namenode_ip_address, namenode_port)?;
    let (_, resp_buf) = client.write_message("io.blackpine.nahfs.protocol.NahFSProtocol", "getIndexReplicas", req_proto)?;

    // read response
    let resp_proto = GetIndexReplicasResponseProto
        ::decode_length_delimited(resp_buf)?;

    // decode replicas
    let mut replicas = Vec::new();
    for i in 0..resp_proto.datanode_id_protos.len() {
        let di_proto = DatanodeIdProto::decode_length_delimited(
            &resp_proto.datanode_id_protos[i])?;

        replicas.push(di_proto);
    }

    // transfer block
    transfer_block(data, &replicas, bm_proto)
}

fn write_block(data: &Vec<u8>, bm_proto: &BlockMetadataProto,
        data_directory: &str) -> Result<(), NahFSError> {
    let now = SystemTime::now();

    // write block
    let file = File::create(format!("{}/blk_{}", 
        data_directory, bm_proto.block_id))?;
    let mut buf_writer = BufWriter::new(file);

    buf_writer.write_all(data)?;

    // write block metadata
    let mut buf = Vec::new();
    bm_proto.encode_length_delimited(&mut buf)?;

    let meta_file = File::create(format!("{}/blk_{}.meta", 
        data_directory, bm_proto.block_id))?;
    let mut meta_buf_writer = BufWriter::new(meta_file);
    meta_buf_writer.write_all(&buf)?;

    let elapsed = now.elapsed().unwrap();
    debug!("wrote block {} and metadata in {}.{}s", bm_proto.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    Ok(())
}
