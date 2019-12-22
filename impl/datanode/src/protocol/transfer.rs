use byteorder::{BigEndian, ReadBytesExt};
use communication::StreamHandler;
use hdfs_comm::block::{BlockInputStream, BlockOutputStream};
use hdfs_protos::hadoop::hdfs::{BlockOpResponseProto, ChecksumProto, OpReadBlockProto, OpWriteBlockProto, ReadOpChecksumInfoProto, Status};
use prost::Message;
use shared::protos::BlockMetadataProto;

use crate::block::BlockProcessor;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::RwLock;
use std::time::SystemTime;

static PROTOCOL_VERSION: u16 = 28;
static FIRST_BIT_U64: u64 = 9223372036854775808;
static FIRST_BIT_U8: u8 = 128;
static MASK_U8: u8 = 127;

pub struct TransferStreamHandler {
    processor: RwLock<BlockProcessor>,
}

impl TransferStreamHandler {
    pub fn new(processor: RwLock<BlockProcessor>)
            -> TransferStreamHandler {
        TransferStreamHandler {
            processor: processor,
        }
    }
}

impl StreamHandler for TransferStreamHandler {
    fn process(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        loop {
            // read op
            let version = stream.read_u16::<BigEndian>()?;
            if version != PROTOCOL_VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Unsupported protocol version"));
            }

            let op_type = stream.read_u8()?;

            // calculate leb128 encoded op proto length
            let mut length = 0;
            for i in 0.. {
                let byte = stream.read_u8()?;
                length += ((byte & MASK_U8) as u64) << (i * 7);

                if byte & FIRST_BIT_U8 != FIRST_BIT_U8 {
                    break;
                }
            }

            // read op proto into buffer
            let mut buf = vec![0u8; length as usize];
            stream.read_exact(&mut buf)?;
 
            // TODO - parameterize these values
            let chunk_size_bytes = 512;
            let chunks_per_packet = 126;

            // read in proto
            match op_type {
                80 => {
                    // parse write block op
                    let owb_proto = OpWriteBlockProto::decode(&buf)?;
                    debug!("WriteBlock: {:?}", owb_proto);

                    // send op response
                    let mut bor_proto = BlockOpResponseProto::default();
                    bor_proto.status = Status::Success as i32;

                    let mut resp_buf = Vec::new();
                    bor_proto.encode_length_delimited(&mut resp_buf)?;
                    stream.write_all(&resp_buf)?;

                    // recv block
                    let mut buf = Vec::new();
                    let mut block_stream = BlockInputStream::new(
                        stream.try_clone()?, chunk_size_bytes,
                        chunks_per_packet);
                    block_stream.read_to_end(&mut buf)?;
                    block_stream.close();

                    debug!("read {} bytes into block", buf.len());

                    // create Block struct
                    let block_id = owb_proto.header.base_header.block.block_id;

                    let mut replicas = Vec::new();
                    for di_proto in owb_proto.targets {
                        replicas.push(di_proto.id);
                    }
 
                    // process block_id
                    let mut bm_proto = BlockMetadataProto::default();
                    bm_proto.block_id = block_id;
                    bm_proto.length = buf.len() as u64;

                    let processor = self.processor.read().unwrap();
                    let write_result = if block_id & FIRST_BIT_U64
                            == FIRST_BIT_U64 {
                        processor.add_index(bm_proto, buf, replicas)
                    } else {
                        processor.add_write(bm_proto, buf, replicas)
                    };

                    if let Err(e) = write_result {
                        warn!("processor write block {}: {}",
                            block_id, e);
                    }
                },
                81 => {
                    // parse read block op
                    let orb_proto = OpReadBlockProto::decode(&buf)?;
                    debug!("ReadBlock: {:?}", orb_proto);

                    let block_id = orb_proto.header.base_header.block.block_id;
                    let offset = orb_proto.offset;
                    let len = orb_proto.len;
 
                    // send op respone
                    let mut c_proto = ChecksumProto::default();
                    c_proto.bytes_per_checksum = chunk_size_bytes;

                    let mut roci_proto =
                        ReadOpChecksumInfoProto::default();
                    roci_proto.checksum = c_proto;
                    roci_proto.chunk_offset = offset;

                    let mut bor_proto = BlockOpResponseProto::default();
                    bor_proto.status = Status::Success as i32;
                    bor_proto.read_op_checksum_info = Some(roci_proto);

                    let mut resp_buf = Vec::new();
                    bor_proto.encode_length_delimited(&mut resp_buf)?;
                    stream.write_all(&resp_buf)?;

                    // read block from file
                    let read_start = SystemTime::now();
                    let processor = self.processor.read().unwrap();
                    let mut buf = vec![0u8; len as usize];

                    let read_result = if block_id & FIRST_BIT_U64
                            == FIRST_BIT_U64 {
                        // if indexed -> decode block id
                        let (decoded_id, geohashes) =
                            shared::block::decode_block_id(&block_id);

                        processor.read_indexed(decoded_id,
                            &geohashes, offset, &mut buf)
                    } else {
                        processor.read(block_id, offset, &mut buf)
                    };

                    if let Err(e) = read_result {
                        warn!("processor read block {}: {}",
                            block_id, e);
                    }
 
                    // send block
                    match orb_proto.header.client_name.as_str() {
                        "direct-client" => {
                            stream.write_all(&buf)?;
                            let _ = stream.read_u8()?;
                        },
                        _ => {
                            let mut block_stream = BlockOutputStream::new(
                                stream.try_clone()?, offset as i64,
                                chunk_size_bytes, chunks_per_packet);
                            block_stream.write_all(&mut buf)?;
                            block_stream.close();
                        },
                    }

                    let read_duration =
                        SystemTime::now().duration_since(read_start);
                    debug!("read block {} with length {} in {:?}",
                        block_id, buf.len(), read_duration);
                },
                82 => {
                    // parse block metadata op
                    let bm_proto = BlockMetadataProto::decode(&buf)?;
                    debug!("WriteReplica: {:?}", bm_proto);

                    // recv block
                    let mut buf = vec![0u8; bm_proto.length as usize];
                    stream.read_exact(&mut buf)?;

                    debug!("read {} bytes into transfer block",
                        buf.len());

                    // write buf and BlockMetadataProto
                    let block_id = bm_proto.block_id;
                    let processor = self.processor.write().unwrap();
                    if let Err(e) = processor.add_write(
                            bm_proto, buf, Vec::new()) {
                        warn!("processor write transfered block {}: {}",
                            block_id, e);
                    }
                },
                _ => unimplemented!(),
            }
        }
    }
}
