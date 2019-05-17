use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use communication::StreamHandler;
use hdfs_comm::block::{BlockInputStream, BlockOutputStream};
use hdfs_protos::hadoop::hdfs::{BlockOpResponseProto, ChecksumProto, OpReadBlockProto, OpWriteBlockProto, ReadOpChecksumInfoProto, Status};
use prost::Message;

use crate::block::BlockProcessor;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::RwLock;

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
            let op_type = stream.read_u8()?;
            
            if version != PROTOCOL_VERSION {
                // TODO - error
            }

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
                    let owb_proto = OpWriteBlockProto::decode(&buf).unwrap();
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
                        stream.try_clone().unwrap(),
                        chunk_size_bytes, chunks_per_packet);
                    block_stream.read_to_end(&mut buf);
                    block_stream.close();

                    debug!("read {} bytes into block", buf.len());

                    // create Block struct
                    let block_id = owb_proto.header.base_header.block.block_id;
 
                    // parse block_id
                    let processor = self.processor.read().unwrap();
                    if block_id & FIRST_BIT_U64 == FIRST_BIT_U64 {
                        processor.add_index(block_id, buf);
                    } else {
                        processor.add_write(block_id, buf);
                    }
                },
                81 => {
                    // parse read block op
                    let orb_proto = OpReadBlockProto::decode(&buf).unwrap();
                    debug!("ReadBlock: {:?}", orb_proto);

                    let block_id = orb_proto.header.base_header.block.block_id;
                    let offset = orb_proto.offset;
                    let len = orb_proto.len;
 
                    // send op respone
                    let mut c_proto = ChecksumProto::default();;
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
                    let processor = self.processor.read().unwrap();
                    let mut buf = vec![0u8; len as usize];
                    if let Err(e) = processor.read(block_id, 
                            offset, &mut buf) {
                        warn!("processor read block {}: {}", block_id, e);
                    }
 
                    // send block
                    let mut block_stream = BlockOutputStream::new(
                        stream.try_clone().unwrap(),
                        chunk_size_bytes, chunks_per_packet);
                    block_stream.write_all(&mut buf);
                    block_stream.close();

                    debug!("wrote {} bytes from block", buf.len());
                },
                _ => unimplemented!(),
            }
        }
    }
}
