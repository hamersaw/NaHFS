use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use communication::StreamHandler;

use std::net::TcpStream;

static PROTOCOL_VERSION: u16 = 28;

pub struct TransferStreamHandler {
}

impl TransferStreamHandler {
    pub fn new() -> TransferStreamHandler {
        TransferStreamHandler {
        }
    }
}

impl StreamHandler for TransferStreamHandler {
    fn process(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        loop {
            // read op
            let op_type = stream.read_u8()?;
            let version = stream.read_u16::<BigEndian>()?;
            
            if version != PROTOCOL_VERSION {
                // TODO - error
            }

            match op_type {
                80 => {
                    // TODO - write block op
                    unimplemented!();
                },
                81 => {
                    // TODO - read block op
                    unimplemented!();
                },
                _ => unimplemented!(),
            }
        }
        unimplemented!();
    }
}
