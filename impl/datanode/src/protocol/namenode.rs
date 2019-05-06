use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crossbeam_channel::{self, Receiver, Sender};
use hdfs_comm::rpc::Client;
use hdfs_protos::hadoop::hdfs::datanode::{DatanodeRegistrationProto, RegisterDatanodeRequestProto, RegisterDatanodeResponseProto};
use prost::Message;
use shared::NahError;

use std::io::Write;
use std::net::TcpStream;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct NamenodeProtocol {
    dr_proto: DatanodeRegistrationProto,
    block_report_duration: Duration,
    heartbeat_duration: Duration,
    ip_address: String,
    port: u16,
    join_handle: Option<JoinHandle<()>>,
    shutdown_channel: (Sender<bool>, Receiver<bool>),
}

impl NamenodeProtocol {
    pub fn new(dr_proto: DatanodeRegistrationProto, 
            heartbeat_ms: u64, block_report_ms: u64,
            ip_address: &str, port: u16) -> NamenodeProtocol {
        NamenodeProtocol {
            dr_proto: dr_proto,
            block_report_duration: Duration::from_millis(block_report_ms),
            heartbeat_duration: Duration::from_millis(heartbeat_ms),
            ip_address: ip_address.to_string(),
            port: port,
            join_handle: None,
            shutdown_channel: crossbeam_channel::bounded(4),
        }
    }

    pub fn start(&mut self) -> Result<(), NahError> {
        // send DatanodeRegisterProto
        let mut rdr_proto = RegisterDatanodeRequestProto::default();
        rdr_proto.registration = self.dr_proto.clone(); // TODO - don't love clone

        debug!("writing RegistrationDatanodeRequestProto to {}:{}",
            self.ip_address, self.port);

        let mut client = Client::new(&self.ip_address, self.port)?;
        client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "registerDatanode", rdr_proto)?;

        // TODO - read respnose

        // initialize shutdown and tick channels
        let shutdown_receiver = self.shutdown_channel.1.clone();
        let block_report_tick = 
            crossbeam_channel::tick(self.block_report_duration);
        let heartbeat_tick = 
            crossbeam_channel::tick(self.heartbeat_duration);

        // start thread
        let join_handle = std::thread::spawn(move || {
            loop {
                select! {
                    recv(block_report_tick) -> _ => {
                        println!("TODO - block report");
                    },
                    recv(heartbeat_tick) -> _ => {
                        println!("TODO - heartbeat");
                    },
                    recv(shutdown_receiver) -> _ => break,
                }
            }
        });

        self.join_handle = Some(join_handle);
        Ok(())
    }

    pub fn stop(mut self) {
        if let Some(join_handle) = self.join_handle {
            self.shutdown_channel.0.send(true);
            join_handle.join();
        }

        self.join_handle = None;
    }
}

fn block_report() {
    
}

fn heartbeat() {

}
