use crossbeam_channel::{self, Receiver, Sender};
use hdfs_comm::rpc::Client;
use hdfs_protos::hadoop::hdfs::datanode::{BlockReportResponseProto, BlockReportRequestProto, HeartbeatResponseProto, HeartbeatRequestProto, DatanodeRegistrationProto, RegisterDatanodeRequestProto, RegisterDatanodeResponseProto, StorageBlockReportProto};
use prost::Message;
use shared::NahError;
use shared::protos::{BlockIndexProto, BlockMetadataProto, GeohashIndexProto};

use std::fs::File;
use std::io::Read;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct NamenodeProtocol {
    dr_proto: DatanodeRegistrationProto,
    block_report_duration: Duration,
    heartbeat_duration: Duration,
    data_directory: String,
    ip_address: String,
    port: u16,
    join_handle: Option<JoinHandle<()>>,
    shutdown_channel: (Sender<bool>, Receiver<bool>),
}

impl NamenodeProtocol {
    pub fn new(dr_proto: DatanodeRegistrationProto, heartbeat_ms: u64,
            block_report_ms: u64, data_directory: &str,
            ip_address: &str, port: u16) -> NamenodeProtocol {
        NamenodeProtocol {
            dr_proto: dr_proto,
            block_report_duration: Duration::from_millis(block_report_ms),
            heartbeat_duration: Duration::from_millis(heartbeat_ms),
            data_directory: data_directory.to_string(),
            ip_address: ip_address.to_string(),
            port: port,
            join_handle: None,
            shutdown_channel: crossbeam_channel::bounded(1),
        }
    }

    pub fn start(&mut self) -> Result<(), NahError> {
        // initialize RegisterDatanodeRequestProto
        let mut rdr_proto = RegisterDatanodeRequestProto::default();
        rdr_proto.registration = self.dr_proto.clone();

        debug!("writing RegistrationDatanodeRequestProto to {}:{}",
            self.ip_address, self.port);

        // send RegisterDatanodeRequestProto
        let mut client = Client::new(&self.ip_address, self.port)?;
        client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "registerDatanode", rdr_proto)?;

        // TODO - read respnose

        // initialize shutdown and tick channels
        let shutdown_receiver = self.shutdown_channel.1.clone();
        let block_report_tick = 
            crossbeam_channel::tick(self.block_report_duration);
        let heartbeat_tick = 
            crossbeam_channel::tick(self.heartbeat_duration);

        // clone variables
        let dr_proto_clone = self.dr_proto.clone();
        let data_directory_clone = self.data_directory.clone();
        let ip_address_clone = self.ip_address.clone();
        let port_clone = self.port.clone();

        // start thread
        let join_handle = std::thread::spawn(move || {
            loop {
                select! {
                    recv(block_report_tick) -> _ => {
                        if let Err(e) = block_report(&ip_address_clone,
                                port_clone, &data_directory_clone,
                                &dr_proto_clone) {
                            warn!("block report failed: {}", e);
                        }
                    },
                    recv(heartbeat_tick) -> _ => {
                        if let Err(e) = heartbeat(&ip_address_clone,
                                port_clone) {
                            warn!("heartbeat failed: {}", e);
                        }
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

fn block_report(ip_address: &str, port: u16, data_directory: &str,
        dr_proto: &DatanodeRegistrationProto) -> Result<(), NahError> {
    // initialize StorageBlockReportProto
    let mut sbr_proto = StorageBlockReportProto::default();

    // read block metadata files
    let metadata_glob = format!("{}/*.meta", data_directory);
    let mut buf = Vec::new();
    for entry in glob::glob(&metadata_glob)? {
        // read file into buffer
        buf.clear();
        let mut file = File::open(entry?)?;
        file.read_to_end(&mut buf)?;

        // parse BlockMetadataProto
        let bm_proto = BlockMetadataProto
            ::decode_length_delimited(&buf)?;

        // add block metadata to StorageBlockReportProto
        let blocks = &mut sbr_proto.blocks;
        blocks.push(bm_proto.block_id);
        blocks.push(bm_proto.length);
        blocks.push(0u64); // TODO - generation stamp
        blocks.push(0u64); // TODO - replica state

        // TODO - process block index metadata
    }

    trace!("writing BlockReportRequest to {}:{}", ip_address, port);

    // initialize BlockReportRequestProto 
    let mut brr_proto = BlockReportRequestProto::default();
    brr_proto.registration = dr_proto.clone();
    brr_proto.reports.push(sbr_proto);

    let mut client = Client::new(ip_address, port)?;
    client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "blockReport", brr_proto)?;

    // TODO - read response

    Ok(())
}

fn heartbeat(ip_address: &str, port: u16) -> std::io::Result<()> {
    // initialize HeartbeatRequestProto 
    let mut hr_proto = HeartbeatRequestProto::default();
    // TODO - populate HeartbeatRequestProto

    trace!("writing HeartbeatRequest to {}:{}", ip_address, port);

    let mut client = Client::new(ip_address, port)?;
    client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "heartbeat", hr_proto)?;

    // TODO - read response

    Ok(())
}
