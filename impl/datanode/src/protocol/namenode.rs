use crossbeam_channel::{self, Receiver, Sender};
use hdfs_comm::rpc::Client;
use hdfs_protos::hadoop::hdfs::{DatanodeStorageProto, StorageReportProto};
use hdfs_protos::hadoop::hdfs::datanode::{BlockReportResponseProto, BlockReportRequestProto, HeartbeatResponseProto, HeartbeatRequestProto, DatanodeRegistrationProto, RegisterDatanodeRequestProto, RegisterDatanodeResponseProto, StorageBlockReportProto};
use prost::Message;
use shared::NahError;
use shared::protos::{BlockIndexProto, BlockMetadataProto, IndexReportResponseProto, IndexReportRequestProto};

use crate::Config;

use std::fs::File;
use std::io::Read;
use std::process::Command;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct NamenodeProtocol {
    config: Config,
    join_handle: Option<JoinHandle<()>>,
    shutdown_channel: (Sender<bool>, Receiver<bool>),
}

impl NamenodeProtocol {
    pub fn new(config: Config) -> NamenodeProtocol {
        NamenodeProtocol {
            config: config,
            join_handle: None,
            shutdown_channel: crossbeam_channel::bounded(1),
        }
    }

    pub fn start(&mut self) -> Result<(), NahError> {
        // initialize RegisterDatanodeRequestProto
        let mut rdr_proto = RegisterDatanodeRequestProto::default();
        rdr_proto.registration =
            super::to_datanode_registration_proto(&self.config);

        debug!("writing RegistrationDatanodeRequestProto to {}:{}",
            self.config.namenode_ip_address, self.config.namenode_port);

        // send RegisterDatanodeRequestProto
        let mut client = Client::new(&self.config.namenode_ip_address,
            self.config.namenode_port as u16)?;
        client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "registerDatanode", rdr_proto)?;

        // TODO - read respnose

        // initialize shutdown and tick channels
        let shutdown_receiver = self.shutdown_channel.1.clone();
        let block_report_tick = crossbeam_channel
            ::tick(Duration::from_millis(self.config.block_report_ms));
        let heartbeat_tick = crossbeam_channel
            ::tick(Duration::from_millis(self.config.heartbeat_ms));
        let index_tick = crossbeam_channel
            ::tick(Duration::from_millis(self.config.index_report_ms));

        // clone variables
        let config_clone = self.config.clone();

        // start thread
        let join_handle = std::thread::spawn(move || {
            loop {
                select! {
                    recv(block_report_tick) -> _ => {
                        if let Err(e) = block_report(&config_clone) {
                            warn!("block report failed: {}", e);
                        }
                    },
                    recv(heartbeat_tick) -> _ => {
                        if let Err(e) = heartbeat(&config_clone) {
                            warn!("heartbeat failed: {}", e);
                        }
                    },
                    recv(index_tick) -> _ => {
                        if let Err(e) = index_report(&config_clone) {
                            warn!("index report failed: {}", e);
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

fn block_report(config: &Config) -> Result<(), NahError> {
    // initialize StorageBlockReportProto
    let mut sbr_proto = StorageBlockReportProto::default();
    sbr_proto.storage = super::to_datanode_storage_proto(config);;

    // read block metadata files
    let metadata_glob = format!("{}/*.meta", config.data_directory);
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

    trace!("writing BlockReportRequest to {}:{}",
        config.namenode_ip_address, config.namenode_port);

    // initialize BlockReportRequestProto 
    let mut brr_proto = BlockReportRequestProto::default();
    brr_proto.registration = super::to_datanode_registration_proto(config);;
    brr_proto.reports.push(sbr_proto);

    let mut client = Client::new(&config.namenode_ip_address, config.namenode_port)?;
    client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "blockReport", brr_proto)?;

    // TODO - read response

    Ok(())
}

fn heartbeat(config: &Config) -> std::io::Result<()> {
    // initialize StorageReportProto
    let mut sr_proto = StorageReportProto::default();
    sr_proto.storage_uuid = config.storage_id.to_string();
    sr_proto.storage = Some(super::to_datanode_storage_proto(config));
    sr_proto.failed = Some(false);

    let (mut used, mut dfs_used, mut available, mut capacity) = (0, 0, 0, 0);
    if cfg!(target_os = "linux") {
        match Command::new("df").arg(&config.data_directory).output() {
            Ok(output) => {
                let output_str = 
                    String::from_utf8_lossy(&output.stdout);
                let fields: Vec<&str> =
                    output_str.split_whitespace().collect();

                used = fields[9].parse::<u64>().unwrap_or(0);
                available = fields[10].parse::<u64>().unwrap_or(0);
            },
            Err(e) => warn!("retreive heartbeat disk stats: {}", e),
        }

        match Command::new("du")
                .args(&["-d", "0", &config.data_directory])
                .output() {
            Ok(output) => {
                let output_str = 
                    String::from_utf8_lossy(&output.stdout);
                let fields: Vec<&str> =
                    output_str.split_whitespace().collect();

                dfs_used = fields[0].parse::<u64>().unwrap_or(0);
            },
            Err(e) => warn!("retreive heartbeat dfs used: {}", e),
        }
    } else {
        warn!("unsupported os - unable to retreive disk stats");
    }

    sr_proto.capacity = Some(used + available);
    sr_proto.dfs_used = Some(dfs_used);
    sr_proto.remaining = Some(available);
    sr_proto.block_pool_used = Some(dfs_used);
    sr_proto.non_dfs_used = Some(used - dfs_used);

    // initialize HeartbeatRequestProto 
    let mut hr_proto = HeartbeatRequestProto::default();
    hr_proto.registration = super::to_datanode_registration_proto(config);
    hr_proto.reports.push(sr_proto);
    hr_proto.xceiver_count = Some(config.thread_count as u32);
    hr_proto.cache_capacity = Some(0);
    hr_proto.cache_used = Some(0);
    // TODO - populate rest of HeartbeatRequestProto
    /*pub xmits_in_progress: ::std::option::Option<u32>,
    pub failed_volumes: ::std::option::Option<u32>,
    pub volume_failure_summary: ::std::option::Option<VolumeFailureSummaryProto>,
    pub request_full_block_report_lease: ::std::option::Option<bool>,*/

    trace!("writing HeartbeatRequest to {}:{}",
        config.namenode_ip_address, config.namenode_port);

    let mut client = Client::new(&config.namenode_ip_address, config.namenode_port)?;
    client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "heartbeat", hr_proto)?;

    // TODO - read response

    Ok(())
}

fn index_report(config: &Config) -> Result<(), NahError> {
    // initialize IndexReportRequestProto
    let mut irr_proto = IndexReportRequestProto::default();
    let block_ids = &mut irr_proto.block_ids;
    let indices = &mut irr_proto.block_indices;

    // read block metadata files
    let metadata_glob = format!("{}/*.meta", config.data_directory);
    let mut buf = Vec::new();
    for entry in glob::glob(&metadata_glob)? {
        // read file into buffer
        buf.clear();
        let mut file = File::open(entry?)?;
        file.read_to_end(&mut buf)?;

        // parse BlockMetadataProto
        let bm_proto = BlockMetadataProto
            ::decode_length_delimited(&buf)?;

        // process block index metadata
        if let Some(bi_proto) = bm_proto.index {
            block_ids.push(bm_proto.block_id);
            indices.push(bi_proto);
        }
    }

    if block_ids.len() == 0 {
        return Ok(());
    }

    trace!("writing IndexReportRequestProto to {}:{}",
        config.namenode_ip_address, config.namenode_port);

    // write IndexReportRequestProto 
    let mut client = Client::new(&config.namenode_ip_address, config.namenode_port)?;
    client.write_message("com.bushpath.nahfs.protocol.NahfsProtocol", "indexReport", irr_proto)?;

    // TODO - read response

    Ok(())
}
