use crossbeam_channel::{self, Receiver, Sender};
use hdfs_comm::rpc::Client;
use hdfs_protos::hadoop::hdfs::StorageReportProto;
use hdfs_protos::hadoop::hdfs::datanode::{BlockReportResponseProto, BlockReportRequestProto, HeartbeatResponseProto, HeartbeatRequestProto, RegisterDatanodeRequestProto, RegisterDatanodeResponseProto, StorageBlockReportProto};
use prost::Message;
use shared::NahFSError;
use shared::protos::{BlockMetadataProto, IndexReportResponseProto, IndexReportRequestProto};

use crate::Config;

use std::fs::File;
use std::io::Read;
use std::process::Command;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

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

    pub fn start(&mut self) -> Result<(), NahFSError> {
        // initialize RegisterDatanodeRequestProto
        let mut rdr_proto = RegisterDatanodeRequestProto::default();
        rdr_proto.registration =
            super::to_datanode_registration_proto(&self.config);

        debug!("writing RegistrationDatanodeRequestProto to {}:{}",
            self.config.namenode_ip_address, self.config.namenode_port);

        // send RegisterDatanodeRequestProto
        let mut client = Client::new(&self.config.namenode_ip_address,
            self.config.namenode_port as u16)?;
        let (_, resp_buf) = client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "registerDatanode", rdr_proto)?;

        // read respnose
        let _ = RegisterDatanodeResponseProto
            ::decode_length_delimited(resp_buf)?;

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
            let mut block_timestamp = 0;
            let mut index_timestamp = 0;
            loop {
                select! {
                    recv(block_report_tick) -> _ => {
                        match block_report(&config_clone, block_timestamp) {
                            Ok(timestamp) => block_timestamp = timestamp,
                            Err(e) => warn!("block report: {}", e),
                        }
                    },
                    recv(heartbeat_tick) -> _ => {
                        if let Err(e) = heartbeat(&config_clone) {
                            warn!("heartbeat failed: {}", e);
                        }
                    },
                    recv(index_tick) -> _ => {
                        match index_report(&config_clone, index_timestamp) {
                            Ok(timestamp) => index_timestamp = timestamp,
                            Err(e) => warn!("index report: {}", e),
                        }
                    },
                    recv(shutdown_receiver) -> _ => break,
                }
            }
        });

        self.join_handle = Some(join_handle);
        Ok(())
    }

    /*
    // TODO - unused
    pub fn stop(mut self) {
        if let Some(join_handle) = self.join_handle {
            self.shutdown_channel.0.send(true);
            join_handle.join();
        }

        self.join_handle = None;
    }*/
}

fn block_report(config: &Config, block_timestamp: u64)
        -> Result<u64, NahFSError> {
    // initialize StorageBlockReportProto
    let mut sbr_proto = StorageBlockReportProto::default();
    sbr_proto.storage = super::to_datanode_storage_proto(config);;

    // read block metadata files
    let mut max_timestamp = block_timestamp;
    let metadata_glob = format!("{}/*.meta", config.data_directory);
    let mut buf = Vec::new();
    for entry in glob::glob(&metadata_glob)? {
        let mut file = File::open(entry?)?;

        // validate file timestamp
        let timestamp = get_file_timestamp(&file);
        if timestamp <= block_timestamp {
            continue;
        }
        max_timestamp = std::cmp::max(max_timestamp, timestamp);

        // read file into buffer
        buf.clear();
        file.read_to_end(&mut buf)?;

        // parse BlockMetadataProto
        let bm_proto = BlockMetadataProto
            ::decode_length_delimited(&buf)?;

        // add block metadata to StorageBlockReportProto
        // block_id | block_length | generation_stamp | replica_state
        let blocks = &mut sbr_proto.blocks;
        blocks.push(bm_proto.block_id);
        blocks.push(bm_proto.length);
        blocks.push(0u64);
        blocks.push(0u64);
    }

    trace!("writing BlockReportRequest to {}:{} {:?}",
        config.namenode_ip_address, config.namenode_port, sbr_proto);

    // initialize BlockReportRequestProto 
    let mut brr_proto = BlockReportRequestProto::default();
    brr_proto.registration = super::to_datanode_registration_proto(config);;
    brr_proto.reports.push(sbr_proto);

    let mut client = Client::new(&config.namenode_ip_address, config.namenode_port)?;
    let (_, resp_buf) = client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "blockReport", brr_proto)?;

    // read response
    let _ = BlockReportResponseProto
        ::decode_length_delimited(resp_buf)?;

    Ok(max_timestamp)
}

fn heartbeat(config: &Config) -> std::io::Result<()> {
    // initialize StorageReportProto
    let mut sr_proto = StorageReportProto::default();
    sr_proto.storage_uuid = config.storage_id.to_string();
    sr_proto.storage = Some(super::to_datanode_storage_proto(config));
    sr_proto.failed = Some(false);

    let (mut used, mut dfs_used, mut available) = (0, 0, 0);
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
    hr_proto.xceiver_count = Some(1);
    hr_proto.cache_capacity = Some(0);
    hr_proto.cache_used = Some(0);
    // TODO - populate rest of HeartbeatRequestProto
    /*pub xmits_in_progress: ::std::option::Option<u32>,
    pub failed_volumes: ::std::option::Option<u32>,
    pub volume_failure_summary: ::std::option::Option<VolumeFailureSummaryProto>,
    pub request_full_block_report_lease: ::std::option::Option<bool>,*/

    trace!("writing HeartbeatRequest to {}:{} {:?}",
        config.namenode_ip_address, config.namenode_port, hr_proto);

    let mut client = Client::new(&config.namenode_ip_address, config.namenode_port)?;
    let (_, resp_buf) = client.write_message("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol", "heartbeat", hr_proto)?;

    // read response
    let _ = HeartbeatResponseProto::decode_length_delimited(resp_buf)?;

    Ok(())
}

fn index_report(config: &Config, index_timestamp: u64)
        -> Result<u64, NahFSError> {
    // initialize IndexReportRequestProto
    let mut irr_proto = IndexReportRequestProto::default();
    let block_ids = &mut irr_proto.block_ids;
    let indices = &mut irr_proto.block_indices;

    // read block metadata files
    let mut max_timestamp = index_timestamp;
    let metadata_glob = format!("{}/*.meta", config.data_directory);
    let mut buf = Vec::new();
    for entry in glob::glob(&metadata_glob)? {
        let mut file = File::open(entry?)?;

        // validate file timestamp
        let timestamp = get_file_timestamp(&file);
        if timestamp <= index_timestamp {
            continue;
        }
        max_timestamp = std::cmp::max(max_timestamp, timestamp);
 
        // read file into buffer
        buf.clear();
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
        return Ok(index_timestamp);
    }

    trace!("writing IndexReportRequestProto to {}:{} {:?}",
        config.namenode_ip_address, config.namenode_port, irr_proto);

    // write IndexReportRequestProto 
    let mut client = Client::new(&config.namenode_ip_address, config.namenode_port)?;
    let (_, resp_buf) = client.write_message("com.bushpath.nahfs.protocol.NahFSProtocol", "indexReport", irr_proto)?;

    // read response
    let _ = IndexReportResponseProto
        ::decode_length_delimited(resp_buf)?;

    Ok(max_timestamp)
}

fn get_file_timestamp(file: &File) -> u64 {
    match file.metadata() {
        Ok(metadata) => {
            match metadata.modified() {
                Ok(system_time) => {
                    match system_time.duration_since(SystemTime::UNIX_EPOCH) {
                        Ok(duration) => duration.as_secs(),
                        Err(_) => 0,
                    }
                },
                Err(_) => 0,
            }
        },
        Err(_) => 0,
    }
}
