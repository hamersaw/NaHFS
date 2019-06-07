#[macro_use]
extern crate log;
extern crate structopt;

use communication::Server;
use hdfs_comm::rpc::Protocols;
use structopt::StructOpt;

mod block;
mod datanode;
mod file;
mod index;
mod protocol;
mod storage;

use block::BlockStore;
use datanode::DatanodeStore;
use file::FileStore;
use index::Index;
use protocol::{ClientNamenodeProtocol, DatanodeProtocol, AtlasProtocol};
use storage::StorageStore;

use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, RwLock};

fn main() {
    // initialize logger
    env_logger::init();

    // parse arguments
    let config = Config::from_args();

    // initialize BlockStore
    let block_store = Arc::new(RwLock::new(BlockStore::new()));
    info!("initialized block store");

    // initialize DatanodeStore
    let datanode_store = Arc::new(RwLock::new(
        DatanodeStore::new(config.state_queue_length)));
    info!("initialized datanode store");

    // initialize FileStore
    // TODO - refactor to different function
    let path = Path::new(&config.persist_path);
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!("failed to create data directory: {}", e);
                return;
            }
        }
    }

    let file_store_result: Result<FileStore, _> = 
            match path.exists() {
        true => {
            let mut file = File::open(&config.persist_path).unwrap();
            let mut buf = Vec::new();
            if let Err(e) = file.read_to_end(&mut buf) {
                error!("failed to initialize file store: {}", e);
                return;
            }
            bincode::deserialize(&buf[..])
        },
        false => Ok(FileStore::new()),
    };

    let file_store = match file_store_result {
        Ok(file_store) => Arc::new(RwLock::new(file_store)),
        Err(e) => {
            error!("failed to initialize file store: {}", e);
            return;
        }
    };

    info!("initialized file store");

    // initialize Index
    let index = Arc::new(RwLock::new(Index::new()));
    info!("initialized index");
 
    // initialize StorageStore
    let storage_store = Arc::new(RwLock::new(
        StorageStore::new(config.state_queue_length)));
    info!("initialized storage store");
    
    // start TcpListener
    let address = format!("{}:{}", config.ip_address, config.port);
    let listener_result = TcpListener::bind(&address);
    if let Err(e) = listener_result {
        error!("failed to open tcp listener on '{}': {}", address, e);
        return;
    }

    let listener = listener_result.unwrap();

    // initialize Server
    let mut server = Server::new(listener,
        config.thread_count, config.socket_wait_ms);
    info!("initialized rpc server");

    // register protocols
    let mut protocols = Protocols::new();

    let client_namenode_protocol = ClientNamenodeProtocol::new(
        block_store.clone(), datanode_store.clone(),
        file_store.clone(), index.clone(), storage_store.clone());
    protocols.register("org.apache.hadoop.hdfs.protocol.ClientProtocol",
        Box::new(client_namenode_protocol));

    let datanode_protocol = DatanodeProtocol::new(block_store.clone(),
        datanode_store.clone(), storage_store.clone());
    protocols.register("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol",
        Box::new(datanode_protocol));

    let atlas_protocol = AtlasProtocol::new(file_store.clone(),
        index.clone(), &config.persist_path);
    protocols.register("com.bushpath.atlas.protocol.AtlasProtocol",
        Box::new(atlas_protocol));
 
    // start server
    if let Err(e) =
            server.start(Arc::new(RwLock::new(Box::new(protocols)))) {
        error!("failed to start rpc server: {}", e);
    }
    info!("started rpc server");

    // keep running indefinitely
    std::thread::park();
}

#[derive(Debug, StructOpt)]
struct Config {
    #[structopt(name="PERSIST_PATH")]
    persist_path: String,
    #[structopt(short="i", long="ip_address", default_value="127.0.0.1")]
    ip_address: String,
    #[structopt(short="p", long="port", default_value="9000")]
    port: u16,
    #[structopt(short="t", long="thread_count", default_value="8")]
    thread_count: u8,
    #[structopt(short="w", long="socket_wait_ms", default_value="50")]
    socket_wait_ms: u64,
    #[structopt(short="s", long="state_queue_length", default_value="10")]
    state_queue_length: usize,
}
