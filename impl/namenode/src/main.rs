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
use protocol::{ClientNamenodeProtocol, DatanodeProtocol, NahfsProtocol};
use storage::StorageStore;

use std::net::TcpListener;
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
    let datanode_store = Arc::new(RwLock::new(DatanodeStore::new()));
    info!("initialized datanode store");

    // initialize FileStore
    let file_store = Arc::new(RwLock::new(FileStore::new()));
    info!("initialized file store");

    // initialize Index
    let index = Arc::new(RwLock::new(Index::new()));
    info!("initialized index");
 
    // initialize StorageStore
    let storage_store = Arc::new(RwLock::new(StorageStore::new()));
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

    let datanode_protocol = DatanodeProtocol::new(
        block_store.clone(), datanode_store.clone(), storage_store.clone());
    protocols.register("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol",
        Box::new(datanode_protocol));

    let nahfs_protocol = NahfsProtocol::new(index.clone());
    protocols.register("com.bushpath.nahfs.protocol.NahfsProtocol",
        Box::new(nahfs_protocol));
 
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
    #[structopt(short="i", long="ip_address", default_value="127.0.0.1")]
    ip_address: String,
    #[structopt(short="p", long="port", default_value="9000")]
    port: u16,
    #[structopt(short="t", long="thread_count", default_value="4")]
    thread_count: u8,
    #[structopt(short="w", long="socket_wait_ms", default_value="50")]
    socket_wait_ms: u64,
}
