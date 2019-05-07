#[macro_use]
extern crate log;
extern crate structopt;

use communication::Server;
use hdfs_comm::rpc::Protocols;
use structopt::StructOpt;

mod datanode;
mod file;
mod protocol;

use datanode::DatanodeStore;
use file::FileStore;
use protocol::{ClientNamenodeProtocol, DatanodeProtocol};

use std::net::TcpListener;
use std::sync::{Arc, RwLock};

fn main() {
    // initialize logger
    env_logger::init();

    // parse arguments
    let config = Config::from_args();

    // initialize DatanodeStore
    let datanode_store = Arc::new(RwLock::new(DatanodeStore::new()));
    info!("initialized datanode store");

    // initialize FileStore
    let file_store = Arc::new(RwLock::new(FileStore::new()));
    info!("initialized file store");
    
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
    protocols.register("org.apache.hadoop.hdfs.protocol.ClientProtocol",
        Box::new(ClientNamenodeProtocol::new(file_store.clone())));
    protocols.register("org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol",
        Box::new(DatanodeProtocol::new(datanode_store.clone())));
 
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
