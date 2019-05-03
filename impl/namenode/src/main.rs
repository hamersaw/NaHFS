#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

use hdfs_comm::rpc::Server;
use shared::NahError;

mod file;
mod protocol;

use file::FileStore;
use protocol::ClientNamenodeProtocol;

use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};

fn main() {
    // initialize logger
    env_logger::init();

    // parse arguments
    let args: Vec<String>  = std::env::args().collect();
    if args.len() != 5 {
        println!("usage: {} <id> <ip_address> <port> <config-file>", args[0]);
        return;
    }

    let _id = &args[1];
    let ip_address = &args[2];
    let port = &args[3];
    let config_file = &args[4];

    // parse toml configuration file
    let mut contents = String::new();
    let parse_config_result =
        shared::parse_toml_file::<Config>(&config_file, &mut contents);

    if let Err(e) = parse_config_result {
        error!("failed to parse config file: {}", e);
        return;
    }

    let config = parse_config_result.unwrap();

    // initialize FileStore
    let file_store = Arc::new(RwLock::new(FileStore::new()));
    info!("initialized file store");
    
    // start TcpListener
    let address = format!("{}:{}", ip_address, port);
    let listener_result = TcpListener::bind(&address);
    if let Err(e) = listener_result {
        error!("failed to open tcp listener on '{}': {}", address, e);
        return;
    }

    let listener = listener_result.unwrap();

    // initialize Server
    let mut server = Server::new(listener,
        config.rpc.thread_count, config.rpc.socket_wait_ms);
    info!("initialized rpc server");

    // register protocols
    server.register("org.apache.hadoop.hdfs.protocol.ClientProtocol",
        Box::new(ClientNamenodeProtocol::new(file_store.clone())));
 
    // start server
    if let Err(e) = server.start() {
        error!("failed to start rpc server: {}", e);
    }
    info!("started rpc server");

    // keep running indefinitely
    std::thread::park();
}

#[derive(Debug, Deserialize)]
struct Config {
    rpc: RpcConfig,
}

#[derive(Debug, Deserialize)]
struct RpcConfig {
    thread_count: u8,
    socket_wait_ms: u64,
}
