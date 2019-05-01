#[macro_use]
extern crate log;
use hdfs_comm::rpc::{Server, Protocol};

mod file;
mod protocol;

use file::FileStore;
use protocol::ClientProtocol;

use std;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};

fn main() {
    // initialize logger
    env_logger::init();

    // initialize FileStore
    let file_store = Arc::new(RwLock::new(FileStore::new()));
    info!("initialized file store");
    
    // initialize Server
    let listener = TcpListener::bind("127.0.0.1:9000").unwrap();
    let mut server = Server::new(listener, 4);
    info!("initialized rpc server");

    // register protocols
    server.register("org.apache.hadoop.hdfs.protocol.ClientProtocol",
        Box::new(ClientProtocol::new(file_store.clone())));
 
    // start server
    if let Err(e) = server.start() {
        error!("failed to start rpc server: {}", e);
    }
    info!("started rpc server");

    // keep running indefinitely
    std::thread::park();
}
