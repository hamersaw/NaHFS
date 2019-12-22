#[macro_use]
extern crate crossbeam_channel;
#[macro_use]
extern crate log;
extern crate structopt;

use communication::Server;
use structopt::StructOpt;

mod block;
mod index;
mod protocol;

use block::BlockProcessor;
use index::IndexStore;
use protocol::{NamenodeProtocol, TransferStreamHandler};

use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, RwLock};

fn main() {
    // initialize logger
    env_logger::init();

    // parse arguments
    let config = Config::from_args();

    // initialize IndexStore
    let index_store = Arc::new(RwLock::new(IndexStore::new(
        config.namenode_ip_address.clone(), config.namenode_port)));

    // initialize BlockProcessor
    let path = Path::new(&config.data_directory);
    if !path.exists() {
        if let Err(e) = std::fs::create_dir_all(path) {
            error!("failed to create data directory: {}", e);
            return;
        }
    }
 
    let mut processor = BlockProcessor::new(index_store.clone(),
        config.processor_thread_count, config.processor_queue_length, 
        config.data_directory.clone(),
        config.namenode_ip_address.clone(), config.namenode_port);
    info!("initialized block processor");

    // start BlockProcessor
    if let Err(e) = processor.start() {
        error!("failed to start block processor: {}", e);
        return;
    }

    // start transfer TcpListener
    let address = format!("{}:{}", config.ip_address, config.port);
    let listener_result = TcpListener::bind(&address);
    if let Err(e) = listener_result {
        error!("failed to open tcp listener on '{}': {}", address, e);
        return;
    }

    let listener = listener_result.unwrap();

    // initialize Server
    let mut server = Server::new(listener, config.socket_wait_ms);
    info!("initialized transfer server");

    // start server
    let handler = TransferStreamHandler::new(RwLock::new(processor));
    if let Err(e) =
            server.start(Arc::new(RwLock::new(Box::new(handler)))) {
        error!("failed to start transfer server: {}", e);
        return;
    }
    info!("started transfer server");

    // initialize NamenodeProtocol
    let mut namenode_protocol= NamenodeProtocol::new(config);
    info!("initialized namenode protocol");

    // start NamenodeProtocol
    let namenode_protocol_result = namenode_protocol.start();
    if let Err(e) = namenode_protocol_result {
        error!("failed to start namenode protocol: {}", e);
        return;
    }

    info!("started namenode protocol");

    // keep running indefinitely
    std::thread::park();
}

#[derive(Clone, Debug, StructOpt)]
pub struct Config {
    #[structopt(name="ID")]
    id: String,
    #[structopt(name="STORAGE_ID")]
    storage_id: String,
    #[structopt(name="DATA_DIR")]
    data_directory: String,
    #[structopt(short="i", long="ip_address", default_value="127.0.0.1")]
    ip_address: String,
    #[structopt(short="p", long="port", default_value="8020")]
    port: u32,
    #[structopt(short="w", long="socket_wait_ms", default_value="50")]
    socket_wait_ms: u64,
    #[structopt(short="a", long="namenode_ip_address", default_value="127.0.0.1")]
    namenode_ip_address: String,
    #[structopt(short="c", long="processor_thread_count", default_value="4")]
    namenode_port: u16,
    #[structopt(short="b", long="block_report", default_value="5000")]
    processor_thread_count: u8,
    #[structopt(short="q", long="processor_queue_length", default_value="32")]
    processor_queue_length: u8,
    #[structopt(short="o", long="namenode_port", default_value="9000")]
    block_report_ms: u64,
    #[structopt(short="h", long="heartbeat", default_value="2500")]
    heartbeat_ms: u64,
    #[structopt(short="r", long="index_report", default_value="10000")]
    index_report_ms: u64,
}
