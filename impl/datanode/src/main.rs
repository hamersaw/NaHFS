#[macro_use]
extern crate crossbeam_channel;
#[macro_use]
extern crate log;
extern crate structopt;

use structopt::StructOpt;

mod protocol;
use protocol::NamenodeProtocol;

fn main() {
    // initialize logger
    env_logger::init();

    // parse arguments
    let config = Config::from_args();

    // initialize NamenodeProtocol
    let mut namenode_protocol_service = NamenodeProtocol
        ::new(config.block_report_ms, config.heartbeat_ms);
    info!("initialized namenode_protocol service");

    // start NamenodeProtocol
    namenode_protocol_service.start();
    info!("started namenode_protocol service");

    // keep running indefinitely
    std::thread::park();
}

#[derive(Debug, StructOpt)]
struct Config {
    #[structopt(name="ID")]
    id: String,
    #[structopt(short="i", long="ip_address", default_value="127.0.0.1")]
    ip_address: String,
    #[structopt(short="p", long="port", default_value="8020")]
    port: u16,
    #[structopt(short="a", long="namenode_ip_address", default_value="127.0.0.1")]
    namenode_ip_address: String,
    #[structopt(short="o", long="namenode_port", default_value="9000")]
    namenode_port: u16,
    #[structopt(short="b", long="block_report", default_value="1000")]
    block_report_ms: u64,
    #[structopt(short="h", long="heartbeat", default_value="2000")]
    heartbeat_ms: u64,
}
