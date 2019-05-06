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
    let dr_proto = protocol::to_datanode_registration_proto(&config);
    let mut namenode_protocol_service = NamenodeProtocol::new(
        dr_proto, config.block_report_ms, config.heartbeat_ms,
        &config.namenode_ip_address, config.namenode_port);
    info!("initialized namenode_protocol service");

    // start NamenodeProtocol
    let namenode_protocol_result = namenode_protocol_service.start();
    if let Err(e) = namenode_protocol_result {
        error!("failed to start NamenodeProtocolService: {}", e);
        return;
    }

    info!("started namenode_protocol service");

    // keep running indefinitely
    std::thread::park();
}

#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(name="ID")]
    id: String,
    #[structopt(short="i", long="ip_address", default_value="127.0.0.1")]
    ip_address: String,
    #[structopt(short="p", long="port", default_value="8020")]
    port: u32,
    #[structopt(short="a", long="namenode_ip_address", default_value="127.0.0.1")]
    namenode_ip_address: String,
    #[structopt(short="o", long="namenode_port", default_value="9000")]
    namenode_port: u16,
    #[structopt(short="b", long="block_report", default_value="1000")]
    block_report_ms: u64,
    #[structopt(short="h", long="heartbeat", default_value="2000")]
    heartbeat_ms: u64,
}
