#[macro_use]
extern crate crossbeam_channel;
#[macro_use]
extern crate serde;

#[macro_use]
extern crate log;

mod protocol;
use protocol::NamenodeProtocol;

fn main() {
    // initialize logger
    env_logger::init();

    // parse arguments
    let args: Vec<String>  = std::env::args().collect();
    if args.len() != 7 {
        println!("usage: {} <id> <ip_address> <port> <namenode_ip_address> <namenode_port> <config-file>", args[0]);
        return;
    }

    let _id = &args[1];
    let ip_address = &args[2];
    let port = &args[3];
    let namenode_ip_address = &args[4];
    let namenode_port = &args[5];
    let config_file = &args[6];

    // parse toml configuration file
    let mut contents = String::new();
    let parse_config_result =
        shared::parse_toml_file::<Config>(&config_file, &mut contents);

    if let Err(e) = parse_config_result {
        error!("failed to parse config file: {}", e);
        return;
    }

    let config = parse_config_result.unwrap();

    // initialize NamenodeProtocol
    let mut namenode_protocol_service = NamenodeProtocol
        ::new(config.rpc.block_report_ms, config.rpc.heartbeat_ms);
    info!("initialized namenode_protocol service");

    // start NamenodeProtocol
    namenode_protocol_service.start();
    info!("started namenode_protocol service");

    // keep running indefinitely
    std::thread::park();
}

#[derive(Debug, Deserialize)]
struct Config {
    rpc: RpcConfig,
}

#[derive(Debug, Deserialize)]
struct RpcConfig {
    block_report_ms: u64,
    heartbeat_ms: u64,
}
