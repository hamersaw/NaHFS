use clap::ArgMatches;
use shared::NahError;

use hdfs_comm::rpc::Client;
use prost::Message;
use shared::protos::{InodePersistResponseProto, InodePersistRequestProto};

pub fn process(matches: &ArgMatches, inode_matches: &ArgMatches) {
    let result = match inode_matches.subcommand() {
        ("persist", Some(persist_matches)) => {
            persist(&matches, &inode_matches, &persist_matches)
        },
        (cmd, _) => Err(NahError::from(format!("unknown subcommand '{}'", cmd))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

fn persist(matches: &ArgMatches, inode_matches: &ArgMatches,
        persist_matches: &ArgMatches) -> Result<(), NahError> {
    let ipr_proto = InodePersistRequestProto::default();

    // send InodePersistRequestProto
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = Client::new(ip_address, port)?;
    let (_, resp_buf) = client.write_message("com.bushpath.nahfs.protocol.NahfsProtocol", "inodePersist", ipr_proto)?;

    // read respnose
    let _ = InodePersistResponseProto
        ::decode_length_delimited(resp_buf)?;

    Ok(())
}
