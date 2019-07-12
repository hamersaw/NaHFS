use clap::ArgMatches;
use shared::AtlasError;

use hdfs_comm::rpc::Client;
use prost::Message;
use shared::protos::{IndexViewResponseProto, IndexViewRequestProto};

pub fn process(matches: &ArgMatches, inode_matches: &ArgMatches) {
    let result = match inode_matches.subcommand() {
        ("view", Some(vew_matches)) => {
            view(&matches, &inode_matches, &vew_matches)
        },
        (cmd, _) => Err(AtlasError::from(format!("unknown subcommand '{}'", cmd))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

fn view(matches: &ArgMatches, _inode_matches: &ArgMatches,
        _view_matches: &ArgMatches) -> Result<(), AtlasError> {
    let req_proto = IndexViewRequestProto::default();

    // send InodePersistRequestProto
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = Client::new(ip_address, port)?;
    let (_, resp_buf) = client.write_message("com.bushpath.atlas.protocol.AtlasProtocol", "indexView", req_proto)?;

    // read respnose
    let resp_proto = IndexViewResponseProto
        ::decode_length_delimited(resp_buf)?;

    // print index information
    for (block_id, bi_proto) in resp_proto.blocks.iter() {
        println!("BLOCK: {}", block_id);

        // process spatial index
        if let Some(si_proto) = &bi_proto.spatial_index {
            println!("\tSPATIAL_INDEX:");
            for i in 0..si_proto.geohashes.len() {
                println!("\t\t{} -> {}", si_proto.geohashes[i],
                    si_proto.end_indices[i]);
            }
        }

        // process temporal index
        if let Some(ti_proto) = &bi_proto.temporal_index {
            println!("\tTEMPORAL_INDEX: ({}, {})",
                ti_proto.start_timestamp, ti_proto.end_timestamp);
        }
    }

    Ok(())
}
