use hdfs_comm::rpc::Client;
use prost::Message;
use regex::Regex;
use shared::AtlasError;
use shared::protos::{BlockIndexProto, BlockMetadataProto, GetStoragePolicyResponseProto, GetStoragePolicyRequestProto};

mod csv;

use std::collections::HashMap;

pub struct IndexStore {
    ip_address: String,
    port: u16,
    map: HashMap<u32, Indexer>,
}

impl IndexStore {
    pub fn new(ip_address: String, port: u16) -> IndexStore {
        IndexStore {
            ip_address: ip_address,
            port: port,
            map: HashMap::new(),
        }
    }

    pub fn contains_index(&self, id: &u32) -> bool {
        self.map.contains_key(id)
    }

    pub fn get_index(&self, id: &u32) -> Option<&Indexer> {
        self.map.get(id)
    }

    pub fn retrieve_index(&mut self, id: &u32)
            -> Result<(), AtlasError> {
        if self.map.contains_key(id) {
            return Err(AtlasError::from(
                format!("index '{}' already exists", id)));
        }

        // initialize GetStoragePolicyRequestProto
        let mut req = GetStoragePolicyRequestProto::default();
        req.id = *id;

        debug!("writing GetStoragePolicyRequestProto to {}:{}",
            self.ip_address, self.port);

        // send GetStoragePolicyRequestProto
        let mut client = Client::new(&self.ip_address, self.port)?;
        let (_, resp_buf) = client.write_message("com.bushpath.atlas.protocol.AtlasProtocol", "getStoragePolicy", req)?;

        // read response
        let resp = GetStoragePolicyResponseProto
            ::decode_length_delimited(resp_buf)?;

        // parse storage policy -> indexer
        debug!("parsing storage policy '{}'", &resp.storage_policy);
        let indexer = Indexer::parse(&resp.storage_policy)?;
        self.map.insert(*id, indexer);

        Ok(())
    }
}

pub enum IndexType {
    CsvPoint {timestamp_index: usize,
        latitude_index: usize, longitude_index: usize},
}

pub struct Indexer {
    index_type: IndexType,
}

impl Indexer {
    pub fn parse(string: &str) -> Result<Indexer, AtlasError> {
        // compile regexes
        let regex = Regex::new(r"(\w+)\((\w+:\w+)?(,\s*\w+:\w+)*\)")?;
        let fields_regex = Regex::new(r"(\w+):(\w+)")?;

        // check for match
        if !regex.is_match(string) {
            return Err(AtlasError::from(format!(
                "unable to parse storage policy {}", string)));
        }

        // parse string
        let caps = regex.captures(string).unwrap();

        let mut map = HashMap::new();
        for field in fields_regex.captures_iter(string) {
            map.insert(field[1].to_string(), field[2].to_string());
        }

        // initialize Indexer
        let index_type = match &caps[1] {
            "CsvPoint" => {
                IndexType::CsvPoint {
                    timestamp_index: get_field("timestamp_index", &map)?
                        .parse::<usize>()?,
                    latitude_index: get_field("latitude_index", &map)?
                        .parse::<usize>()?,
                    longitude_index: get_field("longitude_index", &map)?
                        .parse::<usize>()?,
                }
            },
            _ => return Err(AtlasError::from(
                format!("unsupported indexer type {}", &caps[1]))),
        };

        Ok(
            Indexer {
                index_type: index_type,
            }
        )
    }

    pub fn process(&self, data: &Vec<u8>, bm_proto: &BlockMetadataProto)
            -> Result<(Vec<u8>, BlockIndexProto), AtlasError> {
        match self.index_type {
            IndexType::CsvPoint {timestamp_index,
                    latitude_index, longitude_index} => {
                csv::process_point(data, bm_proto, timestamp_index,
                    latitude_index, longitude_index)
            },
        }
    }
}

fn get_field<'a>(field: &str, map: &'a HashMap<String, String>)
        -> Result<&'a String, String> {
    map.get(field).ok_or(format!("field '{}' not found", field))
}
