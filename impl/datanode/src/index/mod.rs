use hdfs_comm::rpc::Client;
use prost::Message;
use regex::Regex;
use shared::NahFSError;
use shared::protos::{BlockIndexProto, BlockMetadataProto, GetStoragePolicyResponseProto, GetStoragePolicyRequestProto, SpatialIndexProto, TemporalIndexProto};

mod data_format;
mod spatial_format;
mod temporal_format;

use data_format::DataFormat;
use spatial_format::SpatialFormat;
use temporal_format::TemporalFormat;

use std::collections::{BTreeMap, HashMap};
use std::io::{BufWriter, Write};
use std::time::SystemTime;

type SpatialIndex = BTreeMap<String, Vec<(usize, usize)>>;
type TemporalIndex = (u64, u64);

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
            -> Result<(), NahFSError> {
        if self.map.contains_key(id) {
            return Err(NahFSError::from(
                format!("index '{}' already exists", id)));
        }

        // initialize GetStoragePolicyRequestProto
        let mut req = GetStoragePolicyRequestProto::default();
        req.id = *id;

        debug!("writing GetStoragePolicyRequestProto to {}:{}",
            self.ip_address, self.port);

        // send GetStoragePolicyRequestProto
        let mut client = Client::new(&self.ip_address, self.port)?;
        let (_, resp_buf) = client.write_message("com.bushpath.nahfs.protocol.NahFSProtocol", "getStoragePolicy", req)?;

        // read response
        let resp = GetStoragePolicyResponseProto
            ::decode_length_delimited(resp_buf)?;

        // parse storage policy -> indexer
        debug!("parsing storage policy '{}'", &resp.storage_policy);
        let indexer = Indexer::from(&resp.storage_policy)?;
        self.map.insert(*id, indexer);

        Ok(())
    }
}

pub struct Indexer {
    data_format: DataFormat,
    spatial_format: SpatialFormat,
    temporal_format: TemporalFormat,
}

impl Indexer {
    pub fn from(string: &String) -> Result<Indexer, NahFSError> {
        // compile regexes
        let regex = Regex::new(r"(\w+)\((\w+:\w+)?(,\s*\w+:\w+)*\)")?;
        let fields_regex = Regex::new(r"(\w+):(\w+)")?;

        // check for match
        if !regex.is_match(string) {
            return Err(NahFSError::from(format!(
                "unable to parse storage policy {}", string)));
        }

        // parse string
        let caps = regex.captures(string).unwrap();

        let mut map = HashMap::new();
        for field in fields_regex.captures_iter(string) {
            map.insert(field[1].to_string(), field[2].to_string());
        }

        // initialize Indexer
        let (data_format, spatial_format, temporal_format) =
                match &caps[1] {
            "CsvPoint" => (
                DataFormat::Delimited {delimiter: ',' as u8},
                SpatialFormat::Point {
                    latitude_index: get_field("latitude_index", &map)?
                        .parse::<usize>()?,
                    longitude_index: get_field("longitude_index", &map)?
                        .parse::<usize>()?,
                },
                TemporalFormat::Value {
                    index: get_field("timestamp_index", &map)?
                        .parse::<usize>()?,
                },
            ),
            "Wkt" => (
                DataFormat::Delimited {delimiter: '\t' as u8},
                SpatialFormat::Wkt {
                    spatial_index: get_field("spatial_index", &map)?
                        .parse::<usize>()?,
                },
                TemporalFormat::None,
            ),
            _ => return Err(NahFSError::from(
                format!("unsupported indexer type {}", &caps[1]))),
        };

        Ok(
            Indexer {
                data_format: data_format,
                spatial_format: spatial_format,
                temporal_format: temporal_format,
            }
        )
    }

    pub fn process(&self, data: &Vec<u8>, bm_proto: &BlockMetadataProto)
            -> Result<(Vec<u8>, BlockIndexProto), NahFSError> {
        let now = SystemTime::now();
        let mut spatial_index = SpatialIndex::new();
        let mut temporal_index = (std::u64::MAX, std::u64::MIN);

        // use DataFormat to compute data geohashes and timestamps
        self.data_format.process(data,
            &self.spatial_format, &mut spatial_index,
            &self.temporal_format, &mut temporal_index)?;

        // initialize return values
        let mut indexed_data = Vec::new();
        let mut bi_proto = BlockIndexProto::default();

        // if SpatialIndex is not empty -> process
        if spatial_index.len() != 0 {
            // compute geohash length properties
            let (mut match_len, mut max_len);
            {
                // collect geohash vector
                let geohash_vec: Vec<&String> =
                    spatial_index.keys().collect();
                match_len = geohash_vec[0].len();
                max_len = geohash_vec[0].len();

                // compute minimum and maximum geohash length
                for geohash in geohash_vec.iter() {
                    max_len = std::cmp::max(geohash.len(), max_len);
                }

                // compute maximum geohash match length
                for i in 1..geohash_vec.len() {
                    let mut len = 0;
                    for j in 1..std::cmp::min(geohash_vec[i-1].len(),
                            geohash_vec[i].len()) {
                        if geohash_vec[i-1][..j]
                                != geohash_vec[i][..j] {
                            break;
                        }

                        len += 1;
                    }

                    match_len = std::cmp::min(len, match_len);
                }
            }

            // compute prefix map for geohashes
            // prefix_map -> Map<common_prefix, computed geohash>
            let prefix_len = std::cmp::min(match_len + 1, max_len);
            let mut prefix_map: BTreeMap<String, Vec<String>> =
                BTreeMap::new();
            for (geohash, _) in spatial_index.iter() {
                let prefix = geohash[..prefix_len].to_string();

                let values = prefix_map.entry(prefix)
                    .or_insert(Vec::new());
                values.push(geohash.to_string());
            }

            // copy indexed data
            let mut si_proto = SpatialIndexProto::default();
            let si_geohashes = &mut si_proto.geohashes;
            let si_start_indices = &mut si_proto.start_indices;
            let si_end_indices = &mut si_proto.end_indices;

            let mut buf_writer = BufWriter::new(&mut indexed_data);
            let mut current_index = 0;
            for (prefix, geohash_vec) in prefix_map.iter() {
                si_geohashes.push(prefix.to_string());
                si_start_indices.push(current_index as u32); 

                for geohash in geohash_vec {
                    for (start_index, end_index) in
                            spatial_index.get(geohash).unwrap() {

                        buf_writer.write_all(
                            &data[*start_index..*end_index])?;
                        current_index += end_index - start_index;
                    }
                }

                si_end_indices.push(current_index as u32);
            }

            bi_proto.spatial_index = Some(si_proto);
            buf_writer.flush()?;
        }

        // if TemporalIndex has updated -> process
        if temporal_index.0 != std::u64::MAX
                || temporal_index.1 != std::u64::MIN {
            // set index timestamps
            let mut ti_proto = TemporalIndexProto::default();
            ti_proto.start_timestamp = temporal_index.0;
            ti_proto.end_timestamp = temporal_index.1;

            bi_proto.temporal_index = Some(ti_proto);
        }

        let elapsed = now.elapsed().unwrap();
        debug!("indexed block {} in {}.{}s", bm_proto.block_id,
            elapsed.as_secs(), elapsed.subsec_millis());

        Ok((indexed_data, bi_proto))
    }
}

fn get_field<'a>(field: &str, map: &'a HashMap<String, String>)
        -> Result<&'a String, String> {
    map.get(field).ok_or(format!("field '{}' not found", field))
}
