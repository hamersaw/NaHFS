use radix::{self, RadixQuery, RadixTrie};
use shared::{self, AtlasError};

use std::collections::HashMap;

pub struct Index {
    trie: RadixTrie<Vec<(u64, u32)>>,
    timestamps: HashMap<u64, (u64, u64)>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            trie: RadixTrie::new(),
            timestamps: HashMap::new(),
        }
    }

    pub fn add_geohash(&mut self, geohash: &str,
            block_id: &u64, length: u32) -> Result<(), AtlasError> {
        let bytes = geohash.as_bytes();
        if let Some(blocks) = self.trie.get_mut(&bytes) {
            // check if block already exists
            for (value, _) in blocks.iter() {
                if block_id == value {
                    return Ok(());
                }
            }

            trace!("adding geohash index {} : ({}, {})",
                geohash, block_id, length);
            blocks.push((*block_id, length));
        } else {
            trace!("inserting new geohash index {} : ({}, {})",
                geohash, block_id, length);
            self.trie.insert(&bytes,
                vec!((*block_id, length)))?;
        }

        Ok(())
    }

    pub fn add_time_range(&mut self, start_timestamp: u64,
            end_timestamp: u64, block_id: &u64) -> Result<(), AtlasError> {
        // check if block already exists
        if !self.timestamps.contains_key(block_id) {
            trace!("inserting new timestamp index {} : ({}, {})",
                block_id, start_timestamp, end_timestamp);
            self.timestamps.insert(*block_id,
                (start_timestamp, end_timestamp));
        }

        Ok(())
    }

    pub fn query(&self, query: &RadixQuery, block_ids: &Vec<u64>)
            -> HashMap<u64, (Vec<u8>, Vec<u32>)> {
        // initialize geohash map
        let mut geohash_map = HashMap::new();
        for block_id in block_ids {
            geohash_map.insert(*block_id, (Vec::new(), Vec::new()));
        }

        // TODO - only process blocks which fall into timestamp range

        // evaluate query
        query.evaluate(&self.trie, 
            &mut geohash_map, &mut query_process);

        // return geohash map
        geohash_map
    }
}

fn query_process(key: &Vec<u8>, value: &Vec<(u64, u32)>,
        token: &mut HashMap<u64, (Vec<u8>, Vec<u32>)>) {
    for (block_id, length) in value {
        if let Some((geohashes, lengths)) = token.get_mut(&block_id) {
            // if token contains block id -> add geohash
            let c = key[key.len() - 1] as char;
            let geohash_key = match shared::geohash_char_to_value(c) {
                Ok(geohash_key) => geohash_key,
                Err(e) => {
                    warn!("failed to parse geohash: {}", e);
                    continue;
                },
            };

            geohashes.push(geohash_key);
            lengths.push(*length);
        }
    }
}

pub fn parse_query(query_string: &str) -> Result<RadixQuery, AtlasError> {
    Ok(radix::parse_query(query_string)?)
}
