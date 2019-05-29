use radix::{self, RadixQuery, RadixTrie};
use shared::NahError;

use std::collections::HashMap;

pub struct Index {
    trie: RadixTrie<Vec<(u64, u32)>>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            trie: RadixTrie::new(),
        }
    }

    pub fn add_geohash(&mut self, geohash: &str,
            block_id: &u64, length: u32) -> Result<(), NahError> {
        let bytes = geohash.as_bytes();
        if let Some(blocks) = self.trie.get_mut(&bytes) {
            // check if block already exists
            for (value, _) in blocks.iter() {
                if block_id == value {
                    return Ok(());
                }
            }

            debug!("adding geohash index {} : ({}, {})",
                geohash, block_id, length);
            blocks.push((*block_id, length));
        } else {
            debug!("inserting new geohash index {} : ({}, {})",
                geohash, block_id, length);
            //self.trie.insert(geohash,
            self.trie.insert(&bytes,
                vec!((*block_id, length)))?;
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
            let geohash_index = match key[key.len() - 1] {
                x if x >= 48 && x <= 58 => x - 48,
                x if x >= 97 && x <= 102 => x - 87,
                _ => {
                    warn!("invalid geohash character in {:?}", &key);
                    continue;
                },
            };

            geohashes.push(geohash_index);
            lengths.push(*length);
        }
    }
}

//pub fn parse_query(query_string: &str) -> Result<RadixQuery, NahError> {
pub fn parse_query(query_string: &str) -> Result<RadixQuery, NahError> {
    Ok(radix::parse_query(query_string)?)
}
