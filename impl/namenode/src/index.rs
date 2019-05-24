use radix::{self, RadixQuery, RadixTrie};
use shared::NahError;

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
                vec!((*block_id, length)));
        }

        Ok(())
    }
}

pub fn parse_query(query_string: &str) -> Result<RadixQuery, NahError> {
    Ok(radix::parse_query(query_string)?)
}
