use radix_trie::{Trie, TrieCommon};
use shared::NahError;

pub struct Index {
    trie: Trie<String, Vec<(u64, u32)>>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            trie: Trie::new(),
        }
    }

    pub fn add_geohash(&mut self, geohash: &str,
            block_id: &u64, length: u32) -> Result<(), NahError> {
        if let Some(blocks) = self.trie.get_mut(geohash) {
            // check if block already exists
            for (value, _) in blocks.iter() {
                if block_id == value {
                    return Ok(());
                }
            }

            debug!("adding geohash index {} : ({}, {})", geohash, block_id, length);
            blocks.push((*block_id, length));
        } else {
            debug!("inserting new geohash index {} : ({}, {})", geohash, block_id, length);
            self.trie.insert(geohash.to_string(), vec!((*block_id, length)));
        }

        Ok(())
    }
}
