pub fn encode_block_id(block_id: &u64, geohashes: &Vec<u8>) -> u64 {
    let mut id = block_id >> 36;

    // shift over
    let shift_factor = match geohashes.len() {
        0 => 0,
        x if x < 8 => 8 - (x % 8),
        x if x <= 15=> x % 8,
        _ => 8,
    };

    for _ in 0..shift_factor {
        id <<= 4;
    }

    // process geohashes
    match geohashes.len() {
        x if x <= 8 => {
            // include hashes
            for geohash in geohashes {
                id <<= 4;
                id |= *geohash as u64;
            }
        },
        x if x <= 15 => {
            // exclude hashes
            let mut exclude = vec![true; 16];
            for value in geohashes {
                exclude[*value as usize] = false;
            }

            for i in 0..exclude.len() {
                if exclude[i] {
                    id <<= 4;
                    id |= i as u64;
                }
            }
        },
        _ => {}, // if geohashes length is 16 do nothing
    }

    // process encode id;
    id <<= 4;
    id |= (geohashes.len() % 16) as u64;
    
    id
}

pub fn decode_block_id(block_id: &u64) -> (u64, Vec<u8>) { 
    let mut id = *block_id;

    // parse encode id
    let encode_id = id & 15u64;
    id >>= 4;
 
    // process geohashes
    let mut geohashes = Vec::new();
    match encode_id {
        0 => {
            for i in 0..16 {
                geohashes.push(i);
            }
        },
        x if x <= 8 => {
            for _ in 0..x {
                let geohash = (id & 15u64) as u8;
                geohashes.push(geohash);
                id >>= 4;
            }
        },
        x if x <= 15 => {
            let mut exclude = vec![false; 16];
            for _ in x..16 {
                let value = (id & 15u64) as usize;
                exclude[value] = true;
                id >>= 4;
            }

            for i in 0..exclude.len() {
                if !exclude[i] {
                    geohashes.push(i as u8);
                }
            }
        },
        _ => unreachable!(),
    }

    // shift remaining over
    let shift_factor = match geohashes.len() {
        0 => 0,
        x if x < 8 => 8 - (x % 8),
        x if x <= 15=> x % 8,
        _ => 8,
    };

    for _ in 0..shift_factor {
        id >>= 4;
    }

    id <<= 36;
    (id, geohashes)
}

#[cfg(test)]
mod tests {
    #[test]
    fn encode_cycle() {
        use rand;

        let block_id = (rand::random::<u32>() as u64) << 36;
        let mut geohashes = Vec::new();
        for i in 0..16 {
            geohashes.push(i);

            // encode block id
            let encoded_id = 
                super::encode_block_id(&block_id, &geohashes);

            // decode encoded block id
            let (decoded_id, decoded_geohashes) =
                super::decode_block_id(&encoded_id);

            // validate block ids and geohash vectors
            assert_eq!(decoded_id, block_id);
            assert_eq!(geohashes.len(), decoded_geohashes.len());

            for geohash in geohashes.iter() {
                let mut found = false;
                for decoded_geohash in decoded_geohashes.iter() {
                    if geohash == decoded_geohash {
                        found = true;
                    }
                }

                if !found {
                    panic!("{} not found in decoded values", geohash);
                }
            }
        }
    }
}
