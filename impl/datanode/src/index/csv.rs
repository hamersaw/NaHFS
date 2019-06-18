use shared::AtlasError;
use shared::protos::{BlockIndexProto, BlockMetadataProto};

use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::time::SystemTime;

pub fn process_point(data: &Vec<u8>, bm_proto: &BlockMetadataProto,
        timestamp_index: usize, latitude_index: usize,
        longitude_index: usize) 
        -> Result<(Vec<u8>, BlockIndexProto), AtlasError> {
    let now = SystemTime::now();
    let mut geohashes: BTreeMap<String, Vec<(usize, usize)>> =
        BTreeMap::new();
    let (mut min_timestamp, mut max_timestamp) =
        (std::u64::MAX, std::u64::MIN);

    let mut start_index;
    let mut end_index = 0;
    let mut delimiter_indices = Vec::new();
    let mut feature_count = 0;

    let (mut cont_geohash, mut cont_start, mut cont_end)
        = (String::from(""), 0, 0);

    while end_index < data.len() - 1 {
        // initialize iteration variables
        start_index = end_index;
        delimiter_indices.clear();

        // compute observation boundaries
        while end_index < data.len() - 1 {
            end_index += 1;
            match data[end_index] {
                44 => delimiter_indices.push(end_index - start_index),
                10 => break, // NEWLINE
                _ => (),
            }
        }

        if data[end_index] == 10 {
            end_index += 1; // if currently on NEWLINE -> increment
        }

        // check if this is a valid observation
        if feature_count == 0 && start_index == 0 {
            continue; // first observation
        } else if feature_count == 0 {
            feature_count = delimiter_indices.len() + 1;
        } else if delimiter_indices.len() + 1 != feature_count {
            continue; // observations differing in feature counts
        }

        // process observation
        match parse_metadata(&data[start_index..end_index],
                &delimiter_indices, timestamp_index,
                latitude_index, longitude_index) {
            Ok((geohash, timestamp)) => {
                if cont_geohash == geohash {
                    cont_end = end_index;
                } else if cont_end != cont_start {
                    // index observation
                    let indices = geohashes.entry(cont_geohash)
                        .or_insert(Vec::new());
                    indices.push((cont_start, cont_end));
                    cont_start = cont_end;
                    cont_geohash = geohash;
                } else {
                    cont_start = start_index;
                    cont_end = end_index;
                    cont_geohash = geohash;
                }

                // process timestamps
                if timestamp < min_timestamp {
                    min_timestamp = timestamp;
                }

                if timestamp > max_timestamp {
                    max_timestamp = timestamp;
                }
            },
            Err(e) => warn!("parse observation metadata: {}", e),
        }
    }
 
    // process final continuous interval
    let indices = geohashes.entry(cont_geohash)
        .or_insert(Vec::new());
    indices.push((cont_start, cont_end));
 
    // copy indexed data
    let mut indexed_data = Vec::new();
    let mut bi_proto = BlockIndexProto::default();
    {
        let bi_geohashes = &mut bi_proto.geohashes;
        let bi_start_indices = &mut bi_proto.start_indices;
        let bi_end_indices = &mut bi_proto.end_indices;

        let mut buf_writer = BufWriter::new(&mut indexed_data);
        let mut current_index = 0;
        for (geohash, indices) in geohashes.iter() {
            bi_geohashes.push(geohash.to_string());
            bi_start_indices.push(current_index as u32); 

            for (start_index, end_index) in indices {
                buf_writer.write_all(
                    &data[*start_index..*end_index])?;
                current_index += end_index - start_index;
            }

            bi_end_indices.push(current_index as u32);
        }
        buf_writer.flush()?;
    }

    // set index timestamps
    bi_proto.start_timestamp = min_timestamp;
    bi_proto.end_timestamp = max_timestamp;

    let elapsed = now.elapsed().unwrap();
    debug!("indexed block {} in {}.{}s", bm_proto.block_id,
        elapsed.as_secs(), elapsed.subsec_millis());

    Ok((indexed_data, bi_proto))
}

fn parse_metadata(data: &[u8], delimiters: &Vec<usize>,
        timestamp_index: usize, latitude_index: usize,
        longitude_index: usize) -> Result<(String, u64), AtlasError> {
    let timestamp = get_field(timestamp_index,
        data, delimiters)?.parse::<f64>()? as u64;
    let latitude = get_field(latitude_index,
        data, delimiters)?.parse::<f32>()?;
    let longitude = get_field(longitude_index,
        data, delimiters)?.parse::<f32>()?;

    let geohash = geohash::encode_16(latitude, longitude, 4)?;
    Ok((geohash, timestamp))
}

fn get_field<'a>(index: usize, data: &'a [u8], delimiters: &Vec<usize>)
        -> Result<std::borrow::Cow<'a, str>, AtlasError> {
    let (start_index, end_index) = match index {
        0 => (0, delimiters[0]),
        x if x > delimiters.len() => return Err(AtlasError::from(
            format!("field index '{}' out of bounds for \
            '{}' length string with '{}' delimiters", 
            index, data.len(), delimiters.len()))),
        x if x == delimiters.len() => (delimiters[x] + 1, data.len()),
        x => (delimiters[x-1] + 1, delimiters[x]),
    };

    Ok(String::from_utf8_lossy(&data[start_index..end_index]))
}
