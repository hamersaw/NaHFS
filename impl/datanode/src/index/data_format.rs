use shared::NahFSError;

use crate::index::{SpatialIndex, TemporalIndex};
use crate::index::spatial_format::SpatialFormat;
use crate::index::temporal_format::TemporalFormat;

pub enum DataFormat {
    Delimited {delimiter: u8},
}

impl DataFormat {
    pub fn process(&self, data: &Vec<u8>, spatial_format: &SpatialFormat,
            spatial_index: &mut SpatialIndex, temporal_format: &TemporalFormat,
            temporal_index: &mut TemporalIndex) -> Result<(), NahFSError> {
        // index data
        match self {
            DataFormat::Delimited {delimiter} => process_delimited(
                data, *delimiter, spatial_format, spatial_index,
                temporal_format, temporal_index),
        }

        Ok(())
    }
}

fn process_delimited(data: &Vec<u8>, delimiter: u8,
        spatial_format: &SpatialFormat, spatial_index: &mut SpatialIndex,
        temporal_format: &TemporalFormat, temporal_index: &mut TemporalIndex) {
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
                x if x == delimiter =>
                    delimiter_indices.push(end_index - start_index),
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
        match spatial_format.parse_vec(&data[start_index..end_index],
                &delimiter_indices) {
            Ok(Some(geohash)) => {
                if cont_geohash == geohash {
                    cont_end = end_index;
                } else if cont_end != cont_start {
                    // index observation
                    let indices = spatial_index.entry(cont_geohash)
                        .or_insert(Vec::new());
                    indices.push((cont_start, cont_end));
                    cont_start = cont_end;
                    cont_geohash = geohash;
                } else {
                    cont_start = start_index;
                    cont_end = end_index;
                    cont_geohash = geohash;
                }
            },
            Err(e) => warn!("parse geohash: {}", e),
            _ => {},
        }

        match temporal_format.parse_vec(&data[start_index..end_index],
                &delimiter_indices) {
            Ok(Some(timestamp)) => {
                // process timestamps
                if timestamp < temporal_index.0 {
                    temporal_index.0 = timestamp;
                }

                if timestamp > temporal_index.1 {
                    temporal_index.1 = timestamp;
                }
            },
            Err(e) => warn!("parse timestamp: {}", e),
            _ => {},
        }
    }
 
    // process final continuous interval
    let indices = spatial_index.entry(cont_geohash)
        .or_insert(Vec::new());
    indices.push((cont_start, cont_end));
}

pub fn get_delimited_field<'a>(index: usize,
        data: &'a [u8], delimiters: &Vec<usize>)
        -> Result<std::borrow::Cow<'a, str>, NahFSError> {
    let (start_index, end_index) = match index {
        0 => (0, delimiters[0]),
        x if x > delimiters.len() => return Err(NahFSError::from(
            format!("field index '{}' out of bounds for \
            '{}' length string with '{}' delimiters", 
            index, data.len(), delimiters.len()))),
        x if x == delimiters.len() => (delimiters[x-1] + 1, data.len()-2),
        x => (delimiters[x-1] + 1, delimiters[x]),
    };

    Ok(String::from_utf8_lossy(&data[start_index..end_index]))
}
