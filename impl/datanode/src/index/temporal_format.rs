use shared::NahFSError;

use crate::index::data_format::get_delimited_field;

pub enum TemporalFormat {
    None,
    Value {index: usize},
}

impl TemporalFormat {
    pub fn parse_vec(&self, data: &[u8], delimiters: &Vec<usize>)
            -> Result<Option<u64>, NahFSError> {
        match self {
            TemporalFormat::None => Ok(None),
            TemporalFormat::Value {index} => {
                // parse point fields
                let timestamp = get_delimited_field(*index,
                    data, delimiters)?.parse::<f64>()? as u64;

                Ok(Some(timestamp))
            },
        }
    }
}
