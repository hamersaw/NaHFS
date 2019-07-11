use shared::AtlasError;

use crate::index::data_format::get_delimited_field;

pub enum SpatialFormat {
    Point {latitude_index: usize, longitude_index: usize},
    Wkt {spatial_index: usize},
}

impl SpatialFormat {
    pub fn parse_vec(&self, data: &[u8], delimiters: &Vec<usize>)
            -> Result<Option<String>, AtlasError> {
        match self {
            SpatialFormat::Point {latitude_index, longitude_index} => {
                // parse point fields
                let latitude = get_delimited_field(*latitude_index,
                    data, delimiters)?.parse::<f32>()?;
                let longitude = get_delimited_field(*longitude_index,
                    data, delimiters)?.parse::<f32>()?;

                Ok(Some(geohash::encode_16(latitude, longitude, 8)?))
            },
            SpatialFormat::Wkt {spatial_index} => {
                // parse wkt fields
                let _spatial_field = get_delimited_field(*spatial_index,
                    data, delimiters)?;

                // TODO - process SpatialFormat::Wkt
                unimplemented!();
            },
        }
    }
}
