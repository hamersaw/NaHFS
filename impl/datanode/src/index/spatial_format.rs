use geocode::Geocode;

use shared::NahFSError;

use crate::index::data_format::get_delimited_field;

pub enum SpatialFormat {
    Point {latitude_index: usize, longitude_index: usize},
    Wkt {spatial_index: usize},
}

impl SpatialFormat {
    pub fn parse_vec(&self, data: &[u8], delimiters: &Vec<usize>)
            -> Result<Option<String>, NahFSError> {
        match self {
            SpatialFormat::Point {latitude_index, longitude_index} => {
                // parse point fields
                let latitude = get_delimited_field(*latitude_index,
                    data, delimiters)?.parse::<f32>()?;
                let longitude = get_delimited_field(*longitude_index,
                    data, delimiters)?.parse::<f32>()?;

                Ok(Some(Geocode::Geohash16
                    .encode(latitude as f64, longitude as f64, 8)?))
            },
            SpatialFormat::Wkt {spatial_index} => {
                // retrieve wkt spatial fields
                let spatial_field = get_delimited_field(*spatial_index,
                    data, delimiters)?;

                let (mut min_lat, mut max_lat, mut min_long, mut max_long) =
                    (90f32, -90f32, 180f32, -180f32);

                let (mut latitude, mut longitude) = 
                    (String::new(), String::new());
                let mut pack_longitude = true;
                for c in spatial_field.chars() {
                    if longitude.len() == 0 {
                        match c {
                            x if (x >= '0' && x <= '9')
                                    || x == '.' || x == '-' => 
                                longitude.push(c),
                            _ => {},
                        }
                    } else if pack_longitude {
                        match c {
                            ' ' => pack_longitude = false,
                            _ => longitude.push(c),
                        }
                    } else {
                        match c {
                            x if (x >= '0' && x <= '9')
                                    || x == '.' || x == '-' =>
                                latitude.push(c),
                            _ => {
                                // process latitude and longitude
                                let lat = latitude.parse::<f32>()?;
                                let long = longitude.parse::<f32>()?;

                                if lat < min_lat {min_lat = lat;}
                                if lat > max_lat {max_lat = lat;}
                                if long < min_long {min_long = long;}
                                if long > max_long {max_long = long;}

                                // clear latitude and longitude
                                pack_longitude = true;
                                latitude.clear();
                                longitude.clear();
                            }
                        }
                    }
                }

                // calculate match length for boundary geohashes
                let a = Geocode::Geohash16.encode(
                    min_lat as f64, min_long as f64, 8)?;
                let b = Geocode::Geohash16.encode(
                    max_lat as f64, min_long as f64, 8)?;
                let c = Geocode::Geohash16.encode(
                    min_lat as f64, max_long as f64, 8)?;
                let d = Geocode::Geohash16.encode(
                    max_lat as f64, max_long as f64, 8)?;

                let mut match_length = 0;
                for i in 1..a.len() {
                    let substr = &a[i-1..i];
                    if !((substr == &b[i-1..i])
                         && (substr == &c[i-1..i])
                         && (substr == &d[i-1..i])) {
                        break;
                    }

                    match_length += 1;
                }

                // return geohash
                match match_length {
                    0 => Err(NahFSError::from("unable to geohash bound wkt object")),
                    _ => Ok(Some(a[..match_length].to_string())),
                }
            },
        }
    }
}
