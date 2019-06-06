use glob::{GlobError, PatternError};
use prost::{EncodeError, DecodeError};
use radix::RadixError;

use std::fmt::{Display, Formatter};
use std::num::{ParseFloatError, ParseIntError};

pub mod block;
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/atlas.rs"));
}

pub fn geohash_char_to_value(c: char) -> Result<u8, AtlasError> {
    // if token contains block id -> add geohash
    match c as u8 {
        x if x >= 48 && x <= 58 => Ok(x - 48),
        x if x >= 97 && x <= 102 => Ok(x - 87),
        _ => Err(AtlasError::from(
            format!("invalid geohash character '{}'", c))),
    }
}

impl From<AtlasError> for std::io::Error {
    fn from(err: AtlasError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
    }
}

#[derive(Debug)]
pub enum AtlasError {
    DecodeError(DecodeError),
    EncodeError(EncodeError),
    GlobError(GlobError),
    IoError(std::io::Error),
    Nah(String),
    ParseFloatError(ParseFloatError),
    ParseIntError(ParseIntError),
    PatternError(PatternError),
    RadixError(RadixError),
}

impl Display for AtlasError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            AtlasError::DecodeError(ref err) => write!(f, "DecodeError: {:?}", err),
            AtlasError::EncodeError(ref err) => write!(f, "EncodeError: {:?}", err),
            AtlasError::GlobError(ref err) => write!(f, "GlobError: {:?}", err),
            AtlasError::IoError(ref err) => write!(f, "IoError: {:?}", err),
            AtlasError::Nah(ref err) => write!(f, "AtlasError: {}", err),
            AtlasError::ParseFloatError(ref err) => write!(f, "ParseFloatError: {}", err),
            AtlasError::ParseIntError(ref err) => write!(f, "ParseIntError: {}", err),
            AtlasError::PatternError(ref err) => write!(f, "PatternError: {}", err),
            AtlasError::RadixError(ref err) => write!(f, "RaddixError: {}", err),
        }
    }
}

impl From<DecodeError> for AtlasError {
    fn from(err: DecodeError) -> AtlasError {
        AtlasError::DecodeError(err)
    }
}

impl From<EncodeError> for AtlasError {
    fn from(err: EncodeError) -> AtlasError {
        AtlasError::EncodeError(err)
    }
}

impl From<GlobError> for AtlasError {
    fn from(err: GlobError) -> AtlasError {
        AtlasError::GlobError(err)
    }
}

impl From<std::io::Error> for AtlasError {
    fn from(err: std::io::Error) -> AtlasError {
        AtlasError::IoError(err)
    }
}

impl From<ParseFloatError> for AtlasError {
    fn from(err: ParseFloatError) -> AtlasError {
        AtlasError::ParseFloatError(err)
    }
}

impl From<ParseIntError> for AtlasError {
    fn from(err: ParseIntError) -> AtlasError {
        AtlasError::ParseIntError(err)
    }
}

impl From<PatternError> for AtlasError {
    fn from(err: PatternError) -> AtlasError {
        AtlasError::PatternError(err)
    }
}

impl From<RadixError> for AtlasError {
    fn from(err: RadixError) -> AtlasError {
        AtlasError::RadixError(err)
    }
}

impl<'a> From<&'a str> for AtlasError {
    fn from(err: &'a str) -> AtlasError {
        AtlasError::Nah(String::from(err))
    }
}

impl From<String> for AtlasError {
    fn from(err: String) -> AtlasError {
        AtlasError::Nah(err)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
