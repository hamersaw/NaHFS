use glob::{GlobError, PatternError};
use prost::{EncodeError, DecodeError};
use radix::RadixError;

use std::fmt::{Display, Formatter};
use std::num::{ParseFloatError, ParseIntError};

pub mod block;
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/nahfs.rs"));
}

pub fn geohash_char_to_value(c: char) -> Result<u8, NahError> {
    // if token contains block id -> add geohash
    match c as u8 {
        x if x >= 48 && x <= 58 => Ok(x - 48),
        x if x >= 97 && x <= 102 => Ok(x - 87),
        _ => Err(NahError::from(
            format!("invalid geohash character '{}'", c))),
    }
}

impl From<NahError> for std::io::Error {
    fn from(err: NahError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
    }
}

#[derive(Debug)]
pub enum NahError {
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

impl Display for NahError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            NahError::DecodeError(ref err) => write!(f, "DecodeError: {:?}", err),
            NahError::EncodeError(ref err) => write!(f, "EncodeError: {:?}", err),
            NahError::GlobError(ref err) => write!(f, "GlobError: {:?}", err),
            NahError::IoError(ref err) => write!(f, "IoError: {:?}", err),
            NahError::Nah(ref err) => write!(f, "NahError: {}", err),
            NahError::ParseFloatError(ref err) => write!(f, "ParseFloatError: {}", err),
            NahError::ParseIntError(ref err) => write!(f, "ParseIntError: {}", err),
            NahError::PatternError(ref err) => write!(f, "PatternError: {}", err),
            NahError::RadixError(ref err) => write!(f, "RaddixError: {}", err),
        }
    }
}

impl From<DecodeError> for NahError {
    fn from(err: DecodeError) -> NahError {
        NahError::DecodeError(err)
    }
}

impl From<EncodeError> for NahError {
    fn from(err: EncodeError) -> NahError {
        NahError::EncodeError(err)
    }
}

impl From<GlobError> for NahError {
    fn from(err: GlobError) -> NahError {
        NahError::GlobError(err)
    }
}

impl From<std::io::Error> for NahError {
    fn from(err: std::io::Error) -> NahError {
        NahError::IoError(err)
    }
}

impl From<ParseFloatError> for NahError {
    fn from(err: ParseFloatError) -> NahError {
        NahError::ParseFloatError(err)
    }
}

impl From<ParseIntError> for NahError {
    fn from(err: ParseIntError) -> NahError {
        NahError::ParseIntError(err)
    }
}

impl From<PatternError> for NahError {
    fn from(err: PatternError) -> NahError {
        NahError::PatternError(err)
    }
}

impl From<RadixError> for NahError {
    fn from(err: RadixError) -> NahError {
        NahError::RadixError(err)
    }
}

impl<'a> From<&'a str> for NahError {
    fn from(err: &'a str) -> NahError {
        NahError::Nah(String::from(err))
    }
}

impl From<String> for NahError {
    fn from(err: String) -> NahError {
        NahError::Nah(err)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
