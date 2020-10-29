use glob::{GlobError, PatternError};
use prost::{EncodeError, DecodeError};
use radix::RadixError;

use std::fmt::{Display, Formatter};
use std::error::Error;
use std::num::{ParseFloatError, ParseIntError};

pub mod block;
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/nahfs.rs"));
}

pub fn geohash_char_to_value(c: char) -> Result<u8, NahFSError> {
    // if token contains block id -> add geohash
    match c as u8 {
        x if x >= 48 && x <= 58 => Ok(x - 48),
        x if x >= 97 && x <= 102 => Ok(x - 87),
        _ => Err(NahFSError::from(
            format!("invalid geohash character '{}'", c))),
    }
}

impl From<NahFSError> for std::io::Error {
    fn from(err: NahFSError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, err.to_string())
    }
}

#[derive(Debug)]
pub enum NahFSError {
    BincodeError(Box<bincode::ErrorKind>),
    BoxError(Box<dyn Error>),
    DecodeError(DecodeError),
    EncodeError(EncodeError),
    GlobError(GlobError),
    IoError(std::io::Error),
    Nah(String),
    ParseFloatError(ParseFloatError),
    ParseIntError(ParseIntError),
    PatternError(PatternError),
    RadixError(RadixError),
    RegexError(regex::Error),
}

impl Display for NahFSError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            NahFSError::BincodeError(ref err) => write!(f, "BincodeError: {:?}", err),
            NahFSError::BoxError(ref err) => write!(f, "BoxError: {:?}", err),
            NahFSError::DecodeError(ref err) => write!(f, "DecodeError: {:?}", err),
            NahFSError::EncodeError(ref err) => write!(f, "EncodeError: {:?}", err),
            NahFSError::GlobError(ref err) => write!(f, "GlobError: {:?}", err),
            NahFSError::IoError(ref err) => write!(f, "IoError: {:?}", err),
            NahFSError::Nah(ref err) => write!(f, "NahFSError: {}", err),
            NahFSError::ParseFloatError(ref err) => write!(f, "ParseFloatError: {}", err),
            NahFSError::ParseIntError(ref err) => write!(f, "ParseIntError: {}", err),
            NahFSError::PatternError(ref err) => write!(f, "PatternError: {}", err),
            NahFSError::RadixError(ref err) => write!(f, "RaddixError: {}", err),
            NahFSError::RegexError(ref err) => write!(f, "RegexError: {}", err),
        }
    }
}

impl From<Box<bincode::ErrorKind>> for NahFSError {
    fn from(err: Box<bincode::ErrorKind>) -> NahFSError {
        NahFSError::BincodeError(err)
    }
}

impl From<Box<dyn Error>> for NahFSError {
    fn from(err: Box<dyn Error>) -> NahFSError {
        NahFSError::BoxError(err)
    }
}

impl From<DecodeError> for NahFSError {
    fn from(err: DecodeError) -> NahFSError {
        NahFSError::DecodeError(err)
    }
}

impl From<EncodeError> for NahFSError {
    fn from(err: EncodeError) -> NahFSError {
        NahFSError::EncodeError(err)
    }
}

impl From<GlobError> for NahFSError {
    fn from(err: GlobError) -> NahFSError {
        NahFSError::GlobError(err)
    }
}

impl From<std::io::Error> for NahFSError {
    fn from(err: std::io::Error) -> NahFSError {
        NahFSError::IoError(err)
    }
}

impl From<ParseFloatError> for NahFSError {
    fn from(err: ParseFloatError) -> NahFSError {
        NahFSError::ParseFloatError(err)
    }
}

impl From<ParseIntError> for NahFSError {
    fn from(err: ParseIntError) -> NahFSError {
        NahFSError::ParseIntError(err)
    }
}

impl From<PatternError> for NahFSError {
    fn from(err: PatternError) -> NahFSError {
        NahFSError::PatternError(err)
    }
}

impl From<RadixError> for NahFSError {
    fn from(err: RadixError) -> NahFSError {
        NahFSError::RadixError(err)
    }
}

impl From<regex::Error> for NahFSError {
    fn from(err: regex::Error) -> NahFSError {
        NahFSError::RegexError(err)
    }
}

impl<'a> From<&'a str> for NahFSError {
    fn from(err: &'a str) -> NahFSError {
        NahFSError::Nah(String::from(err))
    }
}

impl From<String> for NahFSError {
    fn from(err: String) -> NahFSError {
        NahFSError::Nah(err)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
