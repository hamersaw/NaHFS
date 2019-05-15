use std::fmt::{Display, Formatter};
use std::num::{ParseFloatError, ParseIntError};

pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/nahfs.rs"));
}

#[derive(Debug)]
pub enum NahError {
    IoError(std::io::Error),
    Nah(String),
    ParseFloatError(ParseFloatError),
    ParseIntError(ParseIntError),
}

impl Display for NahError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            NahError::IoError(ref err) => write!(f, "IoError: {:?}", err),
            NahError::Nah(ref err) => write!(f, "NahError: {}", err),
            NahError::ParseFloatError(ref err) => write!(f, "ParseFloatError: {}", err),
            NahError::ParseIntError(ref err) => write!(f, "ParseIntError: {}", err),
        }
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
