use toml;

use std;
use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum NahError {
    IoError(std::io::Error),
    Nah(String),
    TomlError(toml::de::Error),
}

impl Display for NahError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match *self {
            NahError::IoError(ref err) => write!(f, "IoError: {:?}", err),
            NahError::Nah(ref err) => write!(f, "NahError: {}", err),
            NahError::TomlError(ref err) => write!(f, "TomlError: {:?}", err),
        }
    }
}

impl From<std::io::Error> for NahError {
    fn from(err: std::io::Error) -> NahError {
        NahError::IoError(err)
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

impl From<toml::de::Error> for NahError {
    fn from(err: toml::de::Error) -> NahError {
        NahError::TomlError(err)
    }
}
