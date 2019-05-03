use toml;
use serde::de::Deserialize;

use std::fs::File;
use std::io::Read;
use std::fmt::{Display, Formatter};

pub fn parse_toml_file<'a, T>(path: &'a str, contents: &'a mut String)
        -> Result<T, NahError> where T: Deserialize<'a> {
    let mut file = File::open(path)?;
    file.read_to_string(contents)?;

    match toml::from_str(contents) {
        Ok(config) => Ok(config),
        Err(e) => Err(NahError::from(e)),
    }
}

#[derive(Debug)]
pub enum NahError {
    IoError(std::io::Error),
    Nah(String),
    TomlError(toml::de::Error),
}

impl Display for NahError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
