use std::fs::File;
use std::io::Read;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum NahError {
    IoError(std::io::Error),
    Nah(String),
}

impl Display for NahError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            NahError::IoError(ref err) => write!(f, "IoError: {:?}", err),
            NahError::Nah(ref err) => write!(f, "NahError: {}", err),
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
