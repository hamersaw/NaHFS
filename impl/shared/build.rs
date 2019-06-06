use prost_build::Config;

use std::path::PathBuf;

fn main() {
    // initialize config
    let mut config = Config::new();

    // create output directory 
    let output_directory = PathBuf::from(std::env::var("OUT_DIR")
        .expect("OUT_DIR environment variable not set"));
    std::fs::create_dir_all(&output_directory)
        .expect("failed to create prefix directory");
    config.out_dir(&output_directory);
  
    // compile protos
    config.compile_protos(&["atlas.proto"], &["protos/"]).unwrap();
}
