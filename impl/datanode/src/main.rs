#[macro_use]
extern crate log;

use std;

fn main() {
    // TODO - send RegisterDataRequestProto
    println!("Hello, world!");

    // keep running indefinitely
    std::thread::park();
}
