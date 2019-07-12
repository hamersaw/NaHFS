#[macro_use]
extern crate clap;

use clap::App;

mod index;
mod inode;

fn main() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    // parse subcommands
    match matches.subcommand() {
        ("index", Some(index_matches)) =>
            index::process(&matches, &index_matches),
        ("inode", Some(inode_matches)) =>
            inode::process(&matches, &inode_matches),
        (cmd, _) => println!("unknown subcommand '{}'", cmd),
    }
}
