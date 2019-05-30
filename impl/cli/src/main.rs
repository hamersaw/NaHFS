#[macro_use]
extern crate clap;

use clap::App;

mod inode;

fn main() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    // parse subcommands
    match matches.subcommand() {
        ("index", Some(index_matches)) => {
            match matches.subcommand() {
                ("view", Some(view_matches)) => {
                    // TODO - "index view"
                },
                (cmd, _) => println!("unknown subcommand '{}'", cmd),
            }
        },
        ("inode", Some(inode_matches)) =>
            inode::process(&matches, &inode_matches),
        (cmd, _) => println!("unknown subcommand '{}'", cmd),
    }
}
