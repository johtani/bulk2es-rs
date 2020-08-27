#[macro_use]
extern crate clap;

use clap::{App, AppSettings, Arg};
use log::{error, info};
use std::env;
use bulk2es_rs::loader::{load};

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    let app = App::new(crate_name!())
        .setting(AppSettings::DeriveDisplayOrder)
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .help_message("Prints help information.")
        .version_message("Prints version information.")
        .version_short("v")
        .arg(
            Arg::with_name("INPUT_DIR")
                .help("The directory where NDJSON files. Support only *.json files.")
                .value_name("INPUT_DIR")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("CONFIG")
                .help("The config yaml file for elasticsearch.")
                .value_name("CONFIG")
                .short("c")
                .long("config")
                .required(true)
                .takes_value(true),
        );
    let matches = app.get_matches();
    let config_file = matches.value_of("CONFIG").unwrap();
    let input_dir = matches.value_of("INPUT_DIR").unwrap();

    match load(input_dir, config_file) {
        Ok(()) => {
            info!("{}", "done");
        }
        Err(msg) => error!("{}", msg),
    }
}
