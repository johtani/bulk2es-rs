use glob::glob;
use log::{info, warn};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use crate::output::ElasticsearchOutput;

fn load_file(filepath: &str, config_file: &str) -> Result<(), String> {
    let mut search_engine = ElasticsearchOutput::new(config_file);
    info!("Reading {}", filepath);
    for line_result in BufReader::new(File::open(filepath).unwrap()).lines() {
        match line_result {
            Ok(line) => search_engine.add_document(line),
            Err(error) => warn!("Can not read line. {:?}", error),
        }
    }
    search_engine.close();
    info!("Finish: {}", filepath);
    Ok(())
}

fn initialize_es(config_file: &str) {
    let initializer = ElasticsearchOutput::new(config_file);
    initializer.initialize();
}

pub fn load(
    input_dir: &str,
    config_file: &str,
) -> Result<(), String> {
    initialize_es(config_file);
    // TODO should we care other files?
    let path = Path::new(input_dir).join(Path::new("**/*.json"));
    // read files from input_dir
    let files: Vec<_> = glob(path.to_str().unwrap())
        .unwrap()
        .filter_map(|x| x.ok())
        .collect();
    files
        .par_iter()
        .map(|filepath| {
            load_file(filepath.to_str().unwrap(), config_file)
        })
        .filter_map(|x| x.ok())
        .collect::<()>();
    Ok(())
}
