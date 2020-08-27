use glob::glob;
use log::{info, warn};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use crate::output::{ElasticsearchOutput, SearchEngine};

fn create_search_engine(
    config_file: &str,
) -> Box<dyn SearchEngine>  {
    return
        Box::new(ElasticsearchOutput::new(config_file));
}

fn load_file(filepath: &str, search_engine: &mut Box<dyn SearchEngine>) -> Result<String, String> {
    info!("Reading {}", filepath);
    for line_result in BufReader::new(File::open(filepath).unwrap()).lines() {
        match line_result {
            Ok(line) => search_engine.add_document(line),
            Err(error) => warn!("Can not read line. {:?}", error),
        }
    }
    search_engine.close();
    Ok(format!("Finish: {}", filepath).to_string())
}

pub fn load(
    input_dir: &str,
    config_file: &str,
) -> Result<(), String> {
    // TODO
    let path = Path::new(input_dir).join(Path::new("**/*.json"));
    let initializer = create_search_engine(config_file);
    initializer.initialize();
    // read files from input_dir
    let files: Vec<_> = glob(path.to_str().unwrap())
        .unwrap()
        .filter_map(|x| x.ok())
        .collect();
    files
        .par_iter()
        .map(|filepath| {
            // read JSONs from file
            // create output instance search_engine_type
            let mut search_engine = create_search_engine(config_file);
            load_file(filepath.to_str().unwrap(), &mut search_engine)
        })
        .filter_map(|x| x.ok())
        .collect::<String>();
    Ok(())
}
