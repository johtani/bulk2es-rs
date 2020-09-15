use crate::output::ElasticsearchOutput;
use glob::glob;
use log::{info, warn};
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct Loader<'a> {
    input_dir: &'a str,
    config_file: &'a str,
}

impl<'a> Loader<'a> {
    pub fn new(input_dir: &'a str, config_file: &'a str) -> Self {
        Loader {
            input_dir,
            config_file,
        }
    }

    pub fn load(&self) -> Result<(), String> {
        &self.initialize_es();
        // TODO should we care other files?
        let path = Path::new(&self.input_dir).join(Path::new("**/*.json"));
        // read files from input_dir
        let files: Vec<_> = glob(path.to_str().unwrap())
            .unwrap()
            .filter_map(|x| x.ok())
            .collect();
        files
            .par_iter()
            .map(|filepath| self.load_file(filepath.to_str().unwrap()))
            .filter_map(|x| x.ok())
            .collect::<()>();
        Ok(())
    }

    fn initialize_es(&self) {
        let initializer = ElasticsearchOutput::new(&self.config_file);
        initializer.initialize();
    }

    fn load_file(&self, filepath: &str) -> Result<(), String> {
        let mut search_engine = ElasticsearchOutput::new(&self.config_file);
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
}
