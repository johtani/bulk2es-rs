use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::StatusCode;
use elasticsearch::indices::{IndicesCreateParts, IndicesExistsParts};
use elasticsearch::{BulkParts, Elasticsearch};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::fs::File;
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
pub struct EsConfig {
    url: String,
    buffer_size: usize,
    index_name: String,
    schema_file: String,
    id_field_name: String,
}

impl EsConfig {
    fn new(config_file: &str) -> Self {
        let f = File::open(config_file)
            .expect(format!("config file is not found. {}", config_file).as_str());
        let config: EsConfig = serde_yaml::from_reader(f).expect(format!("Parse Error").as_str());
        return config;
    }
}

pub struct ElasticsearchOutput {
    client: Elasticsearch,
    buffer: Vec<String>,
    config: EsConfig,
}

pub fn load_schema(schema_file: &str) -> Value {
    info!("schema file is {}", schema_file);
    let f = File::open(schema_file)
        .expect(format!("schema file is not found. {}", schema_file).as_str());
    let schema: Value = serde_json::from_reader(f).expect("schema cannot read...");
    return schema;
}

impl ElasticsearchOutput {
    pub fn new(_config_file: &str) -> Self {
        // read config
        let config = EsConfig::new(_config_file);
        debug!("url: {}", config.url);
        debug!("buffer_size: {}", config.buffer_size);
        // TODO Elastic Cloud?
        let url = Url::parse(config.url.as_str()).unwrap();
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool)
            .disable_proxy()
            .build()
            .unwrap();
        let client = Elasticsearch::new(transport);
        let buffer = vec![];
        ElasticsearchOutput {
            client,
            buffer,
            config,
        }
    }

    pub fn add_document(&mut self, _document: String) {
        self.buffer.push(_document);
    }

    pub fn initialize(&self) {
        if self.exist_index() {
            //no-op if index already exists
            info!(
                "{} index already exists. skip initialization phase.",
                &self.config.index_name
            );
        } else {
            // load schema.json from file
            // -> if not found, panic!
            // create index with schema file
            info!("{} index is creating...", &self.config.index_name);
            let mut _rt = tokio::runtime::Runtime::new().expect("Fail initializing runtime");
            let task = self.call_indices_create();
            _rt.block_on(task).expect("Something wrong...")
        }
    }

    pub fn exist_index(&self) -> bool {
        let mut _rt = tokio::runtime::Runtime::new().expect("Fail initializing runtime");
        let task = self.call_indices_exists();
        _rt.block_on(task).expect("Something wrong...")
    }

    pub fn close(&mut self) {
        let chunk_size = if self.buffer.len() <= self.config.buffer_size {
            self.buffer.len()
        } else {
            self.config.buffer_size
        };
        let mut _rt = tokio::runtime::Runtime::new().expect("Fail initializing runtime");
        let mut tasks = vec![];
        for chunk in self.buffer.chunks(chunk_size) {
            let task = self.proceed_chunk(chunk);
            tasks.push(task);
        }

        for task in tasks {
            _rt.block_on(task).expect("Error on task...");
        }
        self.buffer.clear();
    }
}

impl ElasticsearchOutput {
    async fn call_indices_create(&self) -> Result<(), String> {
        let schema_json = load_schema(&self.config.schema_file);
        let response = self
            .client
            .indices()
            .create(IndicesCreateParts::Index(&self.config.index_name))
            .body(schema_json)
            .send()
            .await;
        return match response {
            Ok(response) => {
                if !response.status_code().is_success() {
                    warn!(
                        "Create index request has failed. Status Code is {:?}.",
                        response.status_code()
                    );
                    Err(String::from("Create index failed"))
                } else {
                    info!("{} index was created.", &self.config.index_name);
                    Ok(())
                }
            }
            Err(error) => {
                error!("create index failed. {}", error);
                Err(error.to_string())
            }
        }
    }

    async fn call_indices_exists(&self) -> Result<bool, String> {
        let indices: [&str; 1] = [&self.config.index_name.as_str()];
        let result = self
            .client
            .indices()
            .exists(IndicesExistsParts::Index(&indices))
            .send()
            .await;
        return match result {
            Ok(response) => match response.status_code() {
                StatusCode::NOT_FOUND => Ok(false),
                StatusCode::OK => Ok(true),
                _ => {
                    warn!(
                        "Indices exists request has failed. Status Code is {:?}.",
                        response.status_code()
                    );
                    warn!("Indices exists request failed");
                    Err(format!(
                        "Indices exists request failed. {:?}",
                        response.status_code()
                    ))
                }
            },
            Err(error) => {
                error!("Indices exists request failed...");
                Err(error.to_string())
            }
        }
    }

    pub async fn proceed_chunk(&self, chunk: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        let mut body: Vec<JsonBody<_>> = Vec::new();
        for d in chunk {
            let doc_map: Map<String, Value> =
                serde_json::from_str(d.as_str()).expect("something wrong during parsing json");
            let id = match doc_map.get(self.config.id_field_name.as_str()) {
                None => panic!("ID not found... skip this line. {}", d),
                Some(id_value) => match id_value.as_str() {
                    None => panic!("ID not found... skip this line. {}"),
                    Some(id_str) => id_str,
                },
            };
            body.push(json!({"index": {"_id": id}}).into());
            body.push(JsonBody::from(serde_json::to_value(doc_map).unwrap()));
        }
        info!("Sending {} documents... ", chunk.len());
        let bulk_response = self
            .client
            .bulk(BulkParts::Index(self.config.index_name.as_str()))
            .body(body)
            .send()
            .await?;
        if !bulk_response.status_code().is_success() {
            warn!(
                "Bulk request has failed. Status Code is {:?}. ",
                bulk_response.status_code(),
            );
            panic!("bulk indexing failed")
        } else {
            debug!("response : {}", bulk_response.status_code());
            let response_body = bulk_response.json::<Value>().await?;
            let successful = response_body["errors"].as_bool().unwrap() == false;
            if successful == false {
                warn!("Bulk Request has some errors. {:?}", successful);
                let items = response_body["items"].as_array().unwrap();
                for item in items {
                    if let Some(index_obj) = item["index"].as_object() {
                        if index_obj.contains_key("error") {
                            if let Some(obj) = index_obj["error"].as_object() {
                                warn!(
                                    "error id:[{}], type:[{}], reason:[{}]",
                                    index_obj.get("_id").unwrap(),
                                    obj.get("type").unwrap(),
                                    obj.get("reason").unwrap()
                                );
                            }
                        }
                    }
                }
            }
        }
        debug!("Finished bulk request.");
        Ok(())
    }
}
