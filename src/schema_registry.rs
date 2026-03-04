use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;

use percent_encoding::NON_ALPHANUMERIC;
use serde::Deserialize;
use tracing::info;

#[derive(Clone, Deserialize)]
pub struct Schema {
    pub subject: String,
    pub id: i32,
    pub schema: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

#[derive(Clone, Deserialize)]
pub struct SchemaReference {
    pub subject: String,
    pub version: i32,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SchemaVersionOrLatest {
    Version(i32),
    Latest,
}

impl From<i32> for SchemaVersionOrLatest {
    fn from(value: i32) -> Self {
        SchemaVersionOrLatest::Version(value)
    }
}

impl fmt::Display for SchemaVersionOrLatest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaVersionOrLatest::Version(v) => v.fmt(f),
            SchemaVersionOrLatest::Latest => f.write_str("latest"),
        }
    }
}

pub struct SchemaRegistryClient {
    agent: ureq::Agent,
    base_url: String,
    cache: RefCell<BTreeMap<(String, SchemaVersionOrLatest), Schema>>,
}

impl SchemaRegistryClient {
    pub fn new(base_url: String) -> Self {
        Self {
            agent: ureq::Agent::new_with_defaults(),
            base_url,
            cache: Default::default(),
        }
    }

    pub fn get_subject_schema_version(
        &self,
        subject: String,
        version: SchemaVersionOrLatest,
    ) -> Result<Schema, ureq::Error> {
        let cache_key = (subject, version);

        if let Some(schema) = self.cache.borrow().get(&cache_key) {
            return Ok(schema.clone());
        }

        let encoded_subject = percent_encoding::utf8_percent_encode(&cache_key.0, NON_ALPHANUMERIC);

        let uri = format!(
            "{}/subjects/{encoded_subject}/versions/{version}",
            self.base_url
        );

        info!("Fetching schema from {uri}");

        let schema = self
            .agent
            .get(uri)
            .call()?
            .body_mut()
            .read_json::<Schema>()?;

        self.cache.borrow_mut().insert(cache_key, schema.clone());

        Ok(schema)
    }
}
