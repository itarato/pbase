use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct I32FieldSchema {
    pub required: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FieldSchema {
    I32(I32FieldSchema),
}

impl FieldSchema {
    pub fn byte_size(&self) -> usize {
        match self {
            FieldSchema::I32(_) => 4,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub fields: IndexMap<String, FieldSchema>,
}

pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}
