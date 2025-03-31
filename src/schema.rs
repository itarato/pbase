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

impl TableSchema {
    pub fn row_byte_size(&self) -> usize {
        self.fields
            .values()
            .map(|field_schema| field_schema.byte_size())
            .sum()
    }

    pub fn field_byte_pos(&self, field_name: &str) -> usize {
        let mut pos = 0usize;
        for (schema_field_name, field_schema) in &self.fields {
            if schema_field_name == field_name {
                return pos;
            }

            pos += field_schema.byte_size();
        }

        panic!("Field '{}' not found in table '{}'", field_name, self.name)
    }
}

pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}
