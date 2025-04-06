use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::common::Value;

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

    pub fn value_from_bytes(&self, bytes: &[u8], pos: usize) -> Value {
        let value_bytes = &bytes[pos..pos + self.byte_size()];
        match self {
            FieldSchema::I32(_) => {
                let value = i32::from_le_bytes(
                    value_bytes.try_into().expect("slice with incorrect length"),
                );
                Value::I32(value)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub fields: IndexMap<String, FieldSchema>,
    pub indices: HashMap<String, Vec<String>>,
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

    pub fn index_row_byte_size(&self, index_name: &str) -> usize {
        self.indices[index_name]
            .iter()
            .map(|index_field_name| self.fields[index_field_name].byte_size())
            .sum()
    }

    pub fn index_row_to_bytes(&self, index_name: &str, values: &HashMap<String, Value>) -> Vec<u8> {
        let mut out = vec![];
        out.resize(self.index_row_byte_size(index_name), 0u8);

        let mut pos = 0usize;
        for index_field in &self.indices[index_name] {
            let field_byte_size = self.fields[index_field].byte_size();
            if let Some(value) = values.get(index_field) {
                value.copy_bytes_to(&mut out, pos);
            }

            pos += field_byte_size;
        }

        out
    }
}

pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}
