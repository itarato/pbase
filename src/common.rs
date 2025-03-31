use std::{collections::HashMap, path::PathBuf};

use thiserror;

use crate::schema::{FieldSchema, TableSchema};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {
    #[error("Table size is invalid")]
    InvalidTableSizeError,
}

#[derive(Debug)]
pub enum Value {
    I32(i32),
}

pub fn table_data_file_name(dir: &PathBuf, table_name: &str) -> PathBuf {
    let mut out = dir.clone();
    out.push(format!("{}.pbd", table_name));
    out
}

pub fn table_schema_file_name(dir: &PathBuf, table_name: &str) -> PathBuf {
    let mut out = dir.clone();
    out.push(format!("{}.pbs", table_name));
    out
}

pub fn parse_row_bytes(bytes: &[u8], schema: &TableSchema) -> HashMap<String, Value> {
    let mut out = HashMap::new();

    let mut pos = 0usize;
    for (field_name, field_schema) in &schema.fields {
        match field_schema {
            FieldSchema::I32(_) => {
                let value_bytes = &bytes[pos..pos + field_schema.byte_size()];
                let value = i32::from_le_bytes(
                    value_bytes.try_into().expect("slice with incorrect length"),
                );

                out.insert(field_name.clone(), Value::I32(value));
            }
        };

        pos += field_schema.byte_size();
    }

    out
}
