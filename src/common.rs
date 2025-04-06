use std::{cmp::Ordering, collections::HashMap, path::PathBuf};

use thiserror;

use crate::schema::TableSchema;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {
    #[error("Table size is invalid")]
    InvalidTableSizeError,
}

#[derive(Debug, PartialEq, PartialOrd, Eq)]
pub enum Value {
    NULL,
    I32(i32),
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::NULL, Value::NULL) => Ordering::Equal,

            (Value::NULL, Value::I32(_)) => Ordering::Less,
            (Value::I32(_), Value::NULL) => Ordering::Greater,

            (Value::I32(lhs), Value::I32(rhs)) => lhs.cmp(rhs),
            // _ => panic!("Values cannot be compared {:?} ? {:?}", self, other),
        }
    }
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

pub fn index_file_name(dir: &PathBuf, table_name: &str, index_name: &str) -> PathBuf {
    let mut out = dir.clone();
    out.push(format!("{}__{}.pbi", table_name, index_name));
    out
}

pub fn parse_row_bytes(bytes: &[u8], schema: &TableSchema) -> HashMap<String, Value> {
    let mut out = HashMap::new();

    let mut pos = 0usize;
    for (field_name, field_schema) in &schema.fields {
        out.insert(
            field_name.clone(),
            field_schema.value_from_bytes(&bytes, pos),
        );
        pos += field_schema.byte_size();
    }

    out
}
