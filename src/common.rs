use std::path::PathBuf;

use thiserror;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {}

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
