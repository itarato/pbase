use std::{
    fs::{File, OpenOptions},
    path::PathBuf,
};

use memmap::Mmap;

use crate::{common::Error, schema::TableSchema};

pub struct TableOpener {
    pub dir: PathBuf,
}

impl TableOpener {
    pub fn new(dir: PathBuf) -> TableOpener {
        TableOpener { dir }
    }

    pub fn table_data_file_name(&self, table_name: &str) -> PathBuf {
        let mut out = self.dir.clone();
        out.push(format!("{}.pbd", table_name));
        out
    }

    pub fn table_schema_file_name(&self, table_name: &str) -> PathBuf {
        let mut out = self.dir.clone();
        out.push(format!("{}.pbs", table_name));
        out
    }

    pub fn index_file_name(&self, table_name: &str, index_name: &str) -> PathBuf {
        let mut out = self.dir.clone();
        out.push(format!("{}__{}.pbi", table_name, index_name));
        out
    }

    pub fn table_mmap(&self, table_name: &str) -> Result<Mmap, Error> {
        let table_file = File::open(self.table_data_file_name(&table_name))?;
        Ok(unsafe { memmap::MmapOptions::new().map(&table_file)? })
    }

    pub fn open_schema(&self, table_name: &str) -> Result<TableSchema, Error> {
        let schema_file = File::open(self.table_schema_file_name(table_name))?;
        let table_schema = serde_json::from_reader(schema_file)?;
        Ok(table_schema)
    }

    pub fn table_file_for_insert(&self, table_name: &str) -> Result<File, Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.table_data_file_name(table_name))?;
        Ok(file)
    }
}
