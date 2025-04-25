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
    #[must_use]
    pub const fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    #[must_use]
    pub fn table_data_file_name(&self, table_name: &str) -> PathBuf {
        let mut out = self.dir.clone();
        out.push(format!("{table_name}.pbd"));
        out
    }

    #[must_use]
    pub fn table_schema_file_name(&self, table_name: &str) -> PathBuf {
        let mut out = self.dir.clone();
        out.push(format!("{table_name}.pbs"));
        out
    }

    #[must_use]
    pub fn index_file_name(&self, table_name: &str, index_name: &str) -> PathBuf {
        let mut out = self.dir.clone();
        out.push(format!("{table_name}__{index_name}.pbi"));
        out
    }

    /// # Errors
    ///
    /// On file operations.
    pub fn table_mmap(&self, table_name: &str) -> Result<Mmap, Error> {
        let table_file = File::open(self.table_data_file_name(table_name))?;
        Ok(unsafe { memmap::MmapOptions::new().map(&table_file)? })
    }

    /// # Errors
    ///
    /// On file operations.
    pub fn index_mmap(&self, table_schema: &TableSchema, index_name: &str) -> Result<Mmap, Error> {
        let index_file = File::open(self.index_file_name(&table_schema.name, index_name))?;
        Ok(unsafe { memmap::MmapOptions::new().map(&index_file)? })
    }

    /// # Errors
    ///
    /// On file operations.
    pub fn open_schema(&self, table_name: &str) -> Result<TableSchema, Error> {
        let schema_file = File::open(self.table_schema_file_name(table_name))?;
        let table_schema = serde_json::from_reader(schema_file)?;
        Ok(table_schema)
    }

    /// # Errors
    ///
    /// On file operations.
    pub fn table_file_for_insert(&self, table_name: &str) -> Result<File, Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.table_data_file_name(table_name))?;
        Ok(file)
    }
}
