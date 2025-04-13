use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use crate::{common::*, query::*, query_tools::*, schema::*, table_opener::TableOpener};

use anyhow::Context;

pub struct PBase {
    table_opener: TableOpener,
}

impl PBase {
    pub fn new(current_dir: PathBuf) -> PBase {
        PBase {
            table_opener: TableOpener::new(current_dir),
        }
    }

    pub fn is_table_exist(&self, table_name: &str) -> bool {
        self.table_opener
            .table_schema_file_name(table_name)
            .exists()
    }

    pub fn run_select_query(
        &self,
        query: SelectQuery,
    ) -> Result<Vec<HashMap<String, Value>>, Error> {
        SelectQueryExecutor::new(&self.table_opener, query).call()
    }

    pub fn run_insert_query(&self, query: InsertQuery) -> Result<usize, Error> {
        let table_schema = self.table_opener.open_schema(&query.table)?;
        let bytes = table_schema.data_row_to_bytes(&query.values);
        let mut table_data_file = self.table_opener.table_file_for_insert(&query.table)?;
        let new_row_pos = table_data_file
            .metadata()
            .context("Failed reading table file size")?
            .len();
        table_data_file.write(&bytes)?;

        for (index_name, index_fields) in &table_schema.indices {
            self.insert_to_index(index_name, index_fields, &query, &table_schema, new_row_pos)?;
        }

        Ok(1)
    }

    pub fn run_create_table_query(&self, query: CreateTableQuery) -> Result<(), Error> {
        let mut schema_file =
            File::create(self.table_opener.table_schema_file_name(&query.schema.name))?;
        serde_json::to_writer(&mut schema_file, &query.schema)?;

        File::create(self.table_opener.table_data_file_name(&query.schema.name))?;

        println!("Created bigtable");

        Ok(())
    }

    fn insert_to_index(
        &self,
        index_name: &str,
        index_fields: &Vec<String>,
        query: &InsertQuery,
        table_schema: &TableSchema,
        row_ptr: TablePtrType,
    ) -> Result<(), Error> {
        // TODO:
        // - experiment with memory mapped vs raw file mode.

        // Extract exact fields in exact order.
        let index_values: Vec<&Value> = index_fields
            .iter()
            .map(|index_field_name| query.values.get(index_field_name).unwrap_or(&Value::NULL))
            .collect();

        let index_row_bytes =
            &table_schema.index_row_to_bytes(index_name, &query.values, row_ptr)[..];

        // Find position.
        let index_file_name = self.table_opener.index_file_name(&query.table, index_name);
        let mut index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&index_file_name)
            .context(format!("Cannot open index file: {:?}", &index_file_name))?;

        if index_file.metadata()?.len() == 0 {
            // Short circuit. We cannot map "nothing" to memory.
            index_file.write_all(index_row_bytes)?;
            return Ok(());
        }

        let index_file_mmap = unsafe {
            memmap::MmapOptions::new()
                .map(&index_file)
                .context("Convert index file to memory mapped file.")?
        };
        let insert_pos =
            find_insert_pos_in_index(&index_name, &index_file_mmap, &index_values, &table_schema);

        // Divide index list + insert + merge
        let index_row_size = table_schema.index_row_byte_size(index_name);
        let lhs_bytes = &index_file_mmap[0..insert_pos * index_row_size];
        let rhs_bytes = &index_file_mmap[insert_pos * index_row_size..];

        // Save
        let tmp_file_path = index_file_name.with_extension("tmp");
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_file_path)?;

        tmp_file.write_all(lhs_bytes)?;
        tmp_file.write_all(index_row_bytes)?;
        tmp_file.write_all(rhs_bytes)?;

        drop(index_file_mmap);
        drop(index_file);

        std::fs::rename(tmp_file_path, index_file_name)?;

        Ok(())
    }
}
