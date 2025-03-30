use std::{fs::File, path::PathBuf};

use crate::{
    common::{table_schema_file_name, Error},
    query::{CreateTableQuery, InsertQuery, SelectQuery},
};

pub struct PBase {
    current_dir: PathBuf,
}

impl PBase {
    pub fn new(current_dir: PathBuf) -> PBase {
        PBase { current_dir }
    }

    pub fn is_table_exist(&self, table_name: &str) -> bool {
        table_schema_file_name(&self.current_dir, table_name).exists()
    }

    pub fn run_select_query(&self, query: SelectQuery) {
        unimplemented!()
    }

    pub fn run_insert_query(&self, query: InsertQuery) {
        unimplemented!()
    }

    pub fn run_create_table_query(&self, query: CreateTableQuery) -> Result<(), Error> {
        let mut schema_file = File::create(table_schema_file_name(
            &self.current_dir,
            &query.schema.name,
        ))?;

        serde_json::to_writer(&mut schema_file, &query.schema)?;

        Ok(())
    }
}
