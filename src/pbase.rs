use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use bytes::BytesMut;

use crate::{
    common::{
        parse_row_bytes, table_data_file_name, table_schema_file_name, Error, PBaseError, Value,
    },
    query::{CreateTableQuery, InsertQuery, SelectQuery},
    schema::TableSchema,
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

    pub fn run_select_query(
        &self,
        query: SelectQuery,
    ) -> Result<Vec<HashMap<String, Value>>, Error> {
        let table_file = File::open(table_data_file_name(&self.current_dir, &query.from))?;
        let table_schema: TableSchema = serde_json::from_reader(File::open(
            table_schema_file_name(&self.current_dir, &query.from),
        )?)?;

        let table_file_mmap = unsafe { memmap::MmapOptions::new().map(&table_file)? };
        let table_byte_len = table_file_mmap.len();

        let row_byte_len = table_schema.row_byte_size();

        if table_byte_len % row_byte_len != 0 {
            return Err(PBaseError::InvalidTableSizeError.into());
        }

        let mut rows = vec![];
        let mut pos = 0usize;
        while pos < table_byte_len {
            let row_bytes = &table_file_mmap[pos..pos + row_byte_len];
            let row = parse_row_bytes(&row_bytes, &table_schema);
            rows.push(row);

            pos += row_byte_len;
        }

        Ok(rows)
    }

    pub fn run_insert_query(&self, query: InsertQuery) -> Result<usize, Error> {
        let table_schema: TableSchema = serde_json::from_reader(File::open(
            table_schema_file_name(&self.current_dir, &query.table),
        )?)?;

        let mut bytes = BytesMut::with_capacity(table_schema.row_byte_size());
        bytes.resize(table_schema.row_byte_size(), 0);

        query.values.iter().for_each(|(field_name, field_value)| {
            let field_byte_pos = table_schema.field_byte_pos(field_name);

            match field_value {
                Value::I32(i32_value) => {
                    bytes[field_byte_pos..field_byte_pos + 4]
                        .copy_from_slice(&i32_value.to_le_bytes());
                }
            };
        });

        let mut table_data_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(table_data_file_name(&self.current_dir, &query.table))?;
        table_data_file.write(&bytes)?;

        Ok(1)
    }

    pub fn run_create_table_query(&self, query: CreateTableQuery) -> Result<(), Error> {
        let mut schema_file = File::create(table_schema_file_name(
            &self.current_dir,
            &query.schema.name,
        ))?;

        serde_json::to_writer(&mut schema_file, &query.schema)?;

        File::create(table_data_file_name(&self.current_dir, &query.schema.name))?;

        println!("Created bigtable");

        Ok(())
    }
}
