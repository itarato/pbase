use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use bytes::BytesMut;
use memmap::Mmap;

use crate::{
    common::{
        index_file_name, parse_row_bytes, table_data_file_name, table_schema_file_name, Error,
        PBaseError, Value,
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
                Value::NULL => {
                    // Noop - as long as we zero out the line.
                }
            };
        });

        let mut table_data_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(table_data_file_name(&self.current_dir, &query.table))?;
        table_data_file.write(&bytes)?;

        for (index_name, index_fields) in &table_schema.indices {
            self.insert_to_index(index_name, index_fields, &query, &table_schema);
        }

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

    fn insert_to_index(
        &self,
        index_name: &str,
        index_fields: &Vec<String>,
        query: &InsertQuery,
        table_schema: &TableSchema,
    ) -> Result<(), Error> {
        // TODO:
        // - experiment with memory mapped vs raw file mode.

        // Extract exact fields in exact order.
        let index_values: Vec<&Value> = index_fields
            .iter()
            .map(|index_field_name| query.values.get(index_field_name).unwrap_or(&Value::NULL))
            .collect();

        // Find position.
        let index_file_name = index_file_name(&self.current_dir, &query.table, index_name);
        let index_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(index_file_name)?;
        let index_file_mmap = unsafe { memmap::MmapOptions::new().map(&index_file)? };
        let insert_pos = self.find_insert_pos_in_index(
            &index_name,
            &index_file_mmap,
            &index_values,
            &table_schema,
        );

        // Divide index list + insert + merge
        // Save

        Ok(())
    }

    fn find_insert_pos_in_index(
        &self,
        index_name: &str,
        index_file_mmap: &Mmap,
        index_values: &Vec<&Value>,
        table_schema: &TableSchema,
    ) -> usize {
        let index_row_size = table_schema.index_row_byte_size(index_name);

        let mut lhs_idx = -1i32;
        let mut rhs_idx = (index_file_mmap.len() / index_row_size) as i32;

        let mut field_byte_pos = 0usize;
        let mut field_idx = 0usize;
        for index_field_name in &table_schema.indices[index_name] {
            let field_schema = &table_schema.fields[index_field_name];
            let cmp_value = index_values[field_idx];

            // Find pos between lhs_idx and rhs_idx for current level.
            // Find LHS.
            let mut i = lhs_idx;
            let mut j = rhs_idx;
            loop {
                if i + 1 >= j {
                    break;
                }

                let mid = (i + j) / 2;
                let mid_value_bytes_pos = mid as usize * index_row_size + field_byte_pos;
                let mid_value =
                    field_schema.value_from_bytes(&index_file_mmap[..], mid_value_bytes_pos);

                if &mid_value < cmp_value {
                    i = mid;
                } else {
                    j = mid;
                }
            }

            // Find RHS.
            lhs_idx = i; // Final.
            j = rhs_idx;

            loop {
                if i + 1 >= j {
                    break;
                }

                let mid = (i + j) / 2;
                let mid_value_bytes_pos = mid as usize * index_row_size + field_byte_pos;
                let mid_value =
                    field_schema.value_from_bytes(&index_file_mmap[..], mid_value_bytes_pos);

                if &mid_value > cmp_value {
                    j = mid;
                } else {
                    i = mid;
                }
            }

            rhs_idx = j; // Final.

            field_byte_pos += field_schema.byte_size();
            field_idx += 1;
        }

        unimplemented!()
    }
}
