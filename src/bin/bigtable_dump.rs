use std::{fs::File, io::Read, path::PathBuf};

use anyhow::Context;
use pbase::{
    common::Error,
    schema::{TableSchema, TABLE_PTR_BYTE_SIZE},
    table_opener::TableOpener,
};

fn main() -> Result<(), Error> {
    let table_name = "bigtable";
    let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::new());
    let table_opener = TableOpener::new(current_dir);

    // SCHEMA
    let schema_file_name = table_opener.table_schema_file_name(&table_name);
    let schema_file = File::open(schema_file_name).context("Failed to open schema file")?;
    let table_schema: TableSchema =
        serde_json::from_reader(schema_file).context("Failed parsing schema")?;

    dbg!(&table_schema);

    // DATA
    let data_file_name = table_opener.table_data_file_name(&table_name);
    let mut data_file = File::open(data_file_name).context("Cannot open data file")?;
    let mut data_buf: Vec<u8> = vec![];
    data_file
        .read_to_end(&mut data_buf)
        .context("Failed reading data")?;

    let mut pos = 0usize;
    let mut row_idx = 0usize;
    while pos < data_buf.len() {
        println!("Row #{}:", row_idx);

        let mut field_pos = 0usize;
        for (field_name, field_schema) in &table_schema.fields {
            let value = field_schema.value_from_bytes(&data_buf, pos + field_pos);
            println!("\t{} = {:?}", field_name, value);

            field_pos += field_schema.byte_size();
        }

        pos += table_schema.row_byte_size();
        row_idx += 1;
    }

    // INDICES
    for (index_name, index_fields) in &table_schema.indices {
        println!("Index #{}", index_name);

        let index_file_name = table_opener.index_file_name(&table_name, &index_name);
        let mut index_file = File::open(index_file_name).context("Failed opening index file")?;
        let mut index_buf: Vec<u8> = vec![];
        index_file
            .read_to_end(&mut index_buf)
            .context("Cannot read index file")?;

        let mut pos = 0usize;
        let mut row_idx = 0usize;
        let index_row_byte_size = table_schema.index_row_byte_size(&index_name);
        while pos < index_buf.len() {
            println!("\tRow #{}:", row_idx);
            let mut field_pos = 0usize;
            for index_field in index_fields {
                let value =
                    table_schema.fields[index_field].value_from_bytes(&index_buf, pos + field_pos);
                println!("\t\t{} = {:?}", index_field, value);

                field_pos += table_schema.fields[index_field].byte_size();
            }
            let row_ptr: u64 = u64::from_le_bytes(
                index_buf[field_pos + pos..field_pos + pos + TABLE_PTR_BYTE_SIZE].try_into()?,
            );
            println!("\t\tPTR = #{}", row_ptr);

            pos += index_row_byte_size;
            row_idx += 1;
        }
    }

    Ok(())
}
