use indexmap::IndexMap;
use pbase::pbase::*;
use pbase::query::*;
use pbase::schema::*;
use std::path::PathBuf;

fn main() {
    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    if !db.is_table_exist("bigtable") {
        let create_table_query = CreateTableQuery {
            schema: TableSchema {
                name: "bigtable".into(),
                fields: IndexMap::from([
                    (
                        "field1".into(),
                        FieldSchema::I32(I32FieldSchema { required: true }),
                    ),
                    (
                        "field2".into(),
                        FieldSchema::I32(I32FieldSchema { required: true }),
                    ),
                    (
                        "field3".into(),
                        FieldSchema::I32(I32FieldSchema { required: true }),
                    ),
                    (
                        "field4".into(),
                        FieldSchema::I32(I32FieldSchema { required: true }),
                    ),
                ]),
            },
        };

        let result = db.run_create_table_query(create_table_query);
        let _ = dbg!(result);
    } else {
        println!("Table already exist");
    }
}
