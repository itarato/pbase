use indexmap::IndexMap;
use pbase::pbase::*;
use pbase::query::*;
use pbase::schema::*;
use std::collections::HashMap;
use std::path::PathBuf;

fn main() {
    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    if !db.is_table_exist("bigtable") {
        let create_table_query = CreateTableQuery {
            schema: TableSchema {
                name: "bigtable".into(),
                fields: IndexMap::from([
                    ("field1".into(), FieldSchema::I32),
                    ("field2".into(), FieldSchema::I32),
                    ("field3".into(), FieldSchema::I32),
                    ("field4".into(), FieldSchema::I32),
                ]),
                indices: HashMap::from([(
                    "field_1_and_2".into(),
                    vec!["field1".into(), "field2".into()],
                )]),
            },
        };

        let result = db.run_create_table_query(create_table_query);
        let _ = dbg!(result);
    } else {
        println!("Table already exist");
    }
}
