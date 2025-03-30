mod common;
mod database;
mod pbase;
mod query;
mod schema;

use std::collections::HashMap;
use std::path::PathBuf;

use indexmap::IndexMap;
use schema::FieldSchema;
use schema::I32FieldSchema;
use schema::TableSchema;

use crate::pbase::*;
use crate::query::*;

fn main() {
    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    if !db.is_table_exist("example") {
        let create_table_query = CreateTableQuery {
            schema: TableSchema {
                name: "example".into(),
                fields: IndexMap::from([(
                    "value".into(),
                    FieldSchema::I32(I32FieldSchema { required: true }),
                )]),
            },
        };

        let result = db.run_create_table_query(create_table_query);
        dbg!(result);
    }

    let insert_query = InsertQuery {
        table: "example".into(),
        values: HashMap::from([("".into(), Value::I32(123))]),
    };
    db.run_insert_query(insert_query);

    let select_query = SelectQuery {
        result: vec![FieldSelector {
            name: "value".into(),
            source: "example".into(),
        }],
        from: "example".into(),
        limit: None,
    };
    let rows = db.run_select_query(select_query);
}
