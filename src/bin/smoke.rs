use std::collections::HashMap;
use std::path::PathBuf;

use indexmap::IndexMap;

use pbase::common::delete_all_files_by_glob;
use pbase::common::Error;
use pbase::pbase::*;
use pbase::query::*;
use pbase::schema::*;
use pbase::value::*;

fn main() -> Result<(), Error> {
    delete_all_files_by_glob("example*");

    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    if !db.is_table_exist("example") {
        let create_table_query = CreateTableQuery {
            schema: TableSchema {
                name: "example".into(),
                fields: IndexMap::from([("value".into(), FieldSchema::I32)]),
                indices: HashMap::new(),
            },
        };

        let result = db.run_create_table_query(&create_table_query)?;
        dbg!(result);
    }

    let insert_query = InsertQuery {
        table: "example".into(),
        values: HashMap::from([("value".into(), Value::I32(123))]),
    };
    let insert_result = db.run_insert_query(&insert_query)?;
    dbg!(insert_result);

    let select_query = SelectQuery {
        // result: vec![FieldSelector {
        //     name: "value".into(),
        //     source: "example".into(),
        // }],
        from: "example".into(),
        joins: vec![],
        filters: vec![],
        // limit: None,
    };
    let rows = db.run_select_query(select_query)?;
    dbg!(rows);

    Ok(())
}
