mod common;
mod database;
mod pbase;
mod query;
mod schema;

use std::collections::HashMap;

use crate::pbase::*;
use crate::query::*;

fn main() {
    let mut db = PBase::new();
    db.select_database("example").unwrap();

    let insert_query = InsertQuery {
        table: "example".into(),
        values: HashMap::from([("".into(), Value::I32(123))]),
    };

    let select_query = Query::Select(SelectQuery {
        result: vec![FieldSelector {
            name: "value".into(),
            source: "example".into(),
        }],
        from: "example".into(),
        limit: None,
    });

    // let rows = db.query("SELECT * FROM users").unwrap();
}
