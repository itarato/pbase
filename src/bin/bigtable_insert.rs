use pbase::common::*;
use pbase::pbase::*;
use pbase::query::*;
use rand::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;

fn main() {
    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    let mut rng = rand::rng();

    for _ in 1..10_000 {
        let insert_query = InsertQuery {
            table: "bigtable".into(),
            values: HashMap::from([
                ("field1".into(), Value::I32(rng.random::<i32>() % 1000)),
                ("field2".into(), Value::I32(rng.random::<i32>() % 1000)),
                ("field3".into(), Value::I32(rng.random::<i32>() % 1000)),
                ("field4".into(), Value::I32(rng.random::<i32>() % 1000)),
            ]),
        };
        if let Err(err) = db.run_insert_query(insert_query) {
            eprintln!("Failed insert: {}", err);
            break;
        }
    }
}
