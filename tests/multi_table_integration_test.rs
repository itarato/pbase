use std::{collections::HashMap, fs, path::PathBuf};

use indexmap::IndexMap;
use pbase::{
    pbase::PBase,
    query::{CreateTableQuery, FieldSelector, InsertQuery, JoinContract, RowFilter, SelectQuery},
    schema::{FieldSchema, TableSchema},
    value::Value,
};

#[test]
fn test_one_join_table_create_and_load() {
    delete_all_by_glob("t1*");
    delete_all_by_glob("t2*");

    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    // ┌─────┐   ┌─────┐
    // │t1   │   │t2   │
    // ├─────┤   ├─────┤
    // │id   ├──►│t1_id│
    // │value│   │value│
    // └─────┘   └─────┘

    // Create tables.
    let create_table_query = CreateTableQuery {
        schema: TableSchema {
            name: "t1".into(),
            fields: IndexMap::from([
                ("id".into(), FieldSchema::I32),
                ("value".into(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        },
    };
    let create_result = db.run_create_table_query(create_table_query);
    assert!(create_result.is_ok());

    let create_table_query = CreateTableQuery {
        schema: TableSchema {
            name: "t2".into(),
            fields: IndexMap::from([
                ("t1_id".into(), FieldSchema::I32),
                ("value".into(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        },
    };
    let create_result = db.run_create_table_query(create_table_query);
    assert!(create_result.is_ok());

    // ┌──┬─────┐   ┌─────┬─────┐
    // │id│value│   │t1_id│value│
    // ├──┼─────┤   ├─────┼─────┤
    // │0 │100  │   │0    │1000 │
    // │1 │101  │   │0    │2000 │
    // │2 │102  │   │2    │3002 │
    // │3 │103  │   │4    │4004 │
    // └──┴─────┘   └─────┴─────┘

    // Insert.
    let insert_query = InsertQuery {
        table: "t1".into(),
        values: HashMap::from([
            ("id".into(), Value::I32(0)),
            ("value".into(), Value::I32(100)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t1".into(),
        values: HashMap::from([
            ("id".into(), Value::I32(1)),
            ("value".into(), Value::I32(101)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t1".into(),
        values: HashMap::from([
            ("id".into(), Value::I32(2)),
            ("value".into(), Value::I32(102)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t1".into(),
        values: HashMap::from([
            ("id".into(), Value::I32(3)),
            ("value".into(), Value::I32(103)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t2".into(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(0)),
            ("value".into(), Value::I32(1000)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t2".into(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(0)),
            ("value".into(), Value::I32(2000)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t2".into(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(2)),
            ("value".into(), Value::I32(3002)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "t2".into(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(4)),
            ("value".into(), Value::I32(4004)),
        ]),
    };
    let insert_result = db.run_insert_query(insert_query);
    assert!(insert_result.is_ok());

    // Total t1 query.
    let query = SelectQuery {
        from: "t1".into(),
        joins: vec![],
        filters: vec![],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(4, query_result.as_ref().unwrap().len());

    // Total t2 query.
    let query = SelectQuery {
        from: "t2".into(),
        joins: vec![],
        filters: vec![],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(4, query_result.as_ref().unwrap().len());

    // Join query:
    // SELECT *
    // FROM t1
    // JOIN t2 ON t2.t1_id = t1.id
    let query = SelectQuery {
        from: "t1".into(),
        joins: vec![JoinContract {
            join_type: pbase::query::JoinType::Inner,
            lhs: FieldSelector {
                name: "id".into(),
                source: "t1".into(),
            },
            rhs: FieldSelector {
                name: "t1_id".into(),
                source: "t2".into(),
            },
        }],
        filters: vec![],
    };
}

fn delete_all_by_glob(pattern: &str) {
    for entry in glob::glob(pattern).expect("Failed to read 'testtable*' files") {
        fs::remove_file(entry.expect("Failed loading path")).expect("Failed deleting file");
    }
}
