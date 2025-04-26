use std::{collections::HashMap, fs, path::PathBuf};

use indexmap::IndexMap;
use pbase::{
    pbase::PBase,
    query::{CreateTableQuery, FieldSelector, InsertQuery, JoinContract, RowFilter, SelectQuery},
    schema::{FieldSchema, TableSchema},
    value::Value,
};

#[test]
fn test_single_tables() {
    let db = setup_multi_tables("qqq");

    // Total t1 query.
    let query = SelectQuery {
        from: "qqq_t1".into(),
        joins: vec![],
        filters: vec![],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(4, query_result.as_ref().unwrap().len());

    // Total t2 query.
    let query = SelectQuery {
        from: "qqq_t2".into(),
        joins: vec![],
        filters: vec![],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(4, query_result.as_ref().unwrap().len());
}

#[test]
fn test_join_table_all() {
    let db = setup_multi_tables("www");

    // Join query:
    // SELECT *
    // FROM t1
    // JOIN t2 ON t2.t1_id = t1.id
    let query = SelectQuery {
        from: "www_t1".into(),
        joins: vec![JoinContract {
            join_type: pbase::query::JoinType::Inner,
            lhs: FieldSelector {
                name: "id".into(),
                source: "www_t1".into(),
            },
            rhs: FieldSelector {
                name: "t1_id".into(),
                source: "www_t2".into(),
            },
        }],
        filters: vec![],
    };
    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    let query_result = query_result.unwrap();
    assert_eq!(3, query_result.len());

    // ┌──┬─────┐   ┌─────┬─────┐
    // │id│value│   │t1_id│value│
    // ├──┼─────┤   ├─────┼─────┤
    // │0 │100  │   │0    │1000 │
    // │1 │101  │   │0    │2000 │
    // │2 │102  │   │2    │3002 │
    // │3 │103  │   │4    │4004 │
    // └──┴─────┘   └─────┴─────┘

    assert_eq!(
        HashMap::from([
            ("www_t1.id".to_string(), Value::I32(0)),
            ("www_t1.value".to_string(), Value::I32(100)),
            ("www_t2.t1_id".to_string(), Value::I32(0)),
            ("www_t2.value".to_string(), Value::I32(1000)),
        ]),
        query_result[0],
    );
    assert_eq!(
        HashMap::from([
            ("www_t1.id".to_string(), Value::I32(0)),
            ("www_t1.value".to_string(), Value::I32(100)),
            ("www_t2.t1_id".to_string(), Value::I32(0)),
            ("www_t2.value".to_string(), Value::I32(2000)),
        ]),
        query_result[1],
    );
    assert_eq!(
        HashMap::from([
            ("www_t1.id".to_string(), Value::I32(2)),
            ("www_t1.value".to_string(), Value::I32(102)),
            ("www_t2.t1_id".to_string(), Value::I32(2)),
            ("www_t2.value".to_string(), Value::I32(3002)),
        ]),
        query_result[2],
    );
}

#[test]
fn test_join_table_filtered() {
    let db = setup_multi_tables("eee");

    // Join query:
    // SELECT *
    // FROM t1
    // JOIN t2 ON t2.t1_id = t1.id
    let query = SelectQuery {
        from: "eee_t1".into(),
        joins: vec![JoinContract {
            join_type: pbase::query::JoinType::Inner,
            lhs: FieldSelector {
                name: "id".into(),
                source: "eee_t1".into(),
            },
            rhs: FieldSelector {
                name: "t1_id".into(),
                source: "eee_t2".into(),
            },
        }],
        filters: vec![RowFilter {
            field: FieldSelector {
                name: "value".to_string(),
                source: "eee_t2".to_string(),
            },
            op: std::cmp::Ordering::Greater,
            rhs: Value::I32(1500),
        }],
    };
    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    let query_result = query_result.unwrap();
    assert_eq!(2, query_result.len());

    // ┌──┬─────┐   ┌─────┬─────┐
    // │id│value│   │t1_id│value│
    // ├──┼─────┤   ├─────┼─────┤
    // │0 │100  │   │0    │1000 │
    // │1 │101  │   │0    │2000 │
    // │2 │102  │   │2    │3002 │
    // │3 │103  │   │4    │4004 │
    // └──┴─────┘   └─────┴─────┘

    assert_eq!(
        HashMap::from([
            ("eee_t1.id".to_string(), Value::I32(0)),
            ("eee_t1.value".to_string(), Value::I32(100)),
            ("eee_t2.t1_id".to_string(), Value::I32(0)),
            ("eee_t2.value".to_string(), Value::I32(2000)),
        ]),
        query_result[0],
    );
    assert_eq!(
        HashMap::from([
            ("eee_t1.id".to_string(), Value::I32(2)),
            ("eee_t1.value".to_string(), Value::I32(102)),
            ("eee_t2.t1_id".to_string(), Value::I32(2)),
            ("eee_t2.value".to_string(), Value::I32(3002)),
        ]),
        query_result[1],
    );
}

fn setup_multi_tables(prefix: &str) -> PBase {
    let t1_name = format!("{prefix}_t1");
    let t2_name = format!("{prefix}_t2");

    delete_all_by_glob(format!("{t1_name}*").as_str());
    delete_all_by_glob(format!("{t2_name}*").as_str());

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
            name: t1_name.clone(),
            fields: IndexMap::from([
                ("id".into(), FieldSchema::I32),
                ("value".into(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        },
    };
    let create_result = db.run_create_table_query(&create_table_query);
    assert!(create_result.is_ok());

    let create_table_query = CreateTableQuery {
        schema: TableSchema {
            name: t2_name.clone(),
            fields: IndexMap::from([
                ("t1_id".into(), FieldSchema::I32),
                ("value".into(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        },
    };
    let create_result = db.run_create_table_query(&create_table_query);
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
        table: t1_name.clone(),
        values: HashMap::from([
            ("id".into(), Value::I32(0)),
            ("value".into(), Value::I32(100)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t1_name.clone(),
        values: HashMap::from([
            ("id".into(), Value::I32(1)),
            ("value".into(), Value::I32(101)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t1_name.clone(),
        values: HashMap::from([
            ("id".into(), Value::I32(2)),
            ("value".into(), Value::I32(102)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t1_name.clone(),
        values: HashMap::from([
            ("id".into(), Value::I32(3)),
            ("value".into(), Value::I32(103)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t2_name.clone(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(0)),
            ("value".into(), Value::I32(1000)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t2_name.clone(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(0)),
            ("value".into(), Value::I32(2000)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t2_name.clone(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(2)),
            ("value".into(), Value::I32(3002)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: t2_name.clone(),
        values: HashMap::from([
            ("t1_id".into(), Value::I32(4)),
            ("value".into(), Value::I32(4004)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    db
}

fn delete_all_by_glob(pattern: &str) {
    for entry in glob::glob(pattern).expect("Failed to read 'testtable*' files") {
        fs::remove_file(entry.expect("Failed loading path")).expect("Failed deleting file");
    }
}
