use std::{collections::HashMap, path::PathBuf};

use indexmap::IndexMap;
use pbase::{
    common::delete_all_files_by_glob,
    pbase::PBase,
    query::{CreateTableQuery, FieldSelector, InsertQuery, RhsValue, RowFilter, SelectQuery},
    schema::{FieldSchema, TableSchema},
    value::Value,
};

#[test]
fn test_basic_single_table_create_and_load() {
    delete_all_files_by_glob("testtable*");

    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    // Create table.
    let create_table_query = CreateTableQuery {
        schema: TableSchema {
            name: "testtable".into(),
            fields: IndexMap::from([
                ("field1".into(), FieldSchema::I32),
                ("field2".into(), FieldSchema::I32),
                ("field3".into(), FieldSchema::I32),
            ]),
            indices: HashMap::from([(
                "field_1_and_2".into(),
                vec!["field1".into(), "field2".into()],
            )]),
        },
    };

    let create_result = db.run_create_table_query(&create_table_query);
    assert!(create_result.is_ok());

    // Insert.
    let insert_query = InsertQuery {
        table: "testtable".into(),
        values: HashMap::from([
            ("field1".into(), Value::I32(1)),
            ("field2".into(), Value::I32(10)),
            ("field3".into(), Value::I32(100)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "testtable".into(),
        values: HashMap::from([
            ("field1".into(), Value::I32(2)),
            ("field2".into(), Value::I32(20)),
            ("field3".into(), Value::I32(200)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    let insert_query = InsertQuery {
        table: "testtable".into(),
        values: HashMap::from([
            ("field1".into(), Value::I32(3)),
            ("field2".into(), Value::I32(30)),
            ("field3".into(), Value::I32(300)),
        ]),
    };
    let insert_result = db.run_insert_query(&insert_query);
    assert!(insert_result.is_ok());

    // Query.
    let query = SelectQuery {
        from: "testtable".into(),
        joins: vec![],
        filters: vec![RowFilter {
            field: FieldSelector {
                name: "field1".to_string(),
                source: "testtable".to_string(),
            },
            op: std::cmp::Ordering::Equal,
            rhs: RhsValue::Value(Value::I32(2)),
        }],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(1, query_result.as_ref().unwrap().len());
    assert_eq!(
        Value::I32(20),
        query_result.as_ref().unwrap()[0]["testtable.field2"]
    );

    let query = SelectQuery {
        from: "testtable".into(),
        joins: vec![],
        filters: vec![RowFilter {
            field: FieldSelector {
                name: "field1".to_string(),
                source: "testtable".to_string(),
            },
            op: std::cmp::Ordering::Less,
            rhs: RhsValue::Value(Value::I32(2)),
        }],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(1, query_result.as_ref().unwrap().len());
    assert_eq!(
        Value::I32(10),
        query_result.as_ref().unwrap()[0]["testtable.field2"]
    );

    let query = SelectQuery {
        from: "testtable".into(),
        joins: vec![],
        filters: vec![RowFilter {
            field: FieldSelector {
                name: "field1".to_string(),
                source: "testtable".to_string(),
            },
            op: std::cmp::Ordering::Greater,
            rhs: RhsValue::Value(Value::I32(2)),
        }],
    };

    let query_result = db.run_select_query(query);
    assert!(query_result.is_ok());
    assert_eq!(1, query_result.as_ref().unwrap().len());
    assert_eq!(
        Value::I32(30),
        query_result.as_ref().unwrap()[0]["testtable.field2"]
    );
}
