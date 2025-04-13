use std::path::PathBuf;

use pbase::{
    common::{Error, Value},
    pbase::PBase,
    query::{FieldSelector, RowFilter, SelectQuery},
};

fn main() -> Result<(), Error> {
    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    let query = SelectQuery {
        // result: vec![FieldSelector {
        //     name: "field1".into(),
        //     source: "bigtable".into(),
        // }],
        from: "bigtable".into(),
        filters: vec![RowFilter {
            field: FieldSelector {
                name: "field1".to_string(),
                source: "bigtable".to_string(),
            },
            op: std::cmp::Ordering::Greater,
            rhs: Value::I32(0),
        }],
        // limit: None,
    };

    let result = db.run_select_query(query);
    dbg!(result);

    Ok(())
}
