use pbase::{
    common::Error,
    pbase::PBase,
    query::{FieldSelector, RowFilter, SelectQuery},
    value::*,
};
use std::path::PathBuf;

fn main() -> Result<(), Error> {
    env_logger::init();

    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    let query = SelectQuery {
        from: "bigtable".into(),
        joins: vec![],
        filters: vec![RowFilter {
            field: FieldSelector {
                name: "field1".to_string(),
                source: "bigtable".to_string(),
            },
            op: std::cmp::Ordering::Greater,
            rhs: Value::I32(0),
        }],
    };

    let result = db.run_select_query(query)?;
    dbg!(result);

    Ok(())
}
