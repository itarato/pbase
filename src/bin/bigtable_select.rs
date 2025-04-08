use std::path::PathBuf;

use pbase::{common::Error, pbase::PBase, query::SelectQuery};

fn main() -> Result<(), Error> {
    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    let query = SelectQuery {
        // result: vec![FieldSelector {
        //     name: "field1".into(),
        //     source: "bigtable".into(),
        // }],
        from: "bigtable".into(),
        filters: vec![],
        // limit: None,
    };

    let result = db.run_select_query(query);
    dbg!(result);

    Ok(())
}
