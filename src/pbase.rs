use crate::{
    common::*,
    query::{Query, SelectQuery},
};
use std::path::PathBuf;

pub struct PBase {
    active_database: Option<PathBuf>,
}

impl PBase {
    pub fn new() -> PBase {
        PBase {
            active_database: None,
        }
    }

    pub fn select_database<PB>(&mut self, name: PB) -> Result<(), Error>
    where
        PB: Into<PathBuf>,
    {
        let name: PathBuf = name.into();

        if name.exists() && name.is_file() {
            self.active_database = Some(name);
            Ok(())
        } else {
            Err(PBaseError::DatabaseDoesNotExist.into())
        }
    }

    pub fn run_query(&self, query: Query) {
        match query {
            Query::Select(select_query) => self.run_select_query(select_query),
        }
    }

    fn run_select_query(&self, select_query: SelectQuery) {}
}
