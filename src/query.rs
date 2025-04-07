use std::{cmp::Ordering, collections::HashMap};

use crate::{common::Value, schema::TableSchema};

pub struct FieldSelector {
    pub name: String,
    pub source: String,
}

pub struct RowFilter {
    field: FieldSelector,
    op: Ordering,
    rhs: Value,
}

pub struct SelectQuery {
    // pub result: Vec<FieldSelector>,
    pub from: String,
    // List of AND-ed filters.
    pub filters: Vec<RowFilter>,
    // pub limit: Option<usize>,
}

pub struct InsertQuery {
    pub table: String,
    pub values: HashMap<String, Value>,
}

pub struct CreateTableQuery {
    pub schema: TableSchema,
}
