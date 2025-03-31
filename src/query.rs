use std::collections::HashMap;

use crate::{common::Value, schema::TableSchema};

pub struct FieldSelector {
    pub name: String,
    pub source: String,
}

pub struct SelectQuery {
    pub result: Vec<FieldSelector>,
    pub from: String,
    pub limit: Option<usize>,
}

pub struct InsertQuery {
    pub table: String,
    pub values: HashMap<String, Value>,
}

pub struct CreateTableQuery {
    pub schema: TableSchema,
}
