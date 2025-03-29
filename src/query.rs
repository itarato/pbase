use std::collections::HashMap;

pub enum Value {
    I32(i32),
}

pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
}

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
