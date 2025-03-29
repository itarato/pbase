use std::collections::HashMap;

pub struct I32FieldSchema {
    pub required: bool,
}

pub enum FieldSchema {
    I32(I32FieldSchema),
}

pub struct TableSchema {
    pub name: String,
    pub fields: HashMap<String, FieldSchema>,
}

pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}
