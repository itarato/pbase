use std::{cmp::Ordering, collections::HashMap};

use crate::{schema::TableSchema, value::Value};

#[derive(Clone)]
pub struct FieldSelector {
    pub name: String,
    pub source: String,
}

impl FieldSelector {
    pub fn full_name(&self) -> String {
        let mut out = self.source.clone();
        out += ".";
        out += self.name.as_str();
        out
    }
}

#[derive(Hash, PartialEq, Eq)]
pub enum FilterSource {
    Single(String),
    Multi(String, String),
}

impl FilterSource {
    pub fn new_single(table: String) -> FilterSource {
        FilterSource::Single(table)
    }

    pub fn new_multi(table_lhs: String, table_rhs: String) -> FilterSource {
        assert!(table_lhs != table_rhs);
        if table_lhs <= table_rhs {
            FilterSource::Multi(table_lhs, table_rhs)
        } else {
            FilterSource::Multi(table_rhs, table_lhs)
        }
    }
}

#[derive(Clone)]
pub struct RowFilter {
    pub field: FieldSelector,
    pub op: Ordering,
    // TODO: when `rhs` can be a join-ed field, `self.filter_source` needs to support mutli-source.
    pub rhs: Value,
}

impl RowFilter {
    pub fn filter_source(&self) -> FilterSource {
        FilterSource::Single(self.field.source.clone())
    }
}

pub enum JoinType {
    Inner,
    // Left,
    // Rigt,
    // Outer,
}

pub struct JoinContract {
    pub join_type: JoinType,
    pub lhs: FieldSelector,
    pub rhs: FieldSelector,
}

pub struct SelectQuery {
    pub from: String,
    pub joins: Vec<JoinContract>,
    // List of AND-ed filters.
    pub filters: Vec<RowFilter>,
}

pub struct InsertQuery {
    pub table: String,
    pub values: HashMap<String, Value>,
}

pub struct CreateTableQuery {
    pub schema: TableSchema,
}
