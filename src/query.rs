use std::{cmp::Ordering, collections::HashMap};

use crate::{schema::TableSchema, value::Value};

#[derive(Clone, PartialEq, Eq)]
pub struct FieldSelector {
    pub name: String,
    pub source: String,
}

impl FieldSelector {
    #[must_use]
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
    #[must_use]
    pub const fn new_single(table: String) -> Self {
        Self::Single(table)
    }

    /// # Panics
    ///
    /// Panics when the tables are the same.
    #[must_use]
    pub fn new_multi(table_lhs: String, table_rhs: String) -> Self {
        assert!(table_lhs != table_rhs);
        if table_lhs <= table_rhs {
            Self::Multi(table_lhs, table_rhs)
        } else {
            Self::Multi(table_rhs, table_lhs)
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum RhsValue {
    Value(Value),
    Ref(FieldSelector),
}

impl RhsValue {
    /// # Panics
    ///
    /// Caller is reponsible for ensuring it's the value variant.
    #[must_use]
    pub fn as_value(&self) -> &Value {
        match self {
            Self::Value(v) => v,
            Self::Ref(_) => panic!("Unexpected reference value in single index filtering"),
        }
    }

    /// # Panics
    ///
    /// Caller is reponsible for ensuring it's the reference variant.
    #[must_use]
    pub fn as_field_selector(&self) -> &FieldSelector {
        match self {
            Self::Ref(field_selector) => field_selector,
            Self::Value(_) => panic!("Unexpected regular value in single index filtering"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RowFilter {
    pub field: FieldSelector,
    pub op: Ordering,
    pub rhs: RhsValue,
}

impl RowFilter {
    #[must_use]
    pub fn filter_source(&self) -> FilterSource {
        match &self.rhs {
            RhsValue::Value(_) => FilterSource::Single(self.field.source.clone()),
            RhsValue::Ref(reference) => {
                FilterSource::new_multi(self.field.source.clone(), reference.source.clone())
            }
        }
    }

    #[must_use]
    pub const fn is_multi_table(&self) -> bool {
        match self.rhs {
            RhsValue::Value(_) => false,
            RhsValue::Ref(_) => true,
        }
    }

    #[must_use]
    pub const fn is_single_table(&self) -> bool {
        !self.is_multi_table()
    }

    #[must_use]
    pub fn is_multi_same_table(&self) -> bool {
        match &self.rhs {
            RhsValue::Value(_) => false,
            RhsValue::Ref(reference) => reference.source == self.field.source,
        }
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
