use std::{cmp::Ordering, collections::HashMap, path::PathBuf};

use thiserror;

use crate::schema::TableSchema;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {
    #[error("Table size is invalid")]
    InvalidTableSizeError,
}

#[derive(Debug, PartialEq, PartialOrd, Eq)]
pub enum Value {
    NULL,
    I32(i32),
}

impl Value {
    pub fn copy_bytes_to(&self, buf: &mut [u8], pos: usize) {
        match self {
            Value::NULL => {} // Noop.
            Value::I32(v) => buf[pos..pos + 4].copy_from_slice(&v.to_le_bytes()),
        };
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::NULL, Value::NULL) => Ordering::Equal,

            (Value::NULL, Value::I32(_)) => Ordering::Less,
            (Value::I32(_), Value::NULL) => Ordering::Greater,

            (Value::I32(lhs), Value::I32(rhs)) => lhs.cmp(rhs),
            // _ => panic!("Values cannot be compared {:?} ? {:?}", self, other),
        }
    }
}

pub fn table_data_file_name(dir: &PathBuf, table_name: &str) -> PathBuf {
    let mut out = dir.clone();
    out.push(format!("{}.pbd", table_name));
    out
}

pub fn table_schema_file_name(dir: &PathBuf, table_name: &str) -> PathBuf {
    let mut out = dir.clone();
    out.push(format!("{}.pbs", table_name));
    out
}

pub fn index_file_name(dir: &PathBuf, table_name: &str, index_name: &str) -> PathBuf {
    let mut out = dir.clone();
    out.push(format!("{}__{}.pbi", table_name, index_name));
    out
}

pub fn parse_row_bytes(bytes: &[u8], schema: &TableSchema) -> HashMap<String, Value> {
    let mut out = HashMap::new();

    let mut pos = 0usize;
    for (field_name, field_schema) in &schema.fields {
        out.insert(
            field_name.clone(),
            field_schema.value_from_bytes(&bytes, pos),
        );
        pos += field_schema.byte_size();
    }

    out
}

pub fn find_insert_pos_in_index(
    index_name: &str,
    index_bytes: &[u8],
    index_values: &Vec<&Value>,
    table_schema: &TableSchema,
) -> usize {
    let index_row_size = table_schema.index_row_byte_size(index_name);

    let mut lhs_idx = -1i32;
    let mut rhs_idx = (index_bytes.len() / index_row_size) as i32;

    let mut field_byte_pos = 0usize;
    let mut field_idx = 0usize;
    for index_field_name in &table_schema.indices[index_name] {
        let field_schema = &table_schema.fields[index_field_name];
        let cmp_value = index_values[field_idx];

        // Find pos between lhs_idx and rhs_idx for current level.
        // Find LHS.
        let mut i = lhs_idx;
        let mut j = rhs_idx;
        loop {
            if i + 1 >= j {
                break;
            }

            let mid = (i + j) / 2;
            let mid_value_bytes_pos = mid as usize * index_row_size + field_byte_pos;
            let mid_value = field_schema.value_from_bytes(&index_bytes, mid_value_bytes_pos);

            if &mid_value < cmp_value {
                i = mid;
            } else {
                j = mid;
            }
        }

        // Find RHS.
        lhs_idx = i; // Final.
        j = rhs_idx;

        loop {
            if i + 1 >= j {
                break;
            }

            let mid = (i + j) / 2;
            let mid_value_bytes_pos = mid as usize * index_row_size + field_byte_pos;
            let mid_value = field_schema.value_from_bytes(&index_bytes, mid_value_bytes_pos);

            if &mid_value > cmp_value {
                j = mid;
            } else {
                i = mid;
            }
        }

        rhs_idx = j; // Final.

        field_byte_pos += field_schema.byte_size();
        field_idx += 1;
    }

    rhs_idx as usize
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indexmap::IndexMap;

    use crate::schema::{FieldSchema, I32FieldSchema, TableSchema};

    use super::*;

    #[test]
    fn test_value_ordering() {
        let null = Value::NULL;
        let i32_zero = Value::I32(0);
        let i32_ten = Value::I32(10);

        assert!(null == null);
        assert!(i32_ten == i32_ten);

        assert!(null < i32_zero);
        assert!(null < i32_ten);

        assert!(null <= i32_zero);
        assert!(null <= i32_ten);

        assert!(i32_zero > null);
        assert!(i32_ten > null);

        assert!(i32_zero >= null);
        assert!(i32_ten >= null);

        assert!(i32_zero < i32_ten);
        assert!(i32_zero <= i32_ten);
    }

    #[test]
    fn test_find_insert_pos_in_index_single_field_index() {
        let table_schema = TableSchema {
            name: "fake_table".to_string(),
            fields: IndexMap::from([(
                "col1".to_string(),
                FieldSchema::I32(I32FieldSchema { required: true }),
            )]),
            indices: HashMap::from([("fake_index".to_string(), vec!["col1".to_string()])]),
        };

        assert_find_insert_pos_in_index(vec![0, 0, 1, 1, 3, 3], vec![2], 4, &table_schema);
        assert_find_insert_pos_in_index(vec![0, 0, 1, 1, 3, 3], vec![1], 4, &table_schema);
        assert_find_insert_pos_in_index(vec![0, 0, 1, 1, 3, 3], vec![0], 2, &table_schema);
        assert_find_insert_pos_in_index(vec![0, 0, 1, 1, 3, 3], vec![3], 6, &table_schema);
        assert_find_insert_pos_in_index(vec![0, 0, 1, 1, 3, 3], vec![-1], 0, &table_schema);
        assert_find_insert_pos_in_index(vec![0, 0, 1, 1, 3, 3], vec![4], 6, &table_schema);
    }

    #[test]
    fn test_find_insert_pos_in_index_multi_field_index() {
        let table_schema = TableSchema {
            name: "fake_table".to_string(),
            fields: IndexMap::from([
                (
                    "col1".to_string(),
                    FieldSchema::I32(I32FieldSchema { required: true }),
                ),
                (
                    "col2".to_string(),
                    FieldSchema::I32(I32FieldSchema { required: true }),
                ),
            ]),
            indices: HashMap::from([(
                "fake_index".to_string(),
                vec!["col1".to_string(), "col1".to_string()],
            )]),
        };

        #[rustfmt::skip]
        assert_find_insert_pos_in_index(
            vec![
                0, 0,
                0, 1,
                0, 2,
                1, 0,
                1, 1,
                1, 2,
                3, 0,
                3, 1,
                3, 2,
            ],
            vec![2, 0],
            6,
            &table_schema
        );

        #[rustfmt::skip]
        assert_find_insert_pos_in_index(
            vec![
                0, 0,
                0, 1,
                0, 2,
                1, 0,
                1, 1,
                1, 2,
                3, 0,
                3, 1,
                3, 2,
            ],
            vec![1, 1],
            5,
            &table_schema
        );
    }

    fn assert_find_insert_pos_in_index(
        index_content: Vec<i32>,
        to_find: Vec<i32>,
        expected: usize,
        table_schema: &TableSchema,
    ) {
        let index_name = "fake_index";

        let index_bytes = &index_content
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>()[..];
        let index_values: Vec<Value> = to_find.iter().map(|v| Value::I32(*v)).collect();
        let index_values_refs: Vec<&Value> = index_values.iter().collect();

        let result =
            find_insert_pos_in_index(index_name, index_bytes, &index_values_refs, &table_schema);

        assert_eq!(expected, result);
    }
}
