use std::{cmp::Ordering, collections::HashMap};

use thiserror;

use crate::{schema::TableSchema, value::Value};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {
    #[error("Table size is invalid")]
    InvalidTableSizeError,
}

pub fn parse_row_bytes(bytes: &[u8], schema: &TableSchema) -> HashMap<String, Value> {
    let mut out = HashMap::new();

    let mut pos = 0usize;
    for (field_name, field_schema) in &schema.fields {
        out.insert(
            field_name.clone(),
            field_schema.value_from_bytes(&bytes[pos..]),
        );
        pos += field_schema.byte_size();
    }

    out
}

///
/// Finds position around (exclusive) a range.
///
/// Example:
/// ------------XXXXX-----
///            ^     ^
///
pub fn binary_narrow_to_range_exclusive<F>(lhs: i32, rhs: i32, pred: F) -> (i32, i32)
where
    F: Fn(i32) -> Ordering,
{
    let mut i = lhs;
    let mut j = rhs;

    let lhs_out = loop {
        if i + 1 >= j {
            break i;
        }

        let mid = (i + j) / 2;
        if pred(mid) == Ordering::Less {
            i = mid;
        } else {
            j = mid;
        }
    };

    j = rhs;
    let rhs_out = loop {
        if i + 1 >= j {
            break j;
        }

        let mid = (i + j) / 2;
        if pred(mid) == Ordering::Greater {
            j = mid;
        } else {
            i = mid;
        }
    };

    (lhs_out, rhs_out)
}

///
/// Finds position right before the upper range.
///
/// Example:
/// ------------XXXXX
///            ^
///
pub fn binary_narrow_to_upper_range_exclusive<F>(lhs: i32, rhs: i32, pred: F) -> i32
where
    F: Fn(i32) -> Ordering,
{
    let mut i = lhs;
    let mut j = rhs;

    loop {
        if i + 1 >= j {
            break i;
        }

        let mid = (i + j) / 2;
        if pred(mid) == Ordering::Greater {
            j = mid;
        } else {
            i = mid;
        }
    }
}

///
/// Finds position right after the lower range.
///
/// Example:
/// XXXXXXX---------
///        ^
///
pub fn binary_narrow_to_lower_range_exclusive<F>(lhs: i32, rhs: i32, pred: F) -> i32
where
    F: Fn(i32) -> Ordering,
{
    let mut i = lhs;
    let mut j = rhs;

    loop {
        if i + 1 >= j {
            break j;
        }

        let mid = (i + j) / 2;
        if pred(mid) == Ordering::Less {
            i = mid;
        } else {
            j = mid;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_binary_narrow_to_range_exclusive() {
        let list = vec![0, 0, 0, 1, 1, 1, 3, 3, 3];

        assert_eq!(
            (-1, 0),
            binary_narrow_to_range_exclusive(-1, list.len() as i32, |i| list[i as usize].cmp(&-10))
        );
        assert_eq!(
            (-1, 3),
            binary_narrow_to_range_exclusive(-1, list.len() as i32, |i| list[i as usize].cmp(&0))
        );
        assert_eq!(
            (2, 6),
            binary_narrow_to_range_exclusive(-1, list.len() as i32, |i| list[i as usize].cmp(&1))
        );
        assert_eq!(
            (5, 6),
            binary_narrow_to_range_exclusive(-1, list.len() as i32, |i| list[i as usize].cmp(&2))
        );
        assert_eq!(
            (5, 9),
            binary_narrow_to_range_exclusive(-1, list.len() as i32, |i| list[i as usize].cmp(&3))
        );
        assert_eq!(
            (8, 9),
            binary_narrow_to_range_exclusive(-1, list.len() as i32, |i| list[i as usize].cmp(&10))
        );
    }

    #[test]
    fn test_binary_narrow_to_upper_range_exclusive() {
        let list = vec![0, 0, 0, 1, 1, 1, 3, 3, 3];

        assert_eq!(
            -1,
            binary_narrow_to_upper_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&-10))
        );
        assert_eq!(
            2,
            binary_narrow_to_upper_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&0))
        );
        assert_eq!(
            5,
            binary_narrow_to_upper_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&1))
        );
        assert_eq!(
            5,
            binary_narrow_to_upper_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&2))
        );
        assert_eq!(
            8,
            binary_narrow_to_upper_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&3))
        );
        assert_eq!(
            8,
            binary_narrow_to_upper_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&10))
        );
    }

    #[test]
    fn test_binary_narrow_to_lower_range_exclusive() {
        let list = vec![0, 0, 0, 1, 1, 1, 3, 3, 3];

        assert_eq!(
            0,
            binary_narrow_to_lower_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&-10))
        );
        assert_eq!(
            0,
            binary_narrow_to_lower_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&0))
        );
        assert_eq!(
            3,
            binary_narrow_to_lower_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&1))
        );
        assert_eq!(
            6,
            binary_narrow_to_lower_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&2))
        );
        assert_eq!(
            6,
            binary_narrow_to_lower_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&3))
        );
        assert_eq!(
            9,
            binary_narrow_to_lower_range_exclusive(-1, list.len() as i32, |i| list[i as usize]
                .cmp(&10))
        );
    }
}
