use std::cmp::Ordering;

use thiserror;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum PBaseError {
    #[error("Table size is invalid")]
    InvalidTableSizeError,
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

#[derive(Debug)]
pub enum Selection {
    All,
    List(Vec<usize>), // Line byte positions (not line indices).
}

pub struct SelectionIterator<'a> {
    selection: &'a Selection,
    row_byte_len: usize,
    table_byte_len: usize,
    current_idx: usize,
}

impl<'a> SelectionIterator<'a> {
    pub fn new(
        selection: &'a Selection,
        row_byte_len: usize,
        table_byte_len: usize,
    ) -> SelectionIterator<'a> {
        SelectionIterator {
            selection,
            row_byte_len,
            table_byte_len,
            current_idx: 0,
        }
    }
}

impl<'a> Iterator for SelectionIterator<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.selection {
            Selection::All => {
                if self.current_idx >= self.table_byte_len {
                    None
                } else {
                    let previous_idx = self.current_idx;
                    self.current_idx += self.row_byte_len;
                    Some(previous_idx)
                }
            }
            Selection::List(positions) => {
                if self.current_idx >= positions.len() {
                    None
                } else {
                    self.current_idx += 1;
                    Some(positions[self.current_idx - 1])
                }
            }
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
