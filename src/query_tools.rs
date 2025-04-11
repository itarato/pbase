use std::collections::{HashMap, HashSet};

use crate::{
    common::{parse_row_bytes, Value},
    query::{RowFilter, SelectQuery},
    schema::TableSchema,
};

enum Selection {
    All,
    List(Vec<usize>), // Line byte positions (not line indices).
}

struct SelectionIterator<'a> {
    selection: &'a Selection,
    row_byte_len: usize,
    table_byte_len: usize,
    current_idx: usize,
}

impl<'a> SelectionIterator<'a> {
    fn new(
        selection: &'a Selection,
        row_byte_len: usize,
        table_byte_len: usize,
    ) -> SelectionIterator {
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

pub struct SelectQueryExecutor;

impl SelectQueryExecutor {
    pub fn call(
        table_bytes: &[u8],
        select_query: SelectQuery,
        table_schemas: HashMap<String, TableSchema>,
    ) -> Vec<HashMap<String, Value>> {
        let mut current_selection = Selection::All;

        let filters_left = select_query.filters.clone();

        while !filters_left.is_empty() {
            // TODO: Greedy algorithm for index selection might not be the best.
            // Example:
            //  - Indices:
            //      - A, A+B, B+C+D+E
            //  - Filters: A, B, C, D, E
            //  - Greedy index selection: A+B then nothing
            //  - Better index selection: A then B+C+D+E

            // Establish current subset
            let filter_fields: HashSet<&String> = filters_left
                .iter()
                .filter_map(|row_filter| {
                    if row_filter.field.source != select_query.from {
                        unimplemented!("Row filter and select table does not match.");
                    }

                    Some(&row_filter.field.name)
                })
                .collect();

            if let Some(possible_index) =
                index_for_query(&table_schemas[&select_query.from], &filter_fields)
            {
                // Index lookup.
                // Result: list of row index
                // Remove filters from current-filter-set
                // Update current selection
                // continue (to next iteration)
                unimplemented!()
            } else {
                // No more index left. Linear scan needed.
                // Use all filters
                // Result: list of rows
                // -> return?
                current_selection = SelectQueryExecutor::filter(
                    current_selection,
                    &filters_left,
                    &table_bytes,
                    &table_schemas,
                    &select_query,
                );
                unimplemented!()
            }
        }

        // Materialize the selection and return.
        SelectQueryExecutor::materialize(
            current_selection,
            &table_bytes,
            table_schemas,
            select_query,
        )
    }

    fn filter(
        current_selection: Selection,
        filters_left: &Vec<RowFilter>,
        table_bytes: &[u8],
        table_schemas: &HashMap<String, TableSchema>,
        select_query: &SelectQuery,
    ) -> Selection {
        let table_byte_len = table_bytes.len();
        let row_byte_len = table_schemas[&select_query.from].row_byte_size();
        if table_byte_len % row_byte_len != 0 {
            panic!(
                "Invalid table size. Table byte size ({}) is not multiple of row byte size ({}).",
                table_byte_len, row_byte_len
            );
        }

        unimplemented!()
    }

    fn materialize(
        selection: Selection,
        table_bytes: &[u8],
        table_schemas: HashMap<String, TableSchema>,
        select_query: SelectQuery,
    ) -> Vec<HashMap<String, Value>> {
        let mut out = vec![];

        let table_byte_len = table_bytes.len();
        let row_byte_len = table_schemas[&select_query.from].row_byte_size();
        if table_byte_len % row_byte_len != 0 {
            panic!(
                "Invalid table size. Table byte size ({}) is not multiple of row byte size ({}).",
                table_byte_len, row_byte_len
            );
        }

        match selection {
            Selection::All => {
                let mut pos = 0usize;
                while pos < table_byte_len {
                    let row = parse_row_bytes(
                        &table_bytes[pos..pos + row_byte_len],
                        &table_schemas[&select_query.from],
                    );
                    out.push(row);

                    pos += row_byte_len;
                }
            }
            Selection::List(positions) => {
                for pos in positions {
                    let row = parse_row_bytes(
                        &table_bytes[pos..pos + row_byte_len],
                        &table_schemas[&select_query.from],
                    );
                    out.push(row);
                }
            }
        }

        out
    }
}

pub fn index_for_query(
    table_schema: &TableSchema,
    filter_fields: &HashSet<&String>,
) -> Option<String> {
    let ref available_indices = table_schema.indices;

    let mut best_index_name: Option<String> = None;
    let mut best_index_score = 0i32;

    for (index_name, index_fields) in available_indices {
        let index_score = index_score(index_fields, &filter_fields);

        if index_score > best_index_score {
            best_index_score = index_score;
            best_index_name = Some(index_name.clone());
        }
    }

    best_index_name
}

pub fn index_score(index_fields: &Vec<String>, filter_fields: &HashSet<&String>) -> i32 {
    let mut score = 0i32;

    for index_field in index_fields {
        if filter_fields.contains(index_field) {
            score += 1;
        } else {
            break;
        }
    }

    score
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
    use std::collections::{HashMap, HashSet};

    use indexmap::IndexMap;

    use crate::common::Value;
    use crate::query_tools::{find_insert_pos_in_index, index_score};
    use crate::schema::{FieldSchema, TableSchema};

    use super::index_for_query;

    #[test]
    fn test_index_score() {
        assert_eq!(0, index_score(&vec![], &HashSet::new()));

        assert_eq!(0, index_score(&vec![], &HashSet::from([&"A".to_string()])));

        assert_eq!(
            0,
            index_score(&vec!["A".to_string()], &HashSet::from([&"B".to_string()]))
        );

        assert_eq!(
            1,
            index_score(
                &vec!["A".to_string()],
                &HashSet::from([&"B".to_string(), &"A".to_string()])
            )
        );

        assert_eq!(
            2,
            index_score(
                &vec!["A".to_string(), "C".to_string()],
                &HashSet::from([&"B".to_string(), &"A".to_string(), &"C".to_string()])
            )
        );
    }

    #[test]
    fn test_index_for_query() {
        let table_schema = TableSchema {
            name: "example".to_string(),
            fields: IndexMap::from([
                ("A".to_string(), FieldSchema::I32),
                ("B".to_string(), FieldSchema::I32),
                ("C".to_string(), FieldSchema::I32),
                ("D".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::from([
                ("index1".to_string(), vec!["A".to_string(), "B".to_string()]),
                (
                    "index2".to_string(),
                    vec!["B".to_string(), "C".to_string(), "D".to_string()],
                ),
            ]),
        };

        let index_name = index_for_query(
            &table_schema,
            &HashSet::from([&"A".to_string(), &"B".to_string()]),
        );
        assert_eq!(Some("index1".to_string()), index_name);
    }

    #[test]
    fn test_find_insert_pos_in_index_single_field_index() {
        let table_schema = TableSchema {
            name: "fake_table".to_string(),
            fields: IndexMap::from([("col1".to_string(), FieldSchema::I32)]),
            indices: HashMap::from([("fake_index".to_string(), vec!["col1".to_string()])]),
        };

        assert_find_insert_pos_in_index(
            vec![[0], [0], [1], [1], [3], [3]],
            vec![2],
            4,
            &table_schema,
        );
        assert_find_insert_pos_in_index(
            vec![[0], [0], [1], [1], [3], [3]],
            vec![1],
            4,
            &table_schema,
        );
        assert_find_insert_pos_in_index(
            vec![[0], [0], [1], [1], [3], [3]],
            vec![0],
            2,
            &table_schema,
        );
        assert_find_insert_pos_in_index(
            vec![[0], [0], [1], [1], [3], [3]],
            vec![3],
            6,
            &table_schema,
        );
        assert_find_insert_pos_in_index(
            vec![[0], [0], [1], [1], [3], [3]],
            vec![-1],
            0,
            &table_schema,
        );
        assert_find_insert_pos_in_index(
            vec![[0], [0], [1], [1], [3], [3]],
            vec![4],
            6,
            &table_schema,
        );
    }

    #[test]
    fn test_find_insert_pos_in_index_multi_field_index() {
        let table_schema = TableSchema {
            name: "fake_table".to_string(),
            fields: IndexMap::from([
                ("col1".to_string(), FieldSchema::I32),
                ("col2".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::from([(
                "fake_index".to_string(),
                vec!["col1".to_string(), "col2".to_string()],
            )]),
        };

        #[rustfmt::skip]
        assert_find_insert_pos_in_index(
            vec![
                [0, 0],
                [0, 1],
                [0, 2],
                [1, 0],
                [1, 1],
                [1, 2],
                [3, 0],
                [3, 1],
                [3, 2],
            ],
            vec![2, 0],
            6,
            &table_schema
        );

        #[rustfmt::skip]
        assert_find_insert_pos_in_index(
            vec![
                [0, 0],
                [0, 1],
                [0, 2],
                [1, 0],
                [1, 1],
                [1, 2],
                [3, 0],
                [3, 1],
                [3, 2],
            ],
            vec![1, 1],
            5,
            &table_schema
        );
    }

    fn assert_find_insert_pos_in_index<const INDEX_LEN: usize>(
        index_content: Vec<[i32; INDEX_LEN]>,
        to_find: Vec<i32>,
        expected: usize,
        table_schema: &TableSchema,
    ) {
        let index_name = "fake_index";

        let index_bytes = &index_content
            .iter()
            .flat_map(|i32_vals| {
                let mut as_vec = i32_vals.to_vec();
                // The two i32 mimics the row index. We don't use them so it's just a padding.
                as_vec.push(0);
                as_vec.push(0);

                as_vec
            })
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();

        let index_bytes = &index_bytes[..];
        let index_values: Vec<Value> = to_find.iter().map(|v| Value::I32(*v)).collect();
        let index_values_refs: Vec<&Value> = index_values.iter().collect();

        let result =
            find_insert_pos_in_index(index_name, index_bytes, &index_values_refs, &table_schema);

        assert_eq!(expected, result);
    }
}
