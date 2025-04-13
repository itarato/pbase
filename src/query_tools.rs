use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use crate::{
    common::*,
    query::{RowFilter, SelectQuery},
    schema::{TablePtrType, TableSchema, TABLE_PTR_BYTE_SIZE},
    table_opener::TableOpener,
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

pub struct SelectQueryExecutor<'a> {
    table_opener: &'a TableOpener,
    query: SelectQuery,
}

impl<'a> SelectQueryExecutor<'a> {
    pub fn new(table_opener: &'a TableOpener, query: SelectQuery) -> SelectQueryExecutor<'a> {
        SelectQueryExecutor {
            table_opener,
            query,
        }
    }

    pub fn call(&self) -> Result<Vec<HashMap<String, Value>>, Error> {
        let mut table_schemas = HashMap::new();
        let table_schema: TableSchema = self.table_opener.open_schema(&self.query.from)?;
        table_schemas.insert(self.query.from.clone(), &table_schema);

        let table_file_mmap = self.table_opener.table_mmap(&self.query.from)?;
        let table_bytes = &table_file_mmap[..];

        let mut selection = Selection::All;

        // TODO: Greedy algorithm for index selection might not be the best.
        // Example:
        //  - Indices:
        //      - A, A+B, B+C+D+E
        //  - Filters: A, B, C, D, E
        //  - Greedy index selection: A+B then nothing
        //  - Better index selection: A then B+C+D+E

        // NOTE: We're only doing a single index filter (if there is one change at least).
        // There might be a better performance to evaluate more than one and using a crossecton of the pos-list results. Later.

        let mut filters_left = self.query.filters.clone();

        // Establish current subset
        let filter_fields: HashSet<&String> = filters_left
            .iter()
            .filter_map(|row_filter| {
                if row_filter.field.source != self.query.from {
                    unimplemented!("Row filter and select table does not match.");
                }

                Some(&row_filter.field.name)
            })
            .collect();

        if let Some(index_name) = index_for_query(&table_schemas[&self.query.from], &filter_fields)
        {
            // Index lookup.
            selection =
                self.index_filter(&index_name, &filters_left, &table_schemas[&self.query.from])?;

            let index_fields = &table_schema.indices[&index_name];
            filters_left = filters_left
                .into_iter()
                .filter_map(|filter| {
                    if index_fields.contains(&filter.field.name) {
                        None
                    } else {
                        Some(filter)
                    }
                })
                .collect();
        }

        // Linear scan the rest.
        if !filters_left.is_empty() {
            selection = self.scan_filter(selection, &filters_left, &table_bytes, &table_schemas);
        }

        // Materialize the selection and return.
        Ok(self.materialize(selection, &table_bytes, table_schemas))
    }

    fn index_filter(
        &self,
        index_name: &str,
        filters_left: &Vec<RowFilter>,
        table_schema: &TableSchema,
    ) -> Result<Selection, Error> {
        let index_row_byte_len = table_schema.index_row_byte_size(index_name);
        let index_mmap = self.table_opener.index_mmap(&table_schema, &index_name)?;
        let index_bytes = &index_mmap[..];
        let index_fields = &table_schema.indices[index_name];

        let mut filter_by_field_map: HashMap<&String, Vec<&RowFilter>> = HashMap::new();
        for filter in filters_left {
            filter_by_field_map
                .entry(&filter.field.name)
                .or_insert(vec![])
                .push(filter);
        }

        // 1 / 2:
        // Get filter fields
        // Get index fields
        // Get crossection ordered
        // Iterate the crossection in order
        // Narrow down the index ranges
        let mut lhs_idx = -1i32; // Line index.
        let mut rhs_idx = (index_bytes.len() / index_row_byte_len) as i32; // Line index.
        for index_field in index_fields {
            if !filter_by_field_map.contains_key(index_field) {
                // No more filters to leverage the index columns.
                break;
            }

            let index_field_byte_pos = table_schema.index_field_byte_pos(index_name, index_field);
            let index_field_schema = &table_schema.fields[index_field];

            for filter in &filter_by_field_map[index_field] {
                // Narrow the range.
                match filter.op {
                    Ordering::Equal => {
                        (lhs_idx, rhs_idx) =
                            binary_narrow_to_range_exclusive(lhs_idx, rhs_idx, |i| {
                                let index_row_pos = index_row_byte_len * i as usize;
                                let index_value_pos = index_row_pos + index_field_byte_pos;
                                let index_value = index_field_schema
                                    .value_from_bytes(&index_bytes[index_value_pos..]);

                                index_value.cmp(&filter.rhs)
                            });
                    }
                    Ordering::Greater => {
                        rhs_idx = binary_narrow_to_upper_range_exclusive(lhs_idx, rhs_idx, |i| {
                            let index_row_pos = index_row_byte_len * i as usize;
                            let index_value_pos = index_row_pos + index_field_byte_pos;
                            let index_value = index_field_schema
                                .value_from_bytes(&index_bytes[index_value_pos..]);

                            index_value.cmp(&filter.rhs)
                        });
                    }
                    Ordering::Less => {
                        lhs_idx = binary_narrow_to_lower_range_exclusive(lhs_idx, rhs_idx, |i| {
                            let index_row_pos = index_row_byte_len * i as usize;
                            let index_value_pos = index_row_pos + index_field_byte_pos;
                            let index_value = index_field_schema
                                .value_from_bytes(&index_bytes[index_value_pos..]);

                            index_value.cmp(&filter.rhs)
                        });
                    }
                };
            }
        }

        // 3:
        // Collect positions from final range.
        let mut i = lhs_idx + 1;
        let mut out_positions = vec![];
        while i < rhs_idx {
            let index_row_pos = i as usize * index_row_byte_len;
            let index_row_row_idx_field_pos =
                index_row_pos + table_schema.index_row_ptr_field_byte_pos(index_name);

            let table_row_ptr = TablePtrType::from_le_bytes(
                index_bytes[index_row_row_idx_field_pos
                    ..index_row_row_idx_field_pos + TABLE_PTR_BYTE_SIZE]
                    .try_into()?,
            );
            out_positions.push(table_row_ptr as usize);

            i += 1;
        }

        // 4:
        // Return.
        Ok(Selection::List(out_positions))
    }

    fn scan_filter(
        &self,
        current_selection: Selection,
        filters_left: &Vec<RowFilter>,
        table_bytes: &[u8],
        table_schemas: &HashMap<String, &TableSchema>,
    ) -> Selection {
        let table_byte_len = table_bytes.len();
        let row_byte_len = table_schemas[&self.query.from].row_byte_size();
        if table_byte_len % row_byte_len != 0 {
            panic!(
                "Invalid table size. Table byte size ({}) is not multiple of row byte size ({}).",
                table_byte_len, row_byte_len
            );
        }

        assert!(filters_left.len() > 0);

        let selection_it = SelectionIterator::new(&current_selection, row_byte_len, table_byte_len);
        let mut filtered_positions = vec![];
        for pos in selection_it {
            let row_bytes = &table_bytes[pos..pos + row_byte_len];

            // We need to go through all filters.
            for filter in filters_left {
                // Skip if not match.
                let filter_field_pos =
                    table_schemas[&filter.field.source].field_byte_pos(&filter.field.name);
                let field_schema = &table_schemas[&filter.field.source].fields[&filter.field.name];
                let value = field_schema.value_from_bytes(&row_bytes[filter_field_pos..]);
                let is_satisfy = value.cmp(&filter.rhs) == filter.op;

                if is_satisfy {
                    // Add to filtered positions.
                    filtered_positions.push(pos);
                }
            }
        }

        Selection::List(filtered_positions)
    }

    fn materialize(
        &self,
        selection: Selection,
        table_bytes: &[u8],
        table_schemas: HashMap<String, &TableSchema>,
    ) -> Vec<HashMap<String, Value>> {
        let mut out = vec![];

        let table_byte_len = table_bytes.len();
        let row_byte_len = table_schemas[&self.query.from].row_byte_size();
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
                        &table_schemas[&self.query.from],
                    );
                    out.push(row);

                    pos += row_byte_len;
                }
            }
            Selection::List(positions) => {
                for pos in positions {
                    let row = parse_row_bytes(
                        &table_bytes[pos..pos + row_byte_len],
                        &table_schemas[&self.query.from],
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

        (lhs_idx, rhs_idx) = binary_narrow_to_range_exclusive(lhs_idx, rhs_idx, |i| {
            let value_bytes_pos = i as usize * index_row_size + field_byte_pos;
            let value = field_schema.value_from_bytes(&index_bytes[value_bytes_pos..]);
            value.cmp(cmp_value)
        });

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
