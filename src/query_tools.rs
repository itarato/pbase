use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

use log::debug;
use memmap::Mmap;

use crate::{
    common::{
        binary_narrow_to_lower_range_exclusive, binary_narrow_to_range_exclusive,
        binary_narrow_to_upper_range_exclusive, Error, Selection, SelectionIterator,
    },
    multi_table_view::MultiTableView,
    query::{FieldSelector, FilterSource, RowFilter, SelectQuery},
    schema::{TablePtrType, TableSchema, TABLE_PTR_BYTE_SIZE},
    table_opener::TableOpener,
    value::Value,
};

pub struct SelectQueryExecutor<'a> {
    table_opener: &'a TableOpener,
    query: SelectQuery,
}

impl<'a> SelectQueryExecutor<'a> {
    #[must_use]
    pub const fn new(table_opener: &'a TableOpener, query: SelectQuery) -> Self {
        Self {
            table_opener,
            query,
        }
    }

    /// # Errors
    ///
    /// Errors on file operations.
    pub fn call(&self) -> Result<Vec<HashMap<String, Value>>, Error> {
        let table_schemas = self.collect_table_schemas_from_query()?;

        // Preloading memory mapped table files for main table and all join tables.
        let table_bytes_mmap_map: HashMap<&str, Mmap> = self.collect_table_bytes_map()?;
        let table_bytes_map: HashMap<&str, &[u8]> = table_bytes_mmap_map
            .iter()
            .map(|(k, v)| (*k, &v[..]))
            .collect();

        // Reducing table search spaces using single table filters.
        let mut selections: HashMap<&str, Selection> = HashMap::new();
        selections.insert(
            self.query.from.as_str(),
            self.execute_filters_on_single_tables(
                table_bytes_map[self.query.from.as_str()],
                &table_schemas[self.query.from.as_str()],
            )?,
        );
        for join_contract in &self.query.joins {
            selections.insert(
                self.query.from.as_str(),
                self.execute_filters_on_single_tables(
                    table_bytes_map[join_contract.rhs.source.as_str()],
                    &table_schemas[join_contract.rhs.source.as_str()],
                )?,
            );
        }

        // Compile joined view. (Assuming we will need all to present/filter.)
        let multi_table_view =
            self.generate_multi_table_view(&selections, &table_bytes_map, &table_schemas);

        // TODO: execute multi-table filters.

        // Materialize the selection and return.
        Ok(self.materialize_view(multi_table_view, &table_bytes_map, &table_schemas))
    }

    fn generate_multi_table_view(
        &self,
        selections: &HashMap<&str, Selection>,
        table_bytes_map: &HashMap<&str, &[u8]>,
        table_schema_map: &HashMap<&str, TableSchema>,
    ) -> MultiTableView {
        let mut view = MultiTableView::new_from_table_bytes_and_selection(
            table_bytes_map[self.query.from.as_str()],
            &table_schema_map[self.query.from.as_str()],
            &selections[self.query.from.as_str()],
        );

        for join_contract in &self.query.joins {
            view.join(
                &join_contract.join_type,
                &selections[join_contract.rhs.source.as_str()],
                &join_contract.lhs.source,
                &join_contract.rhs.source,
                &join_contract.lhs.name,
                &join_contract.rhs.name,
                table_bytes_map,
                table_schema_map,
            );
        }

        view
    }

    //
    // Applies all single table filters on a table and returns the selection.
    //
    fn execute_filters_on_single_tables(
        &self,
        table_bytes: &[u8],
        table_schema: &TableSchema,
    ) -> Result<Selection, Error> {
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

        let mut filters_left: Vec<&RowFilter> = self
            .query
            .filters
            .iter()
            .filter(|row_filter| {
                match row_filter.filter_source() {
                    // Ignore other table and multi table filters.
                    FilterSource::Single(table_name) => table_name == table_schema.name,
                    FilterSource::Multi(_, _) => false,
                }
            })
            .collect();

        // Establish current subset.
        let filter_fields: HashSet<&String> = filters_left
            .iter()
            .map(|row_filter| &row_filter.field.name)
            .collect();

        if let Some(index_name) = index_for_query(table_schema, &filter_fields) {
            debug!("Using index: {}", &index_name);

            // Index lookup.
            selection = self.index_filter(&index_name, &filters_left, table_schema)?;
            debug!("Index filter result selection: {:?}", &selection);

            let index_fields = &table_schema.indices[&index_name];
            filters_left.retain(|filter| !index_fields.contains(&filter.field.name));
        } else {
            debug!("No index found");
        }

        // Linear scan the rest.
        if !filters_left.is_empty() {
            selection = self.scan_filter(selection, &filters_left, table_bytes, table_schema);
        }

        Ok(selection)
    }

    fn collect_table_schemas_from_query(&self) -> Result<HashMap<&str, TableSchema>, Error> {
        let mut table_schemas = HashMap::new();

        // Main table schema.
        table_schemas.insert(
            self.query.from.as_str(),
            self.table_opener.open_schema(&self.query.from)?,
        );

        // Join table schemas.
        for join_contract in &self.query.joins {
            table_schemas.insert(
                join_contract.rhs.source.as_str(),
                self.table_opener.open_schema(&join_contract.rhs.source)?,
            );
        }

        Ok(table_schemas)
    }

    fn collect_table_bytes_map(&self) -> Result<HashMap<&str, Mmap>, Error> {
        let mut table_bytes_map: HashMap<&str, Mmap> = HashMap::new();
        table_bytes_map.insert(
            self.query.from.as_str(),
            self.table_opener.table_mmap(&self.query.from)?,
        );
        for join_contract in &self.query.joins {
            table_bytes_map.insert(
                &join_contract.rhs.source,
                self.table_opener.table_mmap(&join_contract.rhs.source)?,
            );
        }

        Ok(table_bytes_map)
    }

    fn index_filter(
        &self,
        index_name: &str,
        filters_left: &Vec<&RowFilter>,
        table_schema: &TableSchema,
    ) -> Result<Selection, Error> {
        let index_row_byte_len = table_schema.index_row_byte_size(index_name);
        let index_mmap = self.table_opener.index_mmap(table_schema, index_name)?;
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
                        lhs_idx = binary_narrow_to_upper_range_exclusive(lhs_idx, rhs_idx, |i| {
                            let index_row_pos = index_row_byte_len * i as usize;
                            let index_value_pos = index_row_pos + index_field_byte_pos;
                            let index_value = index_field_schema
                                .value_from_bytes(&index_bytes[index_value_pos..]);

                            index_value.cmp(&filter.rhs)
                        });
                    }
                    Ordering::Less => {
                        rhs_idx = binary_narrow_to_lower_range_exclusive(lhs_idx, rhs_idx, |i| {
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

        debug!("Index narrowing result range: ({}..{})", lhs_idx, rhs_idx);

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

    //
    // Filters a selection on a table using row-fitlers line by line (no index use).
    //
    fn scan_filter(
        &self,
        current_selection: Selection,
        filters: &Vec<&RowFilter>,
        table_bytes: &[u8],
        table_schema: &TableSchema,
    ) -> Selection {
        let table_byte_len = table_bytes.len();
        let row_byte_len = table_schema.row_byte_size();
        if table_byte_len % row_byte_len != 0 {
            panic!(
                "Invalid table size. Table byte size ({}) is not multiple of row byte size ({}).",
                table_byte_len, row_byte_len
            );
        }

        assert!(filters.len() > 0);

        let selection_it = SelectionIterator::new(&current_selection, row_byte_len, table_byte_len);
        let mut filtered_positions = vec![];
        for pos in selection_it {
            let row_bytes = &table_bytes[pos..pos + row_byte_len];

            // We need to go through all filters.
            for filter in filters {
                if filter.field.source != table_schema.name {
                    panic!(
                        "Wrong filter. Table: {} Filter source: {}",
                        table_schema.name, filter.field.source
                    );
                }

                // Skip if not match.
                let filter_field_pos = table_schema.field_byte_pos(&filter.field.name);
                let field_schema = &table_schema.fields[&filter.field.name];
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

    fn materialize_view(
        &self,
        view: MultiTableView,
        table_bytes_map: &HashMap<&str, &[u8]>,
        table_schema_map: &HashMap<&str, TableSchema>,
    ) -> Vec<HashMap<String, Value>> {
        let mut out = vec![];

        // Collecting output fields.
        let mut output_fields = vec![];
        for main_table_field in table_schema_map[self.query.from.as_str()].fields.keys() {
            output_fields.push(FieldSelector {
                name: main_table_field.clone(),
                source: self.query.from.clone(),
            });
        }

        for join_contract in &self.query.joins {
            for join_field in table_schema_map[join_contract.rhs.source.as_str()]
                .fields
                .keys()
            {
                output_fields.push(FieldSelector {
                    name: join_contract.rhs.source.clone(),
                    source: join_field.clone(),
                });
            }
        }

        for view_reader in view.iter(&table_bytes_map, &table_schema_map) {
            let mut out_row = HashMap::new();
            for output_field in &output_fields {
                let table_reader = view_reader.table_reader(&output_field.source);
                let value = table_reader.get_field_value(&output_field.name);
                out_row.insert(output_field.full_name(), value);
            }

            out.push(out_row);
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
    use std::hash::{DefaultHasher, Hash, Hasher};

    use indexmap::IndexMap;

    use crate::query_tools::{find_insert_pos_in_index, index_score, FilterSource};
    use crate::schema::{FieldSchema, TableSchema};
    use crate::value::Value;

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

    #[test]
    fn test_single_filter_source_hash_works() {
        let mut hasher1 = DefaultHasher::new();
        FilterSource::new_single("abc".into()).hash(&mut hasher1);

        let mut hasher2 = DefaultHasher::new();
        FilterSource::new_single("abc".into()).hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_multi_filter_source_hash_works() {
        let mut hasher1 = DefaultHasher::new();
        FilterSource::new_multi("abc".into(), "def".into()).hash(&mut hasher1);

        let mut hasher2 = DefaultHasher::new();
        FilterSource::new_multi("def".into(), "abc".into()).hash(&mut hasher2);

        assert_eq!(hasher1.finish(), hasher2.finish());
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
