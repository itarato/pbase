use std::collections::HashMap;

use crate::{
    common::Selection,
    query::JoinType,
    schema::{TableReader, TableRowIterator, TableRowPositionIterator, TableSchema},
};

pub struct MultiTableViewRowReader<'a> {
    table_bytes_map: &'a HashMap<&'a str, &'a [u8]>,
    table_schema_map: &'a HashMap<&'a str, TableSchema>,
    view_row: &'a Vec<usize>,
    tables: &'a HashMap<String, usize>,
    pub view_idx: usize,
}

impl<'a> MultiTableViewRowReader<'a> {
    /// # Panics
    ///
    /// On bad query requesting bad table name.
    #[must_use]
    pub fn table_reader(&'a self, table_name: &str) -> TableReader<'a> {
        let table_pos_idx = *self
            .tables
            .get(table_name)
            .unwrap_or_else(|| panic!("Missing table {table_name}"));
        let table_row_pos = self.view_row[table_pos_idx];

        let row_bytes_size = self.table_schema_map[table_name].row_byte_size();
        let row_bytes =
            &self.table_bytes_map[table_name][table_row_pos..table_row_pos + row_bytes_size];

        TableReader::new(&self.table_schema_map[table_name], row_bytes, table_row_pos)
    }
}

pub struct MultiTableView {
    pub view: Vec<Vec<usize>>,
    pub tables: HashMap<String, usize>,
}

impl MultiTableView {
    #[must_use]
    pub fn new_from_table_bytes_and_selection(
        table_bytes: &[u8],
        table_schema: &TableSchema,
        selection: &Selection,
    ) -> Self {
        let tables = HashMap::from([(table_schema.name.clone(), 0)]);

        let view = match selection {
            Selection::All => {
                TableRowPositionIterator::new(table_schema.row_byte_size(), table_bytes.len())
                    .map(|pos| vec![pos])
                    .collect()
            }
            Selection::List(positions) => positions.iter().map(|pos| vec![*pos]).collect(),
        };

        Self { view, tables }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.view.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.view.is_empty()
    }

    #[must_use]
    pub fn row_pos(&self, row_idx: usize, table: &str) -> usize {
        self.view[row_idx][self.tables[table]]
    }

    #[allow(clippy::too_many_arguments)]
    pub fn join(
        &mut self,
        join_type: &JoinType,
        selection: &Selection,
        lhs_table_name: &str,
        rhs_table_name: &str,
        lhs_match_field_name: &str,
        rhs_match_field_name: &str,
        table_bytes_map: &HashMap<&str, &[u8]>,
        table_schema_map: &HashMap<&str, TableSchema>,
    ) {
        match join_type {
            JoinType::Inner => self.inner_join(
                selection,
                lhs_table_name,
                rhs_table_name,
                lhs_match_field_name,
                rhs_match_field_name,
                table_bytes_map,
                table_schema_map,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn inner_join(
        &mut self,
        selection: &Selection,
        lhs_table_name: &str,
        rhs_table_name: &str,
        lhs_match_field_name: &str,
        rhs_match_field_name: &str,
        table_bytes_map: &HashMap<&str, &[u8]>,
        table_schema_map: &HashMap<&str, TableSchema>,
    ) {
        // Register new table.
        self.tables
            .insert(rhs_table_name.to_string(), self.tables.len());

        // TODO: This is O(N^2) scanning. Leverage indices.
        let lhs_table_idx = self.tables[lhs_table_name];

        // Save old view and prepare new view for insert.
        let mut old_view = vec![];
        std::mem::swap(&mut old_view, &mut self.view);
        let old_view = old_view; // Make it immutable.

        for old_view_row in &old_view {
            let lhs_row_pos = old_view_row[lhs_table_idx];
            let lhs_row_bytes = &table_bytes_map[lhs_table_name][lhs_row_pos..];
            let lhs_row_reader = TableReader::new(
                &table_schema_map[lhs_table_name],
                lhs_row_bytes,
                lhs_row_pos,
            );
            let lhs_value = lhs_row_reader.get_field_value(lhs_match_field_name);

            let rhs_table_it = TableRowIterator::new(
                &table_schema_map[rhs_table_name],
                table_bytes_map[rhs_table_name],
                selection,
            );
            for rhs_row_reader in rhs_table_it {
                let rhs_value = rhs_row_reader.get_field_value(rhs_match_field_name);

                if lhs_value == rhs_value {
                    let mut new_row = old_view_row.clone();
                    new_row.push(rhs_row_reader.absolute_pos);
                    self.view.push(new_row);
                }
            }
        }
    }

    #[must_use]
    pub const fn iter<'a>(
        &'a self,
        table_bytes_map: &'a HashMap<&'a str, &'a [u8]>,
        table_schema_map: &'a HashMap<&'a str, TableSchema>,
    ) -> MultiTableViewIterator<'a> {
        MultiTableViewIterator {
            table_bytes_map,
            table_schema_map,
            view: self,
            current_idx: 0,
        }
    }
}

pub struct MultiTableViewIterator<'a> {
    table_bytes_map: &'a HashMap<&'a str, &'a [u8]>,
    table_schema_map: &'a HashMap<&'a str, TableSchema>,
    view: &'a MultiTableView,
    current_idx: usize,
}

impl<'a> Iterator for MultiTableViewIterator<'a> {
    type Item = MultiTableViewRowReader<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx >= self.view.len() {
            None
        } else {
            let current_idx = self.current_idx;
            self.current_idx += 1;
            Some(MultiTableViewRowReader {
                table_bytes_map: self.table_bytes_map,
                table_schema_map: self.table_schema_map,
                view_row: &self.view.view[current_idx],
                tables: &self.view.tables,
                view_idx: current_idx,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indexmap::IndexMap;

    use crate::{
        query::JoinType,
        schema::{FieldSchema, TableSchema},
    };

    use super::MultiTableView;

    #[test]
    fn test_row_pos_with_an_all_selection() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([
                ("f1".to_string(), FieldSchema::I32),
                ("f2".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        };

        #[rustfmt::skip]
        let table_bytes: [u8; 16] = [
            1, 0, 0, 0,   2, 0, 0, 0, // Row 1
            3, 0, 0, 0,   4, 0, 0, 0, // Row 2
        ];

        let view = MultiTableView::new_from_table_bytes_and_selection(
            &table_bytes,
            &table_schema,
            &crate::common::Selection::All,
        );

        assert_eq!(0, view.row_pos(0, "t1"));
        assert_eq!(8, view.row_pos(1, "t1"));
    }

    #[test]
    fn test_row_pos_with_a_list_selection() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([
                ("f1".to_string(), FieldSchema::I32),
                ("f2".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        };

        #[rustfmt::skip]
        let table_bytes: [u8; 16] = [
            1, 0, 0, 0,   2, 0, 0, 0, // Row 1
            3, 0, 0, 0,   4, 0, 0, 0, // Row 2
        ];

        let view = MultiTableView::new_from_table_bytes_and_selection(
            &table_bytes,
            &table_schema,
            &crate::common::Selection::List(vec![8]),
        );

        assert_eq!(8, view.row_pos(0, "t1"));
    }

    #[test]
    fn test_multi_view_join() {
        let t1_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([("id".to_string(), FieldSchema::U8)]),
            indices: HashMap::new(),
        };
        let t1_bytes: [u8; 4] = [0, 1, 2, 3];

        let t2_schema = TableSchema {
            name: "t2".to_string(),
            fields: IndexMap::from([("t1_id".to_string(), FieldSchema::U8)]),
            indices: HashMap::new(),
        };
        let t2_bytes: [u8; 5] = [1, 2, 3, 7, 8];

        let mut view = MultiTableView::new_from_table_bytes_and_selection(
            &t1_bytes,
            &t1_schema,
            &crate::common::Selection::All,
        );
        assert_eq!(4, view.len());

        let table_bytes_map = HashMap::from([("t1", &t1_bytes[..]), ("t2", &t2_bytes[..])]);
        let table_schema_map = HashMap::from([("t1", t1_schema), ("t2", t2_schema)]);
        let join_selection = crate::common::Selection::List(vec![0, 1, /* no 2 */ 3, 4]);

        view.join(
            &JoinType::Inner,
            &join_selection,
            "t1",
            "t2",
            "id",
            "t1_id",
            &table_bytes_map,
            &table_schema_map,
        );

        assert_eq!(2, view.len());
        assert_eq!(vec![1, 0], view.view[0]);
        assert_eq!(vec![2, 1], view.view[1]);
    }
}
