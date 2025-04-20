use std::collections::HashMap;

use crate::{
    common::Selection,
    query::JoinType,
    schema::{TableReader, TableRowIterator, TableRowPositionIterator, TableSchema},
};

pub struct MultiTableView {
    pub view: Vec<Vec<usize>>,
    pub tables: HashMap<String, usize>,
}

impl MultiTableView {
    pub fn new_from_table_bytes_and_selection(
        table_bytes: &[u8],
        table_schema: &TableSchema,
        selection: &Selection,
    ) -> MultiTableView {
        let tables = HashMap::from([(table_schema.name.clone(), 0)]);

        let view = match selection {
            Selection::All => {
                TableRowPositionIterator::new(table_schema.row_byte_size(), table_bytes.len())
                    .map(|pos| vec![pos])
                    .collect()
            }
            Selection::List(positions) => positions.iter().map(|pos| vec![pos.clone()]).collect(),
        };

        MultiTableView { view, tables }
    }

    pub fn len(&self) -> usize {
        return self.view.len();
    }

    pub fn row_pos(&self, row_idx: usize, table: &str) -> usize {
        self.view[row_idx][self.tables[table]]
    }

    pub fn join(
        &mut self,
        join_type: JoinType,
        lhs_table_name: &str,
        rhs_table_name: &str,
        lhs_match_field_name: &str,
        rhs_match_field_name: &str,
        table_bytes_map: &HashMap<String, &[u8]>,
        table_schema_map: &HashMap<String, TableSchema>,
    ) {
        match join_type {
            JoinType::Inner => self.inner_join(
                lhs_table_name,
                rhs_table_name,
                lhs_match_field_name,
                rhs_match_field_name,
                table_bytes_map,
                table_schema_map,
            ),
        }
    }

    fn inner_join(
        &mut self,
        lhs_table_name: &str,
        rhs_table_name: &str,
        lhs_match_field_name: &str,
        rhs_match_field_name: &str,
        table_bytes_map: &HashMap<String, &[u8]>,
        table_schema_map: &HashMap<String, TableSchema>,
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

        for view_row_idx in 0..old_view.len() {
            let lhs_row_pos = old_view[view_row_idx][lhs_table_idx];
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
            );
            for rhs_row_reader in rhs_table_it {
                let rhs_value = rhs_row_reader.get_field_value(rhs_match_field_name);

                if lhs_value == rhs_value {
                    let mut new_row = old_view[view_row_idx].clone();
                    new_row.push(rhs_row_reader.absolute_pos);
                    self.view.push(new_row);
                }
            }
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
        let t2_bytes: [u8; 4] = [1, 2, 7, 8];

        let mut view = MultiTableView::new_from_table_bytes_and_selection(
            &t1_bytes,
            &t1_schema,
            &crate::common::Selection::All,
        );
        assert_eq!(4, view.len());

        let table_bytes_map = HashMap::from([
            ("t1".to_string(), &t1_bytes[..]),
            ("t2".to_string(), &t2_bytes[..]),
        ]);
        let table_schema_map =
            HashMap::from([("t1".to_string(), t1_schema), ("t2".to_string(), t2_schema)]);

        view.join(
            JoinType::Inner,
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
