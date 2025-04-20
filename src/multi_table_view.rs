use std::collections::HashMap;

use crate::{
    common::Selection,
    schema::{TableRowPositionIterator, TableSchema},
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

    pub fn row_pos(&self, row_idx: usize, table: &str) -> usize {
        self.view[row_idx][self.tables[table]]
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indexmap::IndexMap;

    use crate::schema::{FieldSchema, TableSchema};

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
}
