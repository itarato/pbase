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
        table_name: String,
    ) -> MultiTableView {
        let tables = HashMap::from([(table_name, 0)]);

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
mod test {}
