use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{common::Selection, value::Value};

pub type TablePtrType = u64;
pub const TABLE_PTR_BYTE_SIZE: usize = std::mem::size_of::<TablePtrType>();

#[derive(Debug, Serialize, Deserialize)]
pub enum FieldSchema {
    U8,
    I32,
}

impl FieldSchema {
    pub fn byte_size(&self) -> usize {
        match self {
            FieldSchema::U8 => 1,
            FieldSchema::I32 => 4,
        }
    }

    pub fn value_from_bytes(&self, bytes: &[u8]) -> Value {
        let value_bytes = &bytes[0..self.byte_size()];
        match self {
            FieldSchema::U8 => Value::U8(value_bytes[0]),
            FieldSchema::I32 => {
                let value = i32::from_le_bytes(
                    value_bytes.try_into().expect("slice with incorrect length"),
                );
                Value::I32(value)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub fields: IndexMap<String, FieldSchema>,
    pub indices: HashMap<String, Vec<String>>,
}

impl TableSchema {
    pub fn row_byte_size(&self) -> usize {
        self.fields
            .values()
            .map(|field_schema| field_schema.byte_size())
            .sum()
    }

    pub fn field_byte_pos(&self, field_name: &str) -> usize {
        let mut pos = 0usize;
        for (schema_field_name, field_schema) in &self.fields {
            if schema_field_name == field_name {
                return pos;
            }

            pos += field_schema.byte_size();
        }

        panic!("Field '{}' not found in table '{}'", field_name, self.name)
    }

    pub fn index_row_byte_size(&self, index_name: &str) -> usize {
        if !self.indices.contains_key(index_name) {
            panic!("Index {} not found in table {}", index_name, self.name);
        }

        let fields_total_byte_len: usize = self.indices[index_name]
            .iter()
            .map(|index_field_name| self.fields[index_field_name].byte_size())
            .sum();

        fields_total_byte_len + TABLE_PTR_BYTE_SIZE
    }

    pub fn data_row_to_bytes(&self, values: &HashMap<String, Value>) -> Vec<u8> {
        let mut bytes = vec![];
        bytes.resize(self.row_byte_size(), 0u8);

        for (field_name, field_value) in values {
            field_value.copy_bytes_to(&mut bytes[self.field_byte_pos(field_name)..]);
        }

        bytes
    }

    pub fn index_row_to_bytes(
        &self,
        index_name: &str,
        values: &HashMap<String, Value>,
        row_ptr: TablePtrType,
    ) -> Vec<u8> {
        let mut out = vec![];
        out.resize(self.index_row_byte_size(index_name), 0u8);

        let mut pos = 0usize;
        for index_field in &self.indices[index_name] {
            let field_byte_size = self.fields[index_field].byte_size();
            if let Some(value) = values.get(index_field) {
                value.copy_bytes_to(&mut out[pos..]);
            }

            pos += field_byte_size;
        }

        out[pos..pos + TABLE_PTR_BYTE_SIZE].copy_from_slice(&row_ptr.to_le_bytes());

        out
    }

    pub fn index_field_byte_pos(&self, index_name: &str, index_field: &str) -> usize {
        let mut pos = 0usize;
        for field in &self.indices[index_name] {
            if field == index_field {
                return pos;
            }

            pos += self.fields[field].byte_size();
        }

        unreachable!()
    }

    pub fn index_row_ptr_field_byte_pos(&self, index_name: &str) -> usize {
        self.index_row_byte_size(index_name) - TABLE_PTR_BYTE_SIZE
    }

    pub fn parse_row_bytes(&self, bytes: &[u8]) -> HashMap<String, Value> {
        let mut out = HashMap::new();

        let mut pos = 0usize;
        for (field_name, field_schema) in &self.fields {
            out.insert(
                field_name.clone(),
                field_schema.value_from_bytes(&bytes[pos..]),
            );
            pos += field_schema.byte_size();
        }

        out
    }
}

pub struct TableReader<'a> {
    table_schema: &'a TableSchema,
    row_bytes: &'a [u8],
    pub absolute_pos: usize,
}

impl<'a> TableReader<'a> {
    pub fn new(
        table_schema: &'a TableSchema,
        row_bytes: &'a [u8],
        absolute_pos: usize,
    ) -> TableReader<'a> {
        TableReader {
            table_schema,
            row_bytes,
            absolute_pos,
        }
    }

    pub fn get_field_value(&self, field: &str) -> Value {
        let field_pos = self.table_schema.field_byte_pos(field);
        self.table_schema.fields[field].value_from_bytes(&self.row_bytes[field_pos..])
    }
}

pub struct TableRowIterator<'a> {
    table_schema: &'a TableSchema,
    table_bytes: &'a [u8],
    selection: &'a Selection,
    current_pos: usize,
}

impl<'a> TableRowIterator<'a> {
    pub fn new(
        table_schema: &'a TableSchema,
        table_bytes: &'a [u8],
        selection: &'a Selection,
    ) -> TableRowIterator<'a> {
        TableRowIterator {
            table_schema,
            table_bytes,
            selection,
            current_pos: 0,
        }
    }

    fn next_with_all_selection(&mut self) -> Option<TableReader<'a>> {
        if self.current_pos >= self.table_bytes.len() {
            None
        } else {
            let pos = self.current_pos;
            self.current_pos += self.table_schema.row_byte_size();

            Some(TableReader::new(
                &self.table_schema,
                &self.table_bytes[pos..pos + self.table_schema.row_byte_size()],
                pos,
            ))
        }
    }

    fn next_with_positions(&mut self, positions: &Vec<usize>) -> Option<TableReader<'a>> {
        if self.current_pos >= positions.len() {
            None
        } else {
            let current_pos = positions[self.current_pos];
            self.current_pos += 1;

            Some(TableReader::new(
                &self.table_schema,
                &self.table_bytes[current_pos..current_pos + self.table_schema.row_byte_size()],
                current_pos,
            ))
        }
    }
}

impl<'a> Iterator for TableRowIterator<'a> {
    type Item = TableReader<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.selection {
            Selection::All => self.next_with_all_selection(),
            Selection::List(positions) => self.next_with_positions(positions),
        }
    }
}

pub struct TableRowPositionIterator {
    row_size: usize,
    table_size: usize,
    current_pos: usize,
}

impl TableRowPositionIterator {
    pub fn new(row_size: usize, table_size: usize) -> TableRowPositionIterator {
        TableRowPositionIterator {
            row_size,
            table_size,
            current_pos: 0,
        }
    }
}

impl Iterator for TableRowPositionIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.table_size {
            None
        } else {
            let pos = self.current_pos;
            self.current_pos += self.row_size;
            Some(pos)
        }
    }
}

pub struct DatabaseSchema {
    pub tables: HashMap<String, TableSchema>,
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use indexmap::IndexMap;

    use crate::{schema::FieldSchema, value::Value};

    use super::{TableRowIterator, TableSchema};

    #[test]
    fn test_empty_table_schema() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([]),
            indices: HashMap::from([]),
        };

        assert_eq!(0, table_schema.row_byte_size());
    }

    #[test]
    #[should_panic(expected = "Field 'missing' not found in table 't1'")]
    fn test_missing_field_access_panics() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([]),
            indices: HashMap::from([]),
        };

        table_schema.field_byte_pos("missing");
    }

    #[test]
    #[should_panic]
    fn test_missing_index_access_panics() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([]),
            indices: HashMap::from([]),
        };
        table_schema.index_row_byte_size("missing");
    }

    #[test]
    fn test_normal_table_schema() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([
                ("f1".to_string(), FieldSchema::I32),
                ("f2".to_string(), FieldSchema::I32),
                ("f3".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::from([
                ("i1".to_string(), vec!["f1".to_string(), "f2".to_string()]),
                ("i2".to_string(), vec!["f3".to_string()]),
            ]),
        };

        assert_eq!(12, table_schema.row_byte_size());

        assert_eq!(0, table_schema.field_byte_pos("f1"));
        assert_eq!(4, table_schema.field_byte_pos("f2"));
        assert_eq!(8, table_schema.field_byte_pos("f3"));

        assert_eq!(16, table_schema.index_row_byte_size("i1"));
        assert_eq!(12, table_schema.index_row_byte_size("i2"));

        #[rustfmt::skip]
        let expected_bytes = vec![
            1, 0, 0, 0,
            0, 0, 0, 0,
            3, 0, 0, 0,
        ];
        assert_eq!(
            expected_bytes,
            table_schema.data_row_to_bytes(&HashMap::from([
                ("f3".to_string(), Value::I32(3)),
                ("f1".to_string(), Value::I32(1)),
            ])),
        );

        #[rustfmt::skip]
        let expected_bytes = vec![
            1, 0, 0, 0,
            2, 0, 0, 0,
            7, 0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(
            expected_bytes,
            table_schema.index_row_to_bytes(
                "i1",
                &HashMap::from([
                    ("f1".to_string(), Value::I32(1)),
                    ("f2".to_string(), Value::I32(2)),
                ]),
                7
            ),
        );

        assert_eq!(0, table_schema.index_field_byte_pos("i1", "f1"));
        assert_eq!(4, table_schema.index_field_byte_pos("i1", "f2"));
        assert_eq!(0, table_schema.index_field_byte_pos("i2", "f3"));
    }

    #[test]
    fn test_parse_row_bytes() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([
                ("f1".to_string(), FieldSchema::I32),
                ("f2".to_string(), FieldSchema::I32),
                ("f3".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::from([
                ("i1".to_string(), vec!["f1".to_string(), "f2".to_string()]),
                ("i2".to_string(), vec!["f3".to_string()]),
            ]),
        };

        let bytes: [u8; 12] = [1, 2, 3, 4, 5, 5, 5, 5, 6, 7, 8, 9];
        let values = table_schema.parse_row_bytes(&bytes);

        assert_eq!(Value::I32(0x04030201), values["f1"]);
        assert_eq!(Value::I32(0x05050505), values["f2"]);
        assert_eq!(Value::I32(0x09080706), values["f3"]);
    }

    #[test]
    fn test_table_row_iterator_with_all_selection() {
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

        let mut it =
            TableRowIterator::new(&table_schema, &table_bytes, &crate::common::Selection::All);

        let row1 = it.next().unwrap();
        assert_eq!(Value::I32(1), row1.get_field_value("f1"));
        assert_eq!(Value::I32(2), row1.get_field_value("f2"));

        let row2 = it.next().unwrap();
        assert_eq!(Value::I32(3), row2.get_field_value("f1"));
        assert_eq!(Value::I32(4), row2.get_field_value("f2"));

        assert!(it.next().is_none());
    }

    #[test]
    fn test_table_row_iterator_with_list_selection() {
        let table_schema = TableSchema {
            name: "t1".to_string(),
            fields: IndexMap::from([
                ("f1".to_string(), FieldSchema::I32),
                ("f2".to_string(), FieldSchema::I32),
            ]),
            indices: HashMap::new(),
        };

        #[rustfmt::skip]
        let table_bytes: [u8; 24] = [
            1, 0, 0, 0,   2, 0, 0, 0, // Row 1
            3, 0, 0, 0,   4, 0, 0, 0, // Row 2
            5, 0, 0, 0,   6, 0, 0, 0, // Row 3
        ];

        let selection = crate::common::Selection::List(vec![8, 16]);
        let mut it = TableRowIterator::new(&table_schema, &table_bytes, &selection);

        let row1 = it.next().unwrap();
        assert_eq!(Value::I32(3), row1.get_field_value("f1"));
        assert_eq!(Value::I32(4), row1.get_field_value("f2"));
        assert_eq!(8, row1.absolute_pos);

        let row2 = it.next().unwrap();
        assert_eq!(Value::I32(5), row2.get_field_value("f1"));
        assert_eq!(Value::I32(6), row2.get_field_value("f2"));
        assert_eq!(16, row2.absolute_pos);

        assert!(it.next().is_none());
    }
}
