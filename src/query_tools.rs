use std::collections::HashSet;

use crate::{query::SelectQuery, schema::TableSchema};

pub fn index_for_query(table_schema: &TableSchema, select_query: &SelectQuery) -> Option<String> {
    let mut filter_fields: HashSet<&String> = HashSet::new();
    for filter in &select_query.filters {
        if filter.field.source == table_schema.name {
            filter_fields.insert(&filter.field.name);
        }
    }

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

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use crate::query_tools::index_score;

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
}
