use crate::schema::*;
use std::path::PathBuf;

struct Database {
    schema: DatabaseSchema,
    /* File structure:
       - <database_name>__<table_name>.pb
    */
    filename: PathBuf,
}
