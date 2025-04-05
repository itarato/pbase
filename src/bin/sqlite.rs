use rand::prelude::*;
use sqlite;

fn main() {
    let connection = sqlite::open("database.db").expect("Failed to open database");

    connection
        .execute(
            "
            CREATE TABLE IF NOT EXISTS bigtable (
                field1 INTEGER,
                field2 INTEGER,
                field3 INTEGER,
                field4 INTEGER
            );
            ",
        )
        .expect("Failed to create table");

    let mut rng = rand::rng();
    for _ in 0..10_000 {
        connection
            .execute(format!(
                "
            INSERT INTO bigtable (field1, field2, field3, field4) VALUES ({}, {}, {}, {});
            ",
                rng.random::<i32>() % 1000,
                rng.random::<i32>() % 1000,
                rng.random::<i32>() % 1000,
                rng.random::<i32>() % 1000
            ))
            .expect("Failed to insert into table");
    }

    println!("Inserted value into bigtable.");
}
