use pbase::{common::Error, lexer::Lexer, parser::Parser, pbase::PBase, query::Query};
use std::{
    io::{self, stdout, Write},
    path::PathBuf,
};

fn main() -> Result<(), Error> {
    let mut buffer = String::new();
    let stdin = io::stdin();

    let db = PBase::new(std::env::current_dir().unwrap_or_else(|_| PathBuf::new()));

    loop {
        stdout().write_all(b"> ")?;
        stdout().flush()?;

        buffer.clear();
        stdin.read_line(&mut buffer)?;

        if buffer.trim() == "exit" {
            break;
        } else {
            let Ok(tokens) = Lexer::tokenize(buffer.as_bytes()) else {
                stdout().write_all(b"Unrecognized characters")?;
                continue;
            };
            let mut parser = Parser::new(&tokens[..]);
            let query = parser.parse();

            match query {
                Ok(Query::Select(select_query)) => {
                    let result = db.run_select_query(select_query)?;
                    dbg!(result);
                }
                Ok(_) => unimplemented!(),
                Err(err) => {
                    stdout().write_fmt(format_args!("Unrecognized query. Error: {:?}\n\n", err))?
                }
            }
        }
    }

    Ok(())
}
