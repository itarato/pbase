use std::cmp::Ordering;

use crate::common::{Error, PBaseError};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Token {
    Select,
    From,
    Join,
    Comma,
    And,
    Identifier(String),
    Op(Ordering),
    Int(i32),
    Dot,
}

const SELECT_WORD: &[u8; 6] = b"SELECT";
const FROM_WORD: &[u8; 4] = b"FROM";
const JOIN_WORD: &[u8; 4] = b"JOIN";
const AND_WORD: &[u8; 3] = b"AND";
const COMMA_CHAR: u8 = b',';
const EQ_CHAR: u8 = b'=';
const LT_CHAR: u8 = b'<';
const GT_CHAR: u8 = b'>';
const DOT_CHAR: u8 = b'.';

pub struct Lexer;

impl Lexer {
    /// # Errors
    ///
    /// Returns error for unrecognizable stream.
    pub fn tokenize(input: &[u8]) -> Result<Vec<Token>, Error> {
        let mut raw = input;
        let mut tokens = vec![];

        while !raw.is_empty() {
            if let Some(part) = read_keyword(raw) {
                let token = match part {
                    part if part == SELECT_WORD => Token::Select,
                    part if part == FROM_WORD => Token::From,
                    part if part == JOIN_WORD => Token::Join,
                    part if part == AND_WORD => Token::And,
                    _ => Token::Identifier(String::from_utf8_lossy(part).to_string()),
                };

                raw = &raw[part.len()..];
                tokens.push(token);
            } else if raw[0] == COMMA_CHAR {
                raw = &raw[1..];
                tokens.push(Token::Comma);
            } else if raw[0] == EQ_CHAR {
                raw = &raw[1..];
                tokens.push(Token::Op(Ordering::Equal));
            } else if raw[0] == LT_CHAR {
                raw = &raw[1..];
                tokens.push(Token::Op(Ordering::Less));
            } else if raw[0] == GT_CHAR {
                raw = &raw[1..];
                tokens.push(Token::Op(Ordering::Greater));
            } else if raw[0] == DOT_CHAR {
                raw = &raw[1..];
                tokens.push(Token::Dot);
            } else if raw[0].is_ascii_whitespace() {
                let whitespace = take_while(raw, u8::is_ascii_whitespace);
                raw = &raw[whitespace.len()..];
            } else if raw[0].is_ascii_digit() {
                let digits = take_while(raw, u8::is_ascii_digit);
                raw = &raw[digits.len()..];

                let number = std::str::from_utf8(digits)
                    .map_err(|_| {
                        PBaseError::BadToken("Digit sequence cannot be stringified".into())
                    })?
                    .parse::<i32>()
                    .map_err(|_| PBaseError::BadToken("Unrecognizable integer".into()))?;

                tokens.push(Token::Int(number));
            } else {
                return Err(PBaseError::BadToken("Unrecognizable next character".into()).into());
            }
        }

        Ok(tokens)
    }
}

fn read_keyword(raw: &[u8]) -> Option<&[u8]> {
    if raw[0].is_ascii_alphabetic() {
        Some(take_while(raw, |c| c.is_ascii_alphanumeric() || c == &b'_'))
    } else {
        None
    }
}

fn take_while<F>(raw: &[u8], cond: F) -> &[u8]
where
    F: Fn(&u8) -> bool,
{
    let mut i = 0;
    while i < raw.len() && cond(&raw[i]) {
        i += 1;
    }

    &raw[0..i]
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;

    use super::Lexer;
    use crate::lexer::Token;

    #[test]
    fn test_simple_select_query() {
        let raw_query = br"
        SELECT
        FROM t1
";

        let tokens = Lexer::tokenize(raw_query).unwrap();

        assert_eq!(3, tokens.len());
        assert_eq!(Token::Select, tokens[0]);
        assert_eq!(Token::From, tokens[1]);
        assert_eq!(Token::Identifier("t1".into()), tokens[2]);
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_full_select_query() {
        let raw_query = br"
        SELECT
            FROM t1
            JOIN t2 ON t1.id = t2.t1_id
        WHERE
            t1.id = 1 AND
            t2.v < 2
";

        let tokens = Lexer::tokenize(raw_query).unwrap();

        assert_eq!(25, tokens.len());
        assert_eq!(Token::Select, tokens[0]);
        assert_eq!(Token::From, tokens[1]);
        assert_eq!(Token::Identifier("t1".into()), tokens[2]);
        assert_eq!(Token::Join, tokens[3]);
        assert_eq!(Token::Identifier("t2".into()), tokens[4]);
        assert_eq!(Token::Identifier("ON".into()), tokens[5]);
        assert_eq!(Token::Identifier("t1".into()), tokens[6]);
        assert_eq!(Token::Dot, tokens[7]);
        assert_eq!(Token::Identifier("id".into()), tokens[8]);
        assert_eq!(Token::Op(Ordering::Equal), tokens[9]);
        assert_eq!(Token::Identifier("t2".into()), tokens[10]);
        assert_eq!(Token::Dot, tokens[11]);
        assert_eq!(Token::Identifier("t1_id".into()), tokens[12]);
        assert_eq!(Token::Identifier("WHERE".into()), tokens[13]);
        assert_eq!(Token::Identifier("t1".into()), tokens[14]);
        assert_eq!(Token::Dot, tokens[15]);
        assert_eq!(Token::Identifier("id".into()), tokens[16]);
        assert_eq!(Token::Op(Ordering::Equal), tokens[17]);
        assert_eq!(Token::Int(1), tokens[18]);
        assert_eq!(Token::And, tokens[19]);
        assert_eq!(Token::Identifier("t2".into()), tokens[20]);
        assert_eq!(Token::Dot, tokens[21]);
        assert_eq!(Token::Identifier("v".into()), tokens[22]);
        assert_eq!(Token::Op(Ordering::Less), tokens[23]);
        assert_eq!(Token::Int(2), tokens[24]);
    }
}
