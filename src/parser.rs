use crate::{
    common::{Error, PBaseError},
    lexer::Token,
    query::{Query, SelectQuery},
};

pub struct Parser<'a> {
    __tokens: &'a [Token],
    i: usize,
}

impl<'a> Parser<'a> {
    #[must_use]
    pub const fn new(__tokens: &'a [Token]) -> Self {
        Self { __tokens, i: 0 }
    }

    #[must_use]
    fn tokens(&self) -> &[Token] {
        &self.__tokens[self.i..]
    }

    const fn advance(&mut self) {
        self.i += 1;
    }

    fn head(&self) -> Option<&Token> {
        self.tokens().first()
    }

    /// # Errors
    ///
    /// Returns error when token stream cannot be parsed.
    pub fn parse(&mut self) -> Result<Query, Error> {
        match self.head() {
            Some(&Token::Select) => Ok(Query::Select(self.parse_select_query()?)),
            head => Err(format!("Not yet implemented for token: {head:?}",).into()),
        }
    }

    fn must_swallow(&mut self, expected_token: &Token) -> Result<(), Error> {
        if self.head() == Some(expected_token) {
            self.advance();
            Ok(())
        } else {
            Err(PBaseError::UnexpextedToken(format!(
                "Expected {expected_token:?} got {:?}",
                self.head()
            ))
            .into())
        }
    }
    fn bail(&self, message: &str) -> Error {
        PBaseError::UnexpextedToken(format!(
            "Message: {message}. Current token: {:?}",
            self.head()
        ))
        .into()
    }

    fn parse_select_query(&mut self) -> Result<SelectQuery, Error> {
        self.must_swallow(&Token::Select)?;
        self.must_swallow(&Token::From)?;

        let Some(Token::Identifier(table_name)) = self.head().cloned() else {
            return Err(self.bail("expected table name"));
        };
        self.advance();

        Ok(SelectQuery {
            from: table_name,
            joins: vec![],
            filters: vec![],
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        lexer::Lexer,
        query::{Query, SelectQuery},
    };

    use super::Parser;

    #[test]
    fn test_minimal_select_query() {
        let query = Parser::new(
            &Lexer::tokenize(
                br"
        SELECT
        FROM t1
        ",
            )
            .expect("failed to tokenize")[..],
        )
        .parse()
        .expect("failed to parse");

        assert_eq!(
            Query::Select(SelectQuery {
                from: "t1".into(),
                joins: vec![],
                filters: vec![]
            }),
            query,
        );
    }
}
