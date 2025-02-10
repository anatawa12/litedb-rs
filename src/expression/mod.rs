use crate::expression::parser::Expression;
use crate::utils::CaseInsensitiveString;
use std::borrow::Cow;
use std::collections::HashSet;

mod parser;
mod tokenizer;

type Error = super::Error;

impl Error {
    fn expr_error(str: &str) -> Self {
        Self::err(format!("parsing: {}", str))
    }
    fn unexpected_token(str: &str, token: &Token) -> Self {
        Self::err(format!("unexpected token ({}): {:?}", str, token))
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Error {
        Self::err(format!("unexpected token: {}", err))
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Self::err(format!("unexpected token: {}", err))
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum BsonExpressionType {
    Double = 1,
    Int = 2,
    String = 3,
    Boolean = 4,
    Null = 5,
    Array = 6,
    Document = 7,

    Parameter = 8,
    Call = 9,
    Path = 10,

    Modulo = 11,
    Add = 12,
    Subtract = 13,
    Multiply = 14,
    Divide = 15,

    Equal = 16,
    Like = 17,
    Between = 18,
    GreaterThan = 19,
    GreaterThanOrEqual = 20,
    LessThan = 21,
    LessThanOrEqual = 22,
    NotEqual = 23,
    In = 24,

    Or = 25,
    And = 26,

    Map = 27,
    Filter = 28,
    Sort = 29,
    Source = 30,
}

#[derive(Debug, Clone)]
pub struct BsonExpression {
    r#type: BsonExpressionType,
    is_immutable: bool,
    use_source: bool,
    is_scalar: bool,
    fields: HashSet<CaseInsensitiveString>,
    expression: Expression,
    left: Option<Box<BsonExpression>>,
    right: Option<Box<BsonExpression>>,
    source: String,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum TokenType {
    /// `{`
    OpenBrace,
    /// `}`
    CloseBrace,
    /// `[`
    OpenBracket,
    /// `]`
    CloseBracket,
    /// `(`
    OpenParenthesis,
    /// `)`
    CloseParenthesis,
    /// `,`
    Comma,
    /// `:`
    Colon,
    /// `;`
    SemiColon,
    /// `@`
    At,
    /// `#`
    Hashtag,
    /// `~`
    Til,
    /// `.`
    Period,
    /// `&`
    Ampersand,
    /// `$`
    Dollar,
    /// `!`
    Exclamation,
    /// `!=`
    NotEquals,
    /// `=`
    Equals,
    /// `>`
    Greater,
    /// `>=`
    GreaterOrEquals,
    /// `<`
    Less,
    /// `<=`
    LessOrEquals,
    /// `-`
    Minus,
    /// `+`
    Plus,
    /// `*`
    Asterisk,
    /// `/`
    Slash,
    /// `\`
    Backslash,
    /// `%`
    Percent,
    /// `"..."` or `'...'`
    String,
    /// number without decimals
    Int,
    /// number with decimals
    Double,
    /// `\n\r\t \u0032`
    /// Generally skipped but can be provided if
    Whitespace,
    /// `[a-Z_$]+[a-Z0-9_$]` possibly keyword or field name
    Word,
    Eof,
    Unknown,
}

#[derive(Debug, Clone)]
struct Token<'a> {
    pub typ: TokenType,
    value: Cow<'a, str>,
    position: usize,
}

impl<'a> Token<'a> {
    pub(crate) fn new(typ: TokenType, value: impl Into<Cow<'a, str>>, position: usize) -> Self {
        Token {
            typ,
            value: value.into(),
            position,
        }
    }

    pub fn is(&self, str: &str) -> bool {
        self.typ == TokenType::Word && self.value.eq_ignore_ascii_case(str)
    }

    pub fn is_case(&self, str: &str) -> bool {
        self.typ == TokenType::Word && self.value == str
    }

    pub fn is_operand(&self) -> bool {
        match self.typ {
            TokenType::Percent
            | TokenType::Slash
            | TokenType::Asterisk
            | TokenType::Plus
            | TokenType::Minus
            | TokenType::Equals
            | TokenType::Greater
            | TokenType::GreaterOrEquals
            | TokenType::Less
            | TokenType::LessOrEquals
            | TokenType::NotEquals => true,
            TokenType::Word => matches!(self.value.to_uppercase().as_str(), "BETWEEN" | "LIKE" | "IN" | "AND" | "OR"),
            _ => false,
        }
    }

    fn expect_token(&self, token: &str) -> Result<&Self, Error> {
        if !self.is(token) {
            return Err(Error::unexpected_token("unexpected token", self));
        }
        Ok(self)
    }
}

trait ExpectTypeTrait: Sized {
    fn token(&self) -> &Token<'_>;

    fn expect_type<const N: usize>(self, types: [TokenType; N]) -> Result<Self, Error> {
        if !types.iter().any(|x| x == &self.token().typ) {
            return Err(Error::unexpected_token("unexpected token", self.token()));
        }
        Ok(self)
    }
}

impl ExpectTypeTrait for &Token<'_> {
    fn token(&self) -> &Token<'_> {
        self
    }
}

impl ExpectTypeTrait for Token<'_> {
    fn token(&self) -> &Token<'_> {
        self
    }
}

impl Eq for Token<'_> {}
impl PartialEq for Token<'_> {
    fn eq(&self, other: &Token) -> bool {
        // no position check
        self.typ == other.typ && self.value == other.value
    }
}
