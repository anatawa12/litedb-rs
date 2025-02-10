use std::borrow::Cow;

mod tokenizer;

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
}

impl Eq for Token<'_> {}
impl PartialEq for Token<'_> {
    fn eq(&self, other: &Token) -> bool {
        // no position check
        self.typ == other.typ && self.value == other.value
    }
}
