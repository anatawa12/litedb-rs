use super::{ParseError, Token, TokenType};
use crate::utils::is_word_char;
use std::borrow::Cow;

pub(super) struct Tokenizer<'a> {
    parser: TokenizeParser<'a>,
    current: Option<Token<'a>>,
    ahead: Option<Token<'a>>,
}

struct TokenizeParser<'a> {
    // the value of source won't be changed; just position will be
    source: &'a str,
    position: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(source: &'a str) -> Self {
        Tokenizer {
            parser: TokenizeParser {
                source,
                position: 0,
            },
            current: None,
            ahead: None,
        }
    }
}

impl<'a> Tokenizer<'a> {
    pub fn eof(&self) -> bool {
        self.ahead.is_none() && self.parser.source.len() <= self.parser.position
    }

    pub fn check_eof(&self) -> Result<bool, ParseError> {
        if self.parser.source.len() <= self.parser.position {
            return Err(ParseError::unexpected_token(
                self.current(),
                format_args!("unexpected eof"),
            ));
        }

        Ok(false)
    }

    pub fn current(&self) -> &Token<'a> {
        self.current.as_ref().unwrap()
    }

    pub fn take_current(&mut self) -> Token<'a> {
        self.current.take().unwrap()
    }

    pub fn look_ahead(&mut self) -> &Token<'a> {
        if self
            .ahead
            .as_ref()
            .map(|token| token.typ == TokenType::Whitespace)
            .unwrap_or(false)
        {
            self.ahead = None;
        }

        self.ahead
            .get_or_insert_with(|| self.parser.read_next(true))
    }

    pub fn look_ahead_with_whitespace(&mut self) -> &Token<'a> {
        self.ahead
            .get_or_insert_with(|| self.parser.read_next(false))
    }

    pub fn read_token(&mut self) -> &Token<'a> {
        self.current.insert(
            self.ahead
                .take()
                .take_if(|x| x.typ != TokenType::Whitespace)
                .unwrap_or_else(|| self.parser.read_next(true)),
        )
    }

    pub fn read_token_with_whitespace(&mut self) -> &Token<'a> {
        self.current.insert(
            self.ahead
                .take()
                .unwrap_or_else(|| self.parser.read_next(false)),
        )
    }
}

impl<'a> TokenizeParser<'a> {
    fn read_next(&mut self, eat_whitespace: bool) -> Token<'a> {
        if eat_whitespace {
            self.eat_whitespace();
        }

        let Some(c) = self.cur_char() else {
            return Token::new(TokenType::Eof, "", self.position);
        };

        let token;

        match c {
            '{' => {
                token = Token::new(TokenType::OpenBrace, "{", self.position);
                self.read_char(c);
            }

            '}' => {
                token = Token::new(TokenType::CloseBrace, "}", self.position);
                self.read_char(c);
            }

            '[' => {
                token = Token::new(TokenType::OpenBracket, "[", self.position);
                self.read_char(c);
            }

            ']' => {
                token = Token::new(TokenType::CloseBracket, "]", self.position);
                self.read_char(c);
            }

            '(' => {
                token = Token::new(TokenType::OpenParenthesis, "(", self.position);
                self.read_char(c);
            }

            ')' => {
                token = Token::new(TokenType::CloseParenthesis, ")", self.position);
                self.read_char(c);
            }

            ',' => {
                token = Token::new(TokenType::Comma, ",", self.position);
                self.read_char(c);
            }

            ':' => {
                token = Token::new(TokenType::Colon, ":", self.position);
                self.read_char(c);
            }

            ';' => {
                token = Token::new(TokenType::SemiColon, ";", self.position);
                self.read_char(c);
            }

            '@' => {
                token = Token::new(TokenType::At, "@", self.position);
                self.read_char(c);
            }

            '#' => {
                token = Token::new(TokenType::Hashtag, "#", self.position);
                self.read_char(c);
            }

            '~' => {
                token = Token::new(TokenType::Til, "~", self.position);
                self.read_char(c);
            }

            '.' => {
                token = Token::new(TokenType::Period, ".", self.position);
                self.read_char(c);
            }

            '&' => {
                token = Token::new(TokenType::Ampersand, "&", self.position);
                self.read_char(c);
            }

            '$' => {
                let begin = self.position;
                self.read_char(c);
                if self
                    .cur_char()
                    .map(|c| is_word_char(c, true))
                    .unwrap_or(false)
                {
                    token = Token::new(TokenType::Word, self.read_word(begin), self.position);
                } else {
                    token = Token::new(TokenType::Dollar, "$", begin);
                }
            }

            '!' => {
                let begin = self.position;
                self.read_char(c);
                if self.cur_char() == Some('=') {
                    token = Token::new(TokenType::NotEquals, "!=", begin);
                    self.read_char('=');
                } else {
                    token = Token::new(TokenType::Exclamation, "!", begin);
                }
            }

            '=' => {
                token = Token::new(TokenType::Equals, "=", self.position);
                self.read_char(c);
            }

            '>' => {
                let begin = self.position;
                self.read_char(c);
                if self.cur_char() == Some('=') {
                    token = Token::new(TokenType::GreaterOrEquals, ">=", begin);
                    self.read_char('=');
                } else {
                    token = Token::new(TokenType::Greater, ">", begin);
                }
            }

            '<' => {
                let begin = self.position;
                self.read_char(c);
                if self.cur_char() == Some('=') {
                    token = Token::new(TokenType::LessOrEquals, "<=", begin);
                    self.read_char('=');
                } else {
                    token = Token::new(TokenType::Less, "<", begin);
                }
            }

            '-' => {
                let begin = self.position;
                self.read_char(c);
                if self.cur_char() == Some('-') {
                    // comment
                    self.read_line();
                    token = self.read_next(eat_whitespace);
                } else {
                    token = Token::new(TokenType::Minus, "-", begin);
                }
            }

            '+' => {
                token = Token::new(TokenType::Plus, "+", self.position);
                self.read_char(c);
            }

            '*' => {
                token = Token::new(TokenType::Asterisk, "*", self.position);
                self.read_char(c);
            }

            '/' => {
                token = Token::new(TokenType::Slash, "/", self.position);
                self.read_char(c);
            }
            '\\' => {
                token = Token::new(TokenType::Backslash, "\\", self.position);
                self.read_char(c);
            }

            '%' => {
                token = Token::new(TokenType::Percent, "%", self.position);
                self.read_char(c);
            }

            '\"' | '\'' => {
                let begin = self.position;
                token = Token::new(TokenType::String, self.read_string(c), begin);
            }

            '0'..='9' => {
                let begin = self.position;
                let (number, typ) = self.read_number();
                token = Token::new(typ, number, begin);
            }

            ' ' | '\n' | '\r' | '\t' => {
                let begin = self.position;

                while let Some(c) = self.cur_char() {
                    if !c.is_whitespace() {
                        break;
                    }
                    self.read_char(c);
                }

                let end = self.position;

                let span = &self.source[begin..end];

                token = Token::new(TokenType::Whitespace, span, begin);
            }

            c => {
                // test if first char is an word
                if is_word_char(c, true) {
                    let begin = self.position;
                    token = Token::new(TokenType::Word, self.read_word(self.position), begin);
                } else {
                    let begin = self.position;
                    let span = &self.source[begin..][..c.len_utf8()];
                    token = Token::new(TokenType::Unknown, span, begin);
                    self.read_char(c);
                }
            }
        }

        token
    }

    fn eat_whitespace(&mut self) {
        let source = &self.source[self.position..];
        let trimmed = source.trim_start();
        self.position += source.len() - trimmed.len();
    }

    fn read_word(&mut self, begin: usize) -> &'a str {
        self.read_char(self.cur_char().unwrap());

        while let Some(c) = self.cur_char() {
            if !is_word_char(c, false) {
                break;
            }
            self.read_char(c);
        }

        let end = self.position;

        &self.source[begin..end]
    }

    fn read_number(&mut self) -> (&'a str, TokenType) {
        let mut is_double = false;
        let begin = self.position;
        debug_assert!(self.cur_char().map(|c| c.is_ascii_digit()).unwrap_or(false));
        self.read_char(self.cur_char().unwrap());

        let mut can_dot = true;
        let mut can_e = true;
        let mut can_sign = false;

        while let Some(c) = self.cur_char() {
            // RustNote(mini breaking change): litedb accepts non-ascii digits here,
            // but it seems a bug so we only accept ascii digits
            match c {
                '.' if can_dot => {
                    is_double = true;
                    can_dot = false;
                }
                'e' | 'E' if can_e => {
                    can_e = false;
                    can_sign = true;
                    can_dot = false;
                    is_double = true;
                }
                '+' | '-' if can_sign => {
                    can_sign = false;
                }
                '0'..='9' => {
                    // sign is allowed only just after e char
                    can_sign = false;
                }
                _ => break,
            }
            self.read_char(c);
        }

        let end = self.position;

        let span = &self.source[begin..end];

        let typ = if is_double {
            TokenType::Double
        } else {
            TokenType::Int
        };

        (span, typ)
    }

    fn read_string(&mut self, quote: char) -> Cow<'a, str> {
        self.read_char(quote);
        let mut str = String::new();

        let mut span_begin = self.position;
        let mut end_position = None;

        while let Some(c) = self.cur_char() {
            if c == '\\' {
                // escape sequence
                str.push_str(&self.source[span_begin..self.position]);
                self.read_char(c);

                if let Some(c) = self.cur_char() {
                    self.read_char(c);
                    match c {
                        '\\' => str.push('\\'),
                        '/' => str.push('/'),
                        'b' => str.push('\x08'),
                        'f' => str.push('\x0c'),
                        'n' => str.push('\n'),
                        'r' => str.push('\r'),
                        't' => str.push('\t'),
                        'u' => {
                            // TODO: surrogate pair support

                            fn parse_char(c: Option<char>, multiplier: u16) -> u16 {
                                match c {
                                    Some(c @ '0'..='9') => (c as u16 - b'0' as u16) * multiplier,
                                    Some(c @ 'A'..='F') => {
                                        (c as u16 - b'A' as u16 + 10) * multiplier
                                    }
                                    Some(c @ 'a'..='f') => {
                                        (c as u16 - b'a' as u16 + 10) * multiplier
                                    }
                                    _ => 0,
                                }
                            }

                            let p1 = parse_char(self.cur_char(), 0x1000);
                            if let Some(c) = self.cur_char() {
                                self.read_char(c)
                            };
                            let p2 = parse_char(self.cur_char(), 0x0100);
                            if let Some(c) = self.cur_char() {
                                self.read_char(c)
                            };
                            let p3 = parse_char(self.cur_char(), 0x0010);
                            if let Some(c) = self.cur_char() {
                                self.read_char(c)
                            };
                            let p4 = parse_char(self.cur_char(), 0x0001);
                            if let Some(c) = self.cur_char() {
                                self.read_char(c)
                            };
                            let c = p1 + p2 + p3 + p4;
                            let c = char::from_u32(c as u32).unwrap_or('\0');

                            str.push(c);
                        }
                        c if c == quote => str.push(c),
                        _ => {}
                    }
                }
                span_begin = self.position;
            } else if c == quote {
                end_position = Some(self.position);
                self.read_char(c);
                break;
            } else {
                self.read_char(c);
            }
        }

        let span = &self.source[span_begin..end_position.unwrap_or(self.position)];

        if str.is_empty() {
            Cow::Borrowed(span)
        } else {
            str.push_str(span);
            Cow::Owned(str)
        }
    }

    fn read_line(&mut self) {
        while let Some(c) = self.cur_char() {
            if c == '\n' {
                self.read_char(c);
                break;
            }
            self.read_char(c);
        }
    }

    //////
    fn cur_char(&self) -> Option<char> {
        self.source[self.position..].chars().next()
    }

    fn read_char(&mut self, c: char) {
        debug_assert!(self.cur_char() == Some(c));
        self.position += c.len_utf8();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        use TokenType::*;
        let mut tokenizer = Tokenizer::new(
            r###"
        word 123 0.123 123e123 123e+123 123e-123 "string" 'string'
        "escaped\" string with \u0000 \u1paF \r 'test \n\_\\\/\b\f\t"
        token1 -- comment
        token2 "-- comment in string"
        $dallerWord
        { } [ ] ( ) , : ; @ # ~ . & $ ! != = > >= < <= - + * / \ %
        whitespace0 whitespace1 whitespace2 whitespace3
        "###,
        );

        macro_rules! check_token {
            ($typ: expr, $value: expr) => {
                let token = Token::new($typ, $value, 0);
                assert_eq!(tokenizer.look_ahead(), &token);
                assert_eq!(tokenizer.look_ahead(), &token);
                assert_eq!(tokenizer.read_token(), &token);
                assert_eq!(tokenizer.current(), &token);
            };
        }

        // check for basic token parsing
        check_token!(Word, "word");
        check_token!(Int, "123");
        check_token!(Double, "0.123");
        check_token!(Double, "123e123");
        check_token!(Double, "123e+123");
        check_token!(Double, "123e-123");
        check_token!(String, "string");
        check_token!(String, "string");
        check_token!(
            String,
            "escaped\" string with \0 \u{10aF} \r 'test \n\\/\x08\x0c\t"
        );
        check_token!(Word, "token1");
        check_token!(Word, "token2");
        check_token!(String, "-- comment in string");
        check_token!(Word, "$dallerWord");
        check_token!(OpenBrace, "{");
        check_token!(CloseBrace, "}");
        check_token!(OpenBracket, "[");
        check_token!(CloseBracket, "]");
        check_token!(OpenParenthesis, "(");
        check_token!(CloseParenthesis, ")");
        check_token!(Comma, ",");
        check_token!(Colon, ":");
        check_token!(SemiColon, ";");
        check_token!(At, "@");
        check_token!(Hashtag, "#");
        check_token!(Til, "~");
        check_token!(Period, ".");
        check_token!(Ampersand, "&");
        check_token!(Dollar, "$");
        check_token!(Exclamation, "!");
        check_token!(NotEquals, "!=");
        check_token!(Equals, "=");
        check_token!(Greater, ">");
        check_token!(GreaterOrEquals, ">=");
        check_token!(Less, "<");
        check_token!(LessOrEquals, "<=");
        check_token!(Minus, "-");
        check_token!(Plus, "+");
        check_token!(Asterisk, "*");
        check_token!(Slash, "/");
        check_token!(Backslash, "\\");
        check_token!(Percent, "%");

        // whitespace parsing checks
        check_token!(Word, "whitespace0");
        assert_eq!(
            tokenizer.look_ahead_with_whitespace(),
            &Token::new(Whitespace, " ", 0)
        );
        assert_eq!(
            tokenizer.look_ahead_with_whitespace(),
            &Token::new(Whitespace, " ", 0)
        );
        assert_eq!(tokenizer.look_ahead(), &Token::new(Word, "whitespace1", 0));
        assert_eq!(tokenizer.look_ahead(), &Token::new(Word, "whitespace1", 0));
        assert_eq!(tokenizer.read_token(), &Token::new(Word, "whitespace1", 0));
        assert_eq!(
            tokenizer.read_token_with_whitespace(),
            &Token::new(Whitespace, " ", 0)
        );
        assert_eq!(
            tokenizer.read_token_with_whitespace(),
            &Token::new(Word, "whitespace2", 0)
        );
        assert_eq!(
            tokenizer.look_ahead_with_whitespace(),
            &Token::new(Whitespace, " ", 0)
        );
        assert_eq!(
            tokenizer.look_ahead_with_whitespace(),
            &Token::new(Whitespace, " ", 0)
        );
        assert_eq!(tokenizer.read_token(), &Token::new(Word, "whitespace3", 0));
        assert_eq!(tokenizer.read_token(), &Token::new(Eof, "", 0));
    }
}
