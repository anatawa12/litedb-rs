use crate::bson;
use crate::utils::{CaseInsensitiveString, Collation};
use itertools::Itertools as _;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::rc::Rc;
use typed_arena::Arena;

mod functions;
mod methods;
mod operator;
mod parser;
mod tokenizer;

type Error = super::Error;

impl Error {
    fn expr_error(str: &str) -> Self {
        Self::err(format!("parsing: {}", str))
    }
    fn expr_run_error(str: &str) -> Self {
        Self::err(format!("executing: {}", str))
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

type ValueIterator<'a, 'b> = Box<dyn IEnumerable<'a, 'b> + 'b>;

trait IEnumerable<'a, 'b>: Iterator<Item = super::Result<&'a bson::Value>> {
    fn box_clone(&self) -> ValueIterator<'a, 'b>;
}

impl<'a, 'b, T> IEnumerable<'a, 'b> for T
where
    T: Iterator<Item = super::Result<&'a bson::Value>> + Clone + 'b,
{
    fn box_clone(&self) -> ValueIterator<'a, 'b> {
        Box::new(Clone::clone(self))
    }
}

impl Clone for ValueIterator<'_, '_> {
    fn clone(&self) -> Self {
        IEnumerable::box_clone(self.as_ref())
    }
}

type ScalarExpr = Rc<dyn for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<&'ctx bson::Value>>;
type SequenceExpr =
    Rc<dyn for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<ValueIterator<'ctx, 'ctx>>>;

#[derive(Clone)]
enum Expression {
    Scalar(ScalarExpr),
    Sequence(SequenceExpr),
}

impl Expression {
    pub fn scalar(
        scalar: impl for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<&'ctx bson::Value> + 'static,
    ) -> Self {
        Self::Scalar(scalar_expr(scalar))
    }

    pub(crate) fn execute(
        self,
        ctx: ExecutionContext,
    ) -> impl Iterator<Item = super::Result<&bson::Value>> + Clone + use<'_> {
        match self {
            Expression::Scalar(expr) => {
                either::Either::Left(std::iter::once_with(move || expr(&ctx)))
            }
            Expression::Sequence(expr) => either::Either::Right(
                std::iter::once_with(move || expr(&ctx))
                    .flatten_ok()
                    .map(|x| x.and_then(|x| x)),
            ),
        }
    }

    pub(crate) fn execute_scalar<'a>(
        &self,
        ctx: ExecutionContext<'a>,
    ) -> super::Result<&'a bson::Value> {
        match self {
            Expression::Scalar(expr) => expr(&ctx),
            Expression::Sequence(_) => Err(super::Error::expr_run_error(
                "Expression is not a scalar expression and can return more than one result",
            )),
        }
    }
}

impl Debug for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Scalar(_) => f.write_str("Scalar(_)"),
            Expression::Sequence(_) => f.write_str("Sequence(_)"),
        }
    }
}

impl From<ScalarExpr> for Expression {
    fn from(expr: ScalarExpr) -> Self {
        Expression::Scalar(expr)
    }
}

impl From<SequenceExpr> for Expression {
    fn from(expr: SequenceExpr) -> Self {
        Expression::Sequence(expr)
    }
}

fn scalar_expr(
    scalar: impl for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<&'ctx bson::Value> + 'static,
) -> ScalarExpr {
    Rc::new(scalar)
}

fn sequence_expr(
    sequence: impl for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<ValueIterator<'ctx, 'ctx>>
    + 'static,
) -> SequenceExpr {
    Rc::new(sequence)
}

#[derive(Clone)]
struct ExecutionContext<'a> {
    source: ValueIterator<'a, 'a>,
    root: Option<&'a bson::Value>,
    current: Option<&'a bson::Value>,
    collation: Collation,
    parameters: &'a bson::Document,
    arena: &'a Arena<bson::Value>,
}

impl<'a> ExecutionContext<'a> {
    fn arena(&self, value: bson::Value) -> &'a bson::Value {
        self.arena.alloc(value)
    }

    fn bool(&self, b: bool) -> &'a bson::Value {
        if b {
            &bson::Value::Boolean(true)
        } else {
            &bson::Value::Boolean(false)
        }
    }

    fn subcontext_root_item(&self, item: &'a bson::Value) -> ExecutionContext<'a> {
        let root = self.root;
        ExecutionContext::<'a> {
            source: Box::new(std::iter::once_with(move || root.unwrap()).map(Ok)),
            current: Some(item),
            root: self.root,
            collation: self.collation,
            parameters: self.parameters,
            arena: self.arena,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(private_interfaces)] // expr is not part of intended api
pub struct BsonExpression<Expr = Expression> {
    r#type: BsonExpressionType,
    is_immutable: bool,
    use_source: bool,
    fields: HashSet<CaseInsensitiveString>,
    left: Option<Box<BsonExpression>>,
    right: Option<Box<BsonExpression>>,
    source: String,
    expression: Expr,
}

type ScalarBsonExpression = BsonExpression<ScalarExpr>;
type SequenceBsonExpression = BsonExpression<SequenceExpr>;

impl BsonExpression {
    fn is_scalar(&self) -> bool {
        matches!(self.expression, Expression::Scalar(_))
    }

    fn into_scalar_or(self) -> Result<ScalarBsonExpression, SequenceBsonExpression> {
        match self.expression {
            Expression::Scalar(expr) => {
                Ok(ScalarBsonExpression {
                    r#type: self.r#type,
                    //parameters: self.parameters,
                    is_immutable: self.is_immutable,
                    use_source: self.use_source,
                    // is_scalar: true,
                    fields: self.fields,
                    expression: expr,
                    source: self.source,
                    left: self.left,
                    right: self.right,
                })
            }
            Expression::Sequence(expr) => {
                Err(SequenceBsonExpression {
                    r#type: self.r#type,
                    //parameters: self.parameters,
                    is_immutable: self.is_immutable,
                    use_source: self.use_source,
                    // is_scalar: true,
                    fields: self.fields,
                    expression: expr,
                    source: self.source,
                    left: self.left,
                    right: self.right,
                })
            }
        }
    }

    fn into_sequence_or(self) -> Result<SequenceBsonExpression, ScalarBsonExpression> {
        match self.into_scalar_or() {
            Ok(v) => Err(v),
            Err(v) => Ok(v),
        }
    }
}

impl From<ScalarBsonExpression> for BsonExpression {
    fn from(expr: ScalarBsonExpression) -> Self {
        Self {
            r#type: expr.r#type,
            is_immutable: expr.is_immutable,
            use_source: expr.use_source,
            fields: expr.fields,
            left: expr.left,
            right: expr.right,
            source: expr.source,
            expression: expr.expression.into(),
        }
    }
}

impl From<SequenceBsonExpression> for BsonExpression {
    fn from(expr: SequenceBsonExpression) -> Self {
        Self {
            r#type: expr.r#type,
            is_immutable: expr.is_immutable,
            use_source: expr.use_source,
            fields: expr.fields,
            left: expr.left,
            right: expr.right,
            source: expr.source,
            expression: expr.expression.into(),
        }
    }
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
            TokenType::Word => matches!(
                self.value.to_uppercase().as_str(),
                "BETWEEN" | "LIKE" | "IN" | "AND" | "OR"
            ),
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

fn is_letter(c: char) -> bool {
    use unicode_properties::*;
    matches!(
        c.general_category(),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
    )
}

fn is_letter_or_digit(c: char) -> bool {
    use unicode_properties::*;
    matches!(
        c.general_category(),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
            | GeneralCategory::DecimalNumber
    )
}

fn is_word_char(c: char, first: bool) -> bool {
    if first {
        is_letter(c) || c == '_' || c == '$'
    } else {
        is_letter_or_digit(c) || c == '_' || c == '$'
    }
}
