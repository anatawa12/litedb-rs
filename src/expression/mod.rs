use crate::bson;
use crate::expression::parser::DocumentScope;
use crate::expression::tokenizer::Tokenizer;
use crate::utils::{CaseInsensitiveString, Collation, OrdBsonValue};
use itertools::Itertools as _;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, LazyLock};

mod functions;
mod methods;
mod operator;
mod parser;
mod tokenizer;

/// The type represents expression parsing error
#[derive(Debug)]
pub struct ParseError(String);
// TODO: distinguishable with enum

impl ParseError {
    fn bad_invocation(f: &str) -> Self {
        Self(format!("Bad invocation of {}", f))
    }

    #[inline]
    fn unexpected_sequence(position: std::fmt::Arguments) -> Self {
        Self(format!(
            "Scalar expression is expected, but sequence is provided at {position}"
        ))
    }

    #[allow(dead_code)]
    #[inline]
    fn unexpected_scalar(position: std::fmt::Arguments) -> Self {
        Self(format!(
            "Sequence expression is expected, but scalar is provided at {position}"
        ))
    }

    #[inline]
    fn unsupported(thing: std::fmt::Arguments) -> Self {
        Self(format!("Unsupported expression: {}", thing))
    }

    #[inline]
    fn unexpected_token(token: &Token, message: std::fmt::Arguments) -> Self {
        if token.typ == TokenType::String {
            Self(format!(r#"unexpected token: {message}: "{}""#, token.value))
        } else {
            Self(format!(r#"unexpected token: {message}: {}"#, token.value))
        }
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type Error = super::Error;

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

pub trait IEnumerable<'a, 'b>:
    Iterator<Item = super::Result<&'a bson::Value>> + Sync + Send
{
    fn box_clone(&self) -> ValueIterator<'a, 'b>;
}

impl<'a, 'b, T> IEnumerable<'a, 'b> for T
where
    T: Iterator<Item = super::Result<&'a bson::Value>> + Clone + Sync + Send + 'b,
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

type ScalarExpr = Arc<
    dyn for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<&'ctx bson::Value> + Send + Sync,
>;
type SequenceExpr = Arc<
    dyn for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<ValueIterator<'ctx, 'ctx>>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub enum Expression {
    Scalar(ScalarExpr),
    Sequence(SequenceExpr),
}

impl Expression {
    pub fn scalar(
        scalar: impl for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<&'ctx bson::Value>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self::Scalar(scalar_expr(scalar))
    }

    pub(crate) fn execute(
        self,
        ctx: ExecutionContext<'_>,
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

    pub(crate) fn execute_ref<'a>(
        &self,
        ctx: ExecutionContext<'a>,
    ) -> impl Iterator<Item = super::Result<&'a bson::Value>> + Clone + Sync + Send + use<'_, 'a>
    {
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
    scalar: impl for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<&'ctx bson::Value>
    + Send
    + Sync
    + 'static,
) -> ScalarExpr {
    Arc::new(scalar)
}

fn sequence_expr(
    sequence: impl for<'ctx> Fn(&ExecutionContext<'ctx>) -> super::Result<ValueIterator<'ctx, 'ctx>>
    + Send
    + Sync
    + 'static,
) -> SequenceExpr {
    Arc::new(sequence)
}

type Arena = thread_local::ThreadLocal<typed_arena::Arena<bson::Value>>;

#[derive(Clone)]
pub struct ExecutionContext<'a> {
    source: ValueIterator<'a, 'a>,
    root: Option<&'a bson::Value>,
    current: Option<&'a bson::Value>,
    collation: Collation,
    parameters: &'a bson::Document,
    arena: &'a Arena,
}

impl<'a> ExecutionContext<'a> {
    fn new(root: &'a bson::Value, collation: Collation, arena: &'a Arena) -> Self {
        static EMPTY_DOCUMENT: LazyLock<bson::Document> = LazyLock::new(bson::Document::new);

        Self {
            source: Box::new(std::iter::once_with(move || root).map(Ok)),
            current: Some(root),
            root: Some(root),
            collation,
            parameters: &EMPTY_DOCUMENT,
            arena,
        }
    }

    fn arena(&self, value: bson::Value) -> &'a bson::Value {
        self.arena.get_or(typed_arena::Arena::new).alloc(value)
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
    pub fn create(expr: &str) -> Result<Self, ParseError> {
        let mut tokenizer = Tokenizer::new(expr);
        parser::parse_full_expression(&mut tokenizer, DocumentScope::Root)
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub(crate) fn is_indexable(&self) -> bool {
        !self.fields.is_empty() && self.is_immutable
    }
}

impl BsonExpression {
    pub(crate) fn is_scalar(&self) -> bool {
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

impl<T> Display for BsonExpression<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, f)
    }
}

pub(crate) struct ExecutionScope {
    arena: Arena,
    collation: Collation,
}

impl ExecutionScope {
    pub(crate) fn new(collation: Collation) -> Self {
        Self {
            arena: Arena::new(),
            collation,
        }
    }

    pub(crate) fn execute<'a, 'b>(
        &'a self,
        expression: &'b BsonExpression,
        root: &'a bson::Value,
    ) -> impl Iterator<Item = super::Result<&'a bson::Value>> + Clone + Sync + Send + use<'a, 'b>
    {
        let context = ExecutionContext::new(root, self.collation, &self.arena);
        expression.expression.execute_ref(context)
    }

    pub(crate) fn get_index_keys<'a, 'b>(
        &'a self,
        expression: &'b BsonExpression,
        root: &'a bson::Value,
    ) -> impl Iterator<Item = super::Result<&'a bson::Value>> + Clone + Sync + Send + use<'a, 'b>
    {
        let mut values = BTreeSet::new();
        self.execute(expression, root)
            .filter_ok(move |&x| values.insert(OrdBsonValue(x)))
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
    #[allow(dead_code)]
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
                self.value.to_ascii_uppercase().as_str(),
                "BETWEEN" | "LIKE" | "IN" | "AND" | "OR"
            ),
            _ => false,
        }
    }

    fn expect_token(&self, token: &str) -> Result<&Self, ParseError> {
        if !self.is(token) {
            return Err(ParseError::unexpected_token(
                self,
                format_args!("expected {token}"),
            ));
        }
        Ok(self)
    }
}

trait ExpectTypeTrait: Sized {
    fn token(&self) -> &Token<'_>;

    fn expect_type<const N: usize>(self, types: [TokenType; N]) -> Result<Self, ParseError> {
        if !types.iter().any(|x| x == &self.token().typ) {
            return Err(ParseError::unexpected_token(
                self.token(),
                format_args!("expected one of {types:?}"),
            ));
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
