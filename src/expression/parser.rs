use crate::expression::tokenizer::Tokenizer;
use crate::expression::{BsonExpression, BsonExpressionType, ExpectTypeTrait, TokenType};
use crate::utils::CaseInsensitiveString;
use std::collections::HashSet;
use std::ops::Neg;

type Result<T, R = super::Error> = std::result::Result<T, R>;

type LiteException = super::Error;

enum MethodParamererType {
    ValueEnumerable,
    Value,
}

impl MethodParamererType {
    fn is_enumerable(&self) -> bool {
        matches!(self, Self::ValueEnumerable)
    }
}

struct MethodInfo {
    name: &'static str,
    volatile: bool,
    parameters: Vec<MethodParamererType>,
    is_enumerable: bool,
}

fn get_method(name: &str, params: usize) -> Option<&MethodInfo> {
    // TODO: currently removed support for method
    None
}

#[derive(Debug, Copy, Clone)]
pub(super) enum BsonBinaryExpression {
    Mod,
    Divide,
    Multiply,
    Add,
    Minus,
    Like,
    Between,
    In,
    Gt,
    Gte,
    Lt,
    Lte,
    Neq,
    Eq,
    LikeAny,
    BetweenAny,
    InAny,
    GtAny,
    GteAny,
    LtAny,
    LteAny,
    NeqAny,
    EqAny,
    LikeAll,
    BetweenAll,
    InAll,
    GtAll,
    GteAll,
    LtAll,
    LteAll,
    NeqAll,
    EqAll,
}

trait TryOrElse<T>: Sized {
    fn try_or_else<E, F: FnOnce() -> Result<Self, E>>(self, f: F) -> Result<Self, E>;
}

// Proposed new API for Option https://github.com/rust-lang/libs-team/issues/59
// This would actually use `self` there of course.
impl<T> TryOrElse<T> for Option<T> {
    fn try_or_else<E, F: FnOnce() -> Result<Self, E>>(self, f: F) -> Result<Self, E> {
        if let Some(v) = self { Ok(Some(v)) } else { f() }
    }
}

trait StrExtension {
    fn as_str(&self) -> &str;
    fn is_word(&self) -> bool {
        self.as_str().chars().enumerate().all(|(i, c)| super::is_word_char(c, i == 0))
    }
}

impl StrExtension for str {
    fn as_str(&self) -> &str {
        self
    }
}

impl StrExtension for String {
    fn as_str(&self) -> &str {
        self
    }
}

fn append_quoted(mut str: &str, builder: &mut String) {
    builder.push('"');
    while let Some((left, right)) = str.split_once('"') {
        builder.push_str(left);
        builder.push_str("\\\"");
        str = right;
    }
    builder.push_str(str);
    builder.push('"');
}

type BsonDocument = crate::bson::Document;
type BsonValue = crate::bson::Value;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) enum DocumentScope {
    Source,
    Root,
    Current,
}

#[derive(Debug, Clone)]
pub(super) enum Expression {
    Constant(BsonValue),
    Member(Box<Expression>, String),
    Map(Box<Expression>, Box<BsonExpression>),
    ParameterPath(String),
    DocumentInit(Vec<String>, Vec<Expression>),
    Function(
        &'static str,
        Box<Expression>,
        Option<Box<BsonExpression>>,
        Vec<Expression>,
    ),
    Method(&'static str, Vec<Expression>),
    Items(Box<Expression>),
    Binary(BsonBinaryExpression, Box<Expression>, Box<Expression>),
    AndAlso(Box<Expression>, Box<Expression>),
    OrElse(Box<Expression>, Box<Expression>),
    Condition(Box<Expression>, Box<Expression>, Box<Expression>),

    Root,
    Current,
    Source,
    ArrayInit(Vec<Expression>),
    ArrayIndex(Box<Expression>, i32),
    ArrayIndexParam(Box<Expression>, Box<Expression>),
    ArrayIndexExpr(Box<Expression>, Box<BsonExpression>),
    ArrayAsterisk(Box<Expression>),
    ArrayAsteriskFilter(Box<Expression>, Box<BsonExpression>),
}

fn inner(expression: BsonExpression) -> Option<Box<BsonExpression>> {
    Some(Box::new(expression))
}

// region Operators quick access

/// <summary>
/// Operation definition by methods with defined expression type (operators are in precedence order)
/// </summary>
static OPERATORS: &[(&str, BsonBinaryExpression, BsonExpressionType)] = &[
    // arithmetic
    (
        "%",
        BsonBinaryExpression::Mod,
        BsonExpressionType::Modulo,
    ),
    (
        "/",
        BsonBinaryExpression::Divide,
        BsonExpressionType::Divide,
    ),
    (
        "*",
        BsonBinaryExpression::Multiply,
        BsonExpressionType::Multiply,
    ),
    ("+", BsonBinaryExpression::Add, BsonExpressionType::Add),
    (
        "-",
        BsonBinaryExpression::Minus,
        BsonExpressionType::Subtract,
    ),
    // predicate
    (
        "LIKE",
        BsonBinaryExpression::Like,
        BsonExpressionType::Like,
    ),
    (
        "BETWEEN",
        BsonBinaryExpression::Between,
        BsonExpressionType::Between,
    ),
    (
        "IN",
        BsonBinaryExpression::In,
        BsonExpressionType::In,
    ),
    (
        ">",
        BsonBinaryExpression::Gt,
        BsonExpressionType::GreaterThan,
    ),
    (
        ">=",
        BsonBinaryExpression::Gte,
        BsonExpressionType::GreaterThanOrEqual,
    ),
    (
        "<",
        BsonBinaryExpression::Lt,
        BsonExpressionType::LessThan,
    ),
    (
        "<=",
        BsonBinaryExpression::Lte,
        BsonExpressionType::LessThanOrEqual,
    ),
    (
        "!=",
        BsonBinaryExpression::Neq,
        BsonExpressionType::NotEqual,
    ),
    (
        "=",
        BsonBinaryExpression::Eq,
        BsonExpressionType::Equal,
    ),
    (
        "ANY LIKE",
        BsonBinaryExpression::LikeAny,
        BsonExpressionType::Like,
    ),
    (
        "ANY BETWEEN",
        BsonBinaryExpression::BetweenAny,
        BsonExpressionType::Between,
    ),
    (
        "ANY IN",
        BsonBinaryExpression::InAny,
        BsonExpressionType::In,
    ),
    (
        "ANY>",
        BsonBinaryExpression::GtAny,
        BsonExpressionType::GreaterThan,
    ),
    (
        "ANY>=",
        BsonBinaryExpression::GteAny,
        BsonExpressionType::GreaterThanOrEqual,
    ),
    (
        "ANY<",
        BsonBinaryExpression::LtAny,
        BsonExpressionType::LessThan,
    ),
    (
        "ANY<=",
        BsonBinaryExpression::LteAny,
        BsonExpressionType::LessThanOrEqual,
    ),
    (
        "ANY!=",
        BsonBinaryExpression::NeqAny,
        BsonExpressionType::NotEqual,
    ),
    (
        "ANY=",
        BsonBinaryExpression::EqAny,
        BsonExpressionType::Equal,
    ),
    (
        "ALL LIKE",
        BsonBinaryExpression::LikeAll,
        BsonExpressionType::Like,
    ),
    (
        "ALL BETWEEN",
        BsonBinaryExpression::BetweenAll,
        BsonExpressionType::Between,
    ),
    (
        "ALL IN",
        BsonBinaryExpression::InAll,
        BsonExpressionType::In,
    ),
    (
        "ALL>",
        BsonBinaryExpression::GtAll,
        BsonExpressionType::GreaterThan,
    ),
    (
        "ALL>=",
        BsonBinaryExpression::GteAll,
        BsonExpressionType::GreaterThanOrEqual,
    ),
    (
        "ALL<",
        BsonBinaryExpression::LtAll,
        BsonExpressionType::LessThan,
    ),
    (
        "ALL<=",
        BsonBinaryExpression::LteAll,
        BsonExpressionType::LessThanOrEqual,
    ),
    (
        "ALL!=",
        BsonBinaryExpression::NeqAll,
        BsonExpressionType::NotEqual,
    ),
    (
        "ALL=",
        BsonBinaryExpression::EqAll,
        BsonExpressionType::Equal,
    ),
    // logic (will use Expression.AndAlso|OrElse)
    (
        "AND",
        BsonBinaryExpression::Mod,
        BsonExpressionType::And,
    ),
    (
        "OR",
        BsonBinaryExpression::Mod,
        BsonExpressionType::Or,
    ),
];

/*
private static readonly MethodInfo _parameterPathMethod = M("PARAMETER_PATH");
private static readonly MethodInfo _memberPathMethod = M("MEMBER_PATH");
private static readonly MethodInfo _arrayIndexMethod = M("ARRAY_INDEX");
private static readonly MethodInfo _arrayFilterMethod = M("ARRAY_FILTER");

private static readonly MethodInfo _documentInitMethod = M("DOCUMENT_INIT");
private static readonly MethodInfo _arrayInitMethod = M("ARRAY_INIT");

private static readonly MethodInfo _itemsMethod = typeof(BsonExpressionMethods).GetMethod("ITEMS");
private static readonly MethodInfo _arrayMethod = typeof(BsonExpressionMethods).GetMethod("ARRAY");
// */

// endregion

/// <summary>
/// Start parse string into linq expression. Read path, function or base type bson values (int, double, bool, string)
/// </summary>
pub fn parse_full_expression(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<BsonExpression> {
    let first = parse_single_expression(tokenizer, parameters, scope)?;
    let mut values = vec![first];
    let mut ops = vec![];

    // read all blocks and operation first
    while !tokenizer.eof() {
        // read operator between expressions
        let Some(op) = read_operant(tokenizer)? else {
            break;
        };

        let mut expr = parse_single_expression(tokenizer, parameters, scope)?;

        // special BETWEEN "AND" read
        // rustchange: we changed to upper in ReadOperant so we use simple ends with
        if op.ends_with("BETWEEN") {
            tokenizer.read_token().expect_token("AND")?;

            let expr2 = parse_single_expression(tokenizer, parameters, scope)?;

            // convert expr and expr2 into an array with 2 values
            expr = new_array(expr, expr2)?;
        }

        values.push(expr);
        ops.push(op.to_uppercase());
    }

    let mut order = 0;

    // now, process operator in correct order
    while values.len() >= 2 {
        let &(op, method, r#type) = &OPERATORS[order];
        //let n = ops.iter().position(|o| o == op.0);

        if let Some(n) = ops.iter().position(|o| o == op) {
            // get left/right values to execute operator
            let mut left = values.remove(n);
            let right = values.remove(n);

            // test left/right scalar
            let is_left_enum = op.starts_with("ALL") || op.starts_with("ANY");

            if is_left_enum && left.is_scalar {
                left = convert_to_enumerable(left);
            }
            //if is_left_enum && left.is_scalar { return Err(LiteException::expr_error(&format!("Left expression `{}` must return multiples values", left.source))); }
            if !is_left_enum && !left.is_scalar {
                return Err(LiteException::expr_error(&format!(
                    "Left expression `{}` returns more than one result. Try use ANY or ALL before operant.",
                    left.source
                )));
            }
            if !is_left_enum && !right.is_scalar {
                return Err(LiteException::expr_error(&format!(
                    "Left expression `{}` must return a single value",
                    right.source
                )));
            }
            if !right.is_scalar {
                return Err(LiteException::expr_error(&format!(
                    "Right expression `{}` must return a single value",
                    right.source
                )));
            }


            // when operation is AND/OR, use AndAlso|OrElse
            let result= if r#type == BsonExpressionType::And || r#type == BsonExpressionType::Or {
                create_logic_expression(r#type, left, right)
            } else {
                // method call parameters

                let pre_space = if op.as_bytes()[0].is_ascii_alphabetic() { " " } else {""};
                let post_space = if op.as_bytes()[op.len() - 1].is_ascii_alphabetic() { " " } else {""};

                // process result in a single value
                BsonExpression {
                    r#type,
                    //parameters: parameters,
                    is_immutable: left.is_immutable && right.is_immutable,
                    use_source: left.use_source || right.use_source,
                    is_scalar: true,
                    fields: left
                        .fields
                        .iter()
                        .cloned()
                        .chain(right.fields.iter().cloned())
                        .collect(),
                    expression: Expression::Binary(
                        method,
                        Box::new(left.expression.clone()),
                        Box::new(right.expression.clone()),
                    ),
                    source: format!("{}{}{}{}{}", left.source, pre_space, op, post_space, right.source),
                    left: inner(left),
                    right: inner(right),
                }
            };

            // remove left+right and insert result
            values.insert(n, result);
            //values.RemoveRange(n + 1, 2);

            // remove operation
            ops.remove(n);
        } else {
            order += 1;
        }
    }

    Ok(values.remove(0))
}

/// <summary>
/// Start parse string into linq expression. Read path, function or base type bson values (int, double, bool, string)
/// </summary>
pub fn parse_single_expression(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<BsonExpression> {
    // read next token and test with all expression parts
    let token = tokenizer.read_token().clone();

    try_parse_double(tokenizer, parameters)?
        .try_or_else(|| try_parse_int(tokenizer, parameters))?
        .or_else(|| try_parse_bool(tokenizer, parameters))
        .or_else(|| try_parse_null(tokenizer, parameters))
        .or_else(|| try_parse_string(tokenizer, parameters))
        .try_or_else(|| try_parse_source(tokenizer, parameters, scope))?
        .try_or_else(|| try_parse_document(tokenizer, parameters, scope))?
        .try_or_else(|| try_parse_array(tokenizer, parameters, scope))?
        .or_else(|| try_parse_parameter(tokenizer, parameters, scope))
        .try_or_else(|| try_parse_inner_expression(tokenizer, parameters, scope))?
        .try_or_else(|| try_parse_function(tokenizer, parameters, scope))?
        .try_or_else(|| try_parse_method_call(tokenizer, parameters, scope))?
        .try_or_else(|| try_parse_path(tokenizer, parameters, scope))?
        .ok_or_else(|| LiteException::unexpected_token("unexpected token", &token))
}

/// <summary>
/// Parse a document builder syntax used in SELECT statment: {expr0} [AS] [{alias}], {expr1} [AS] [{alias}], ...
/// </summary>
#[cfg(any())] // rust: disable for now
pub fn ParseSelectDocumentBuilder(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
) -> Result<BsonExpression> {
    // creating unique field names
    let fields = vec![];
    let names = HashSet::new();
    let counter = 1;

    // define when next token means finish reading document builder
    //bool stop(Token t) => t.is("FROM") || t.is("INTO") || t.r#type == TokenType::EOF || t.r#type == TokenType::SemiColon;

    /*
    void Add(alias: string, expr: BsonExpression)
    {
        if (names.Contains(alias)) { alias += counter++; }

        names.Add(alias);

        if (!expr.is_scalar) { expr = convert_to_array(expr); }

        fields.push((alias, expr));
    };
     */

    while (true) {
        let expr = parse_full_expression(tokenizer, parameters, DocumentScope::Root)?;

        let next = tokenizer.look_ahead();

        // finish reading
        if (stop(next)) {
            Add(expr.DefaultFieldName(), expr);

            break;
        }
        // field with no alias
        if (next.Type == TokenType::Comma) {
            tokenizer.read_token(); // consume ,

            Add(expr.DefaultFieldName(), expr);
        }
        // using alias
        else {
            if (next.is("AS")) {
                tokenizer.read_token(); // consume "AS"
            }

            let mut alias = tokenizer.read_token().Expect(TokenType::Word);

            Add(alias.Value, expr);

            // go ahead to next token to see if last field
            next = tokenizer.look_ahead();

            if (stop(next)) {
                break;
            }

            // consume ,
            tokenizer.read_token().Expect(TokenType::Comma);
        }
    }

    let mut first = fields[0].Value;

    if (fields.Count == 1) {
        // if just $ return empty BsonExpression
        if (first.Type == BsonExpressionType::Path && first.Source == "$") {
            return BsonExpression.Root;
        }

        // if single field already a document
        if (fields.Count == 1 && first.Type == BsonExpressionType::Document) {
            return first;
        }

        // special case: EXTEND method also returns only a document
        if (fields.Count == 1
            && first.Type == BsonExpressionType::Call
            && first.Source.StartsWith("EXTEND"))
        {
            return first;
        }
    }

    /*
       let mut arrKeys = expression.NewArrayInit(typeof(string), fields.Select(x => expression.Constant(x.Key)).ToArray());
       let mut arrValues = expression.NewArrayInit(typeof(BsonValue), fields.Select(x => x.value.expression).ToArray());

       return Ok(BsonExpression
       {
           r#type: BsonExpressionType::Document,
           //parameters: parameters,
           is_immutable: fields.All(x => x.value.is_immutable),
           use_source: fields.Any(x => x.value.use_source),
           is_scalar: true,
           fields: HashSet::new().AddRange(fields.SelectMany(x => x.value.fields)),
           expression: expression.Call(_documentInitMethod, new expression[] { arrKeys, arrValues }),
           source: "{" + string.Join(",", fields.Select(x => x.Key + ":" + x.value.source)) + "}",
           left: None, right: None,
       });
    */
}

/// <summary>
/// Parse a document builder syntax used in UPDATE statment:
/// {key0} = {expr0}, .... will be converted into { key: [expr], ... }
/// {key: value} ... return return a new document
/// </summary>
pub fn parse_update_document_builder(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
) -> Result<BsonExpression> {
    let next = tokenizer.look_ahead();

    // if starts with { just return a normal document expression
    if next.typ == TokenType::OpenBrace {
        tokenizer.read_token(); // consume {

        return Ok(try_parse_document(tokenizer, parameters, DocumentScope::Root)?.unwrap());
    }

    let mut keys = vec![];
    let mut values = vec![];
    let mut src = String::new();
    let mut is_immutable = true;
    let mut use_source = false;
    let mut fields = HashSet::new();

    src.push('{');

    while !tokenizer.check_eof()? {
        let key = read_key(tokenizer, &mut src)?;

        tokenizer.read_token().expect_type([TokenType::Equals])?;

        src.push(':');

        let mut value = parse_full_expression(tokenizer, parameters, DocumentScope::Root)?;

        if !value.is_scalar {
            value = convert_to_array(value);
        }

        // update is_immutable only when came false
        if !value.is_immutable {
            is_immutable = false;
        }
        if value.use_source {
            use_source = true;
        }

        fields.extend(value.fields.iter().cloned());

        // add key and value to parameter list (as an expression)
        keys.push(key);
        values.push(value.expression);

        src.push_str(&value.source);

        // read ,
        if tokenizer.look_ahead().typ == TokenType::Comma {
            src.push_str(&tokenizer.read_token().value);
            continue;
        }
        break;
    }

    src.push('}');

    // create linq expression for "{ doc }"
    let doc_expr = Expression::DocumentInit(keys, values);

    Ok(BsonExpression {
        r#type: BsonExpressionType::Document,
        //parameters: parameters,
        is_immutable,
        use_source,
        is_scalar: true,
        fields,
        expression: doc_expr,
        source: src,

        left: None,
        right: None,
    })
}

// region Constants

/// <summary>
/// Try parse double number - return null if not double token
/// </summary>
fn try_parse_double(
    tokenizer: &mut Tokenizer,
    _parameters: &BsonDocument,
) -> Result<Option<BsonExpression>> {
    let mut value: Option<f64> = None;

    if tokenizer.current().typ == TokenType::Double {
        value = Some(tokenizer.current().value.parse()?);
    } else if tokenizer.current().typ == TokenType::Minus {
        let ahead = tokenizer.look_ahead_with_whitespace();

        if ahead.typ == TokenType::Double {
            value = Some(tokenizer.read_token().value.parse().map(f64::neg)?);
        }
    }

    if let Some(number) = value {
        let constant = Expression::Constant(BsonValue::Double(number));

        return Ok(Some(BsonExpression {
            r#type: BsonExpressionType::Double,
            //parameters = parameters,
            is_immutable: true,
            use_source: false,
            is_scalar: true,
            fields: HashSet::new(),
            expression: constant,
            source: format!("{}", number),
            left: None,
            right: None,
        }));
    }

    Ok(None)
}

/// <summary>
/// Try parse int number - return null if not int token
/// </summary>
fn try_parse_int(
    tokenizer: &mut Tokenizer,
    _parameters: &BsonDocument,
) -> Result<Option<BsonExpression>> {
    let mut value: Option<i64> = None;

    if tokenizer.current().typ == TokenType::Int {
        value = Some(tokenizer.current().value.parse()?);
    } else if tokenizer.current().typ == TokenType::Minus {
        let ahead = tokenizer.look_ahead_with_whitespace();

        if ahead.typ == TokenType::Int {
            value = Some(-tokenizer.read_token().value.parse::<i64>()?)
        }
    }

    if let Some(i64) = value {
        if let Ok(i32) = i32::try_from(i64) {
            let constant32 = Expression::Constant(BsonValue::Int32(i32));

            return Ok(Some(BsonExpression {
                r#type: BsonExpressionType::Int,
                //parameters: parameters,
                is_immutable: true,
                use_source: false,
                is_scalar: true,
                fields: HashSet::new(),
                expression: constant32,
                source: format!("{i32}"),
                left: None,
                right: None,
            }));
        }

        let constant64 = Expression::Constant(BsonValue::Int64(i64));

        return Ok(Some(BsonExpression {
            r#type: BsonExpressionType::Int,
            //parameters: parameters,
            is_immutable: true,
            use_source: false,
            is_scalar: true,
            fields: HashSet::new(),
            expression: constant64,
            source: format!("{i64}"),
            left: None,
            right: None,
        }));
    }
    Ok(None)
}

/// <summary>
/// Try parse bool - return null if not bool token
/// </summary>
fn try_parse_bool(tokenizer: &mut Tokenizer, _parameters: &BsonDocument) -> Option<BsonExpression> {
    if tokenizer.current().typ == TokenType::Word
        && (tokenizer.current().is("true") || tokenizer.current().is("false"))
    {
        let boolean = tokenizer.current().value.eq_ignore_ascii_case("true");
        let constant = Expression::Constant(BsonValue::Boolean(boolean));

        return Some(BsonExpression {
            r#type: BsonExpressionType::Boolean,
            //parameters: parameters,
            is_immutable: true,
            use_source: false,
            is_scalar: true,
            fields: HashSet::new(),
            expression: constant,
            source: format!("{}", boolean),
            left: None,
            right: None,
        });
    }

    None
}

/// <summary>
/// Try parse null constant - return null if not null token
/// </summary>
fn try_parse_null(tokenizer: &mut Tokenizer, _parameters: &BsonDocument) -> Option<BsonExpression> {
    if tokenizer.current().typ == TokenType::Word && tokenizer.current().is("null") {
        let constant = Expression::Constant(BsonValue::Null);

        return Some(BsonExpression {
            r#type: BsonExpressionType::Null,
            //parameters: parameters,
            is_immutable: true,
            use_source: false,
            is_scalar: true,
            fields: HashSet::new(),
            expression: constant,
            source: "null".into(),

            left: None,
            right: None,
        });
    }

    None
}

/// <summary>
/// Try parse string with both single/double quote - return null if not string
/// </summary>
fn try_parse_string(
    tokenizer: &mut Tokenizer,
    _parameters: &BsonDocument,
) -> Option<BsonExpression> {
    if tokenizer.current().typ == TokenType::String {
        let str = tokenizer.take_current().value.into_owned();
        let mut source = String::new();
        append_quoted(&str, &mut source);

        let bstr = BsonValue::String(str);
        let constant = Expression::Constant(bstr);

        return Some(BsonExpression {
            r#type: BsonExpressionType::String,
            //parameters: parameters,
            is_immutable: true,
            use_source: false,
            is_scalar: true,
            fields: HashSet::new(),
            expression: constant,
            source,

            left: None,
            right: None,
        });
    }

    None
}

// endregion

/// <summary>
/// Try parse json document - return null if not document token
/// </summary>
fn try_parse_document(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    if tokenizer.current().typ != TokenType::OpenBrace {
        return Ok(None);
    }

    // read key value
    let mut keys = vec![];
    let mut values = vec![];
    let mut src = String::new();
    let mut is_immutable = true;
    let mut use_source = false;
    let mut fields = HashSet::new();

    src.push('{');

    // test for empty array
    if tokenizer.look_ahead().typ == TokenType::CloseBrace {
        src.push_str(&tokenizer.read_token().value); // read }
    } else {
        while !tokenizer.check_eof()? {
            // read simple or complex document key name
            let mut inner_src = String::new(); // use another builder to re-use in simplified notation
            let key = read_key(tokenizer, &mut inner_src)?;

            src.push_str(&inner_src);

            tokenizer.read_token(); // update s.Current 

            src.push(':');

            let mut value;

            // test normal notation { a: 1 }
            if tokenizer.current().typ == TokenType::Colon {
                value = parse_full_expression(tokenizer, parameters, scope)?;

                // read next token here (, or }) because simplified version already did
                tokenizer.read_token();
            } else {
                let fname = inner_src;

                // support for simplified notation { a, b, c } == { a: $.a, b: $.b, c: $.c }
                value = BsonExpression {
                    r#type: BsonExpressionType::Path,
                    //parameters: parameters,
                    is_immutable,
                    use_source,
                    is_scalar: true,
                    fields: HashSet::from([CaseInsensitiveString(key.clone())]),
                    expression: Expression::Member(Box::new(Expression::Root), key.clone()),
                    source: if fname.is_word() {
                        format!("$.{fname}")
                    } else {
                        format!("$.[{fname}]")
                    },
                    left: None,
                    right: None,
                };
            }

            // document value must be a scalar value
            if !value.is_scalar {
                value = convert_to_array(value);
            }

            // update is_immutable only when came false
            if !value.is_immutable {
                is_immutable = false;
            }
            if value.use_source {
                use_source = true;
            }

            fields.extend(value.fields);

            // add key and value to parameter list (as an expression)
            keys.push(key);
            values.push(value.expression);

            // include value source in current source
            src.push_str(&value.source);

            // test next token for , (continue) or } (break)
            tokenizer
                .current()
                .expect_type([TokenType::Comma, TokenType::CloseBrace])?;

            src.push_str(&tokenizer.current().value);

            if tokenizer.current().typ == TokenType::Comma {
                continue;
            }
            break;
        }
    }

    Ok(Some(BsonExpression {
        r#type: BsonExpressionType::Document,
        //parameters: parameters,
        is_immutable,
        use_source,
        is_scalar: true,
        fields,
        expression: Expression::DocumentInit(keys, values),
        source: src,
        left: None,
        right: None,
    }))
}

/// <summary>
/// Try parse source documents (when passed) * - return null if not source token
/// </summary>
fn try_parse_source(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    _scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    if tokenizer.current().typ != TokenType::Asterisk {
        return Ok(None);
    }

    let source_expr = BsonExpression {
        r#type: BsonExpressionType::Source,
        //parameters: parameters,
        is_immutable: true,
        use_source: true,
        is_scalar: false,
        fields: HashSet::from([CaseInsensitiveString("$".into())]),
        expression: Expression::Source,
        source: "*".into(),
        left: None,
        right: None,
    };

    // checks if next token is "." to shortcut from "*.Name" as "MAP(*, @.Name)"
    if tokenizer.look_ahead_with_whitespace().typ == TokenType::Period {
        tokenizer.read_token(); // consume .

        let path_expr = parse_single_expression(tokenizer, parameters, DocumentScope::Source)?;

        //if (path_expr == null) { throw LiteException.unexpected_token(tokenizer.current()); }

        Ok(Some(BsonExpression {
            r#type: BsonExpressionType::Map,
            //parameters: parameters,
            is_immutable: path_expr.is_immutable,
            use_source: true,
            is_scalar: false,
            fields: path_expr.fields.clone(),
            source: format!("MAP(*=>{})", path_expr.source),
            expression: Expression::Map(Box::new(source_expr.expression), Box::new(path_expr)),
            left: None,
            right: None,
        }))
    } else {
        Ok(Some(source_expr))
    }
}

/// <summary>
/// Try parse array - return null if not array token
/// </summary>
fn try_parse_array(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    if tokenizer.current().typ != TokenType::OpenBracket {
        return Ok(None);
    }

    let mut values = vec![];
    let mut src = String::new();
    let mut is_immutable = true;
    let mut use_source = false;
    let mut fields = HashSet::new();

    src.push('[');

    // test for empty array
    if tokenizer.look_ahead().typ == TokenType::CloseBracket {
        src.push_str(&tokenizer.read_token().value); // read ]
    } else {
        while !tokenizer.check_eof()? {
            // read value expression
            let mut value = parse_full_expression(tokenizer, parameters, scope)?;

            // document value must be a scalar value
            if !value.is_scalar {
                value = convert_to_array(value);
            }

            src.push_str(&value.source);

            // update is_immutable only when came false
            if !value.is_immutable {
                is_immutable = false;
            }
            if value.use_source {
                use_source = true;
            }

            fields.extend(value.fields);

            // include value source in current source
            values.push(value.expression);

            let next = tokenizer
                .read_token()
                .expect_type([TokenType::Comma, TokenType::CloseBracket])?;

            src.push_str(&next.value);

            if next.typ == TokenType::Comma {
                continue;
            }
            break;
        }
    }

    Ok(Some(BsonExpression {
        r#type: BsonExpressionType::Array,
        //parameters: parameters,
        is_immutable,
        use_source,
        is_scalar: true,
        fields,
        expression: Expression::ArrayInit(values),
        source: src,
        left: None,
        right: None,
    }))
}

/// <summary>
/// Try parse parameter - return null if not parameter token
/// </summary>
fn try_parse_parameter(
    tokenizer: &mut Tokenizer,
    __parameters: &BsonDocument,
    _scope: DocumentScope,
) -> Option<BsonExpression> {
    if tokenizer.current().typ != TokenType::At {
        return None;
    }

    let ahead = tokenizer.look_ahead_with_whitespace();

    if ahead.typ == TokenType::Word || ahead.typ == TokenType::Int {
        let parameter_name = tokenizer.read_token_with_whitespace().value.to_string();

        Some(BsonExpression {
            r#type: BsonExpressionType::Parameter,
            //parameters: parameters,
            is_immutable: false,
            use_source: false,
            is_scalar: true,
            fields: HashSet::new(),
            source: format!("@{parameter_name}"),
            expression: Expression::ParameterPath(parameter_name),
            left: None,
            right: None,
        })
    } else {
        None
    }
}

/// <summary>
/// Try parse inner expression - return null if not bracket token
/// </summary>
fn try_parse_inner_expression(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    if tokenizer.current().typ != TokenType::OpenParenthesis {
        return Ok(None);
    }

    // read a inner expression inside ( and )
    let inner = parse_full_expression(tokenizer, parameters, scope)?;

    // read close )
    tokenizer
        .read_token()
        .expect_type([TokenType::CloseParenthesis])?;

    Ok(Some(BsonExpression {
        r#type: inner.r#type,
        //parameters: inner.parameters,
        is_immutable: inner.is_immutable,
        use_source: inner.use_source,
        is_scalar: inner.is_scalar,
        fields: inner.fields,
        expression: inner.expression,
        left: inner.left,
        right: inner.right,
        source: format!("({})", inner.source),
    }))
}

/// <summary>
/// Try parse method call - return null if not method call
/// </summary>
fn try_parse_method_call(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    let token = tokenizer.current().clone();

    if tokenizer.current().typ != TokenType::Word {
        return Ok(None);
    }
    if tokenizer.look_ahead().typ != TokenType::OpenParenthesis {
        return Ok(None);
    }

    // read (
    tokenizer.read_token();

    // get static method from this class
    let mut pars = vec![];
    let mut src = String::new();
    let mut is_immutable = true;
    let mut use_source = false;
    let mut fields = HashSet::new();

    src.push_str(&token.value.to_uppercase());
    src.push('(');

    // method call with no parameters
    if tokenizer.look_ahead().typ == TokenType::CloseParenthesis {
        src.push_str(&tokenizer.read_token().value); // read )
    } else {
        while !tokenizer.check_eof()? {
            let parameter = parse_full_expression(tokenizer, parameters, scope)?;

            // update is_immutable only when came false
            if !parameter.is_immutable {
                is_immutable = false;
            }
            if parameter.use_source {
                use_source = true;
            }

            // add fields from each parameters
            fields.extend(parameter.fields.iter().cloned());

            // append source string
            src.push_str(&parameter.source);

            pars.push(parameter);

            // read , or )
            let next = tokenizer
                .read_token()
                .expect_type([TokenType::Comma, TokenType::CloseParenthesis])?;

            src.push_str(&next.value);

            if next.typ == TokenType::Comma {
                continue;
            }
            break;
        }
    }

    let Some(method) = get_method(&token.value, pars.len()) else {
        return Err(LiteException::unexpected_token(
            &format!(
                "Method '{}' does not exist or contains invalid parameters",
                token.value.to_uppercase()
            ),
            &token,
        ));
    };

    // test if method are decorated with "Variable" (immutable = false)
    if method.volatile {
        is_immutable = false;
    }

    // special IIF case
    if method.name == "IIF" && pars.len() == 3 {
        let [test, if_true, if_false] = pars.try_into().unwrap();
        return Ok(Some(create_conditional_expression(test, if_true, if_false)));
    }

    // method call arguments
    let mut args = vec![];

    // getting linq expression from BsonExpression for all parameters
    for (parameter, expr) in method.parameters.iter().zip(pars) {
        if !parameter.is_enumerable() && !expr.is_scalar {
            // convert enumerable expresion into scalar expression
            args.push(convert_to_array(expr).expression);
        } else if parameter.is_enumerable() && expr.is_scalar {
            // convert scalar expression into enumerable expression
            args.push(convert_to_enumerable(expr).expression);
        } else {
            args.push(expr.expression);
        }
    }

    Ok(Some(BsonExpression {
        r#type: BsonExpressionType::Call,
        //parameters: parameters,
        is_immutable,
        use_source,
        is_scalar: !method.is_enumerable,
        fields,
        expression: Expression::Method(method.name, args),
        source: src,
        left: None,
        right: None,
    }))
}

/// <summary>
/// Parse JSON-Path - return null if not method call
/// </summary>
fn try_parse_path(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    // test $ or @ or WORD
    if tokenizer.current().typ != TokenType::At
        && tokenizer.current().typ != TokenType::Dollar
        && tokenizer.current().typ != TokenType::Word
    {
        return Ok(None);
    }

    let mut default_scope = if scope == DocumentScope::Root {
        TokenType::Dollar
    } else {
        TokenType::At
    };

    if tokenizer.current().typ == TokenType::At || tokenizer.current().typ == TokenType::Dollar {
        default_scope = tokenizer.current().typ;

        let ahead = tokenizer.look_ahead_with_whitespace();

        if ahead.typ == TokenType::Period {
            tokenizer.read_token(); // read .
            tokenizer.read_token(); // read word or [
        }
    }

    let mut src = String::new();
    let mut is_immutable = true;
    let mut use_source = false;
    let mut is_scalar = true;
    let mut fields = HashSet::new();

    src.push_str(if default_scope == TokenType::Dollar {
        "$"
    } else {
        "@"
    });

    // read field name (or "" if root)
    let field = read_field(tokenizer, &mut src)?;
    let mut expr = Expression::Member(
        Box::new(if default_scope == TokenType::Dollar {
            Expression::Root
        } else {
            Expression::Current
        }),
        field.clone(),
    );

    // add as field only if working with root document (or source root)
    if default_scope == TokenType::Dollar || scope == DocumentScope::Source {
        fields.insert(if field.is_empty() {
            CaseInsensitiveString("$".to_string())
        } else {
            CaseInsensitiveString(field.clone())
        });
    }

    // parse the rest of path
    while !tokenizer.eof() {
        let result = parse_path(
            tokenizer,
            expr.clone(),
            parameters,
            &mut fields,
            &mut is_immutable,
            &mut use_source,
            &mut is_scalar,
            &mut src,
        )?;

        // filter method must exit
        let Some(result) = result else {
            break;
        };

        if !is_scalar {
            expr = result;
            break;
        }

        expr = result;
    }

    let path_expr = BsonExpression {
        r#type: BsonExpressionType::Path,
        //parameters: parameters,
        is_immutable,
        use_source,
        is_scalar,
        fields,
        expression: expr,
        source: src,
        left: None,
        right: None,
    };

    // if expr is enumerable and next token is . translate do MAP
    if !is_scalar && tokenizer.look_ahead_with_whitespace().typ == TokenType::Period {
        tokenizer.read_token(); // consume .

        let map_expr = parse_single_expression(tokenizer, parameters, DocumentScope::Current)?;

        //let Some(map_expr) = map_expr else { return Err(LiteException::unexpected_token(tokenizer.current())); };

        Ok(Some(BsonExpression {
            r#type: BsonExpressionType::Map,
            //parameters: parameters,
            is_immutable: path_expr.is_immutable && map_expr.is_immutable,
            use_source: path_expr.use_source || map_expr.use_source,
            is_scalar: false,
            fields: path_expr
                .fields
                .into_iter()
                .chain(map_expr.fields.iter().cloned())
                .collect(),
            source: format!("MAP({}=>{})", path_expr.source, map_expr.source),
            expression: Expression::Map(Box::new(path_expr.expression), Box::new(map_expr)),
            left: None,
            right: None,
        }))
    } else {
        Ok(Some(path_expr))
    }
}

/// <summary>
/// Implement a JSON-Path like navigation on BsonDocument. Support a simple range of paths
/// </summary>
fn parse_path(
    tokenizer: &mut Tokenizer,
    expr: Expression,
    parameters: &BsonDocument,
    fields: &mut HashSet<CaseInsensitiveString>,
    is_immutable: &mut bool,
    use_source: &mut bool,
    is_scalar: &mut bool,
    src: &mut String,
) -> Result<Option<Expression>> {
    let mut ahead = tokenizer.look_ahead_with_whitespace();

    if ahead.typ == TokenType::Period {
        tokenizer.read_token(); // read .
        tokenizer.read_token_with_whitespace(); //

        let field = read_field(tokenizer, src)?;

        Ok(Some(Expression::Member(Box::new(expr), field)))
    } else if ahead.typ == TokenType::OpenBracket
    // array
    {
        src.push('[');

        tokenizer.read_token(); // read [

        ahead = tokenizer.look_ahead(); // look for "index" or "expression"

        let index; // = 0;
        let inner; // = new BsonExpression();
        //let method;// = _arrayIndexMethod;

        if ahead.typ == TokenType::Int {
            // fixed index
            src.push_str(&tokenizer.read_token().value);
            index = tokenizer.current().value.parse::<i32>()?;

            // read ]
            tokenizer
                .read_token()
                .expect_type([TokenType::CloseBracket])?;

            src.push(']');

            Ok(Some(Expression::ArrayIndex(Box::new(expr), index)))
        } else if ahead.typ == TokenType::Minus {
            // fixed negative index
            src.push_str(&tokenizer.read_token().value);
            src.push_str(&tokenizer.read_token().expect_type([TokenType::Int])?.value);
            index = -tokenizer.current().value.parse::<i32>()?;

            // read ]
            tokenizer
                .read_token()
                .expect_type([TokenType::CloseBracket])?;

            src.push(']');

            Ok(Some(Expression::ArrayIndex(Box::new(expr), index)))
        } else if ahead.typ == TokenType::Asterisk {
            // all items * (index = MaxValue)
            //method = _arrayFilterMethod;
            *is_scalar = false;
            //index = int.MaxValue;

            src.push_str(&tokenizer.read_token().value);

            // read ]
            tokenizer
                .read_token()
                .expect_type([TokenType::CloseBracket])?;

            src.push(']');

            Ok(Some(Expression::ArrayAsterisk(Box::new(expr))))
        } else {
            // inner expression
            inner = parse_full_expression(tokenizer, parameters, DocumentScope::Current)?;

            //if (inner == null) { throw LiteException.unexpected_token(tokenizer.current()); }

            // if array filter is not immutable, update ref (update only when false)
            if !inner.is_immutable {
                *is_immutable = false;
            }
            if inner.use_source {
                *use_source = true;
            }

            // if inner expression returns a single parameter, still Scalar
            // otherwise it's an operand filter expression (enumerable)
            if inner.r#type != BsonExpressionType::Parameter {
                //method = _arrayFilterMethod;
                *is_scalar = false;

                // add inner fields (can contains root call)
                fields.extend(inner.fields.iter().cloned());

                src.push_str(&inner.source);

                // read ]
                tokenizer
                    .read_token()
                    .expect_type([TokenType::CloseBracket])?;

                src.push(']');

                Ok(Some(Expression::ArrayIndexExpr(
                    Box::new(expr),
                    Box::new(inner),
                )))
            } else {
                // add inner fields (can contains root call)
                fields.extend(inner.fields.iter().cloned());

                src.push_str(&inner.source);

                // read ]
                tokenizer
                    .read_token()
                    .expect_type([TokenType::CloseBracket])?;

                src.push(']');

                Ok(Some(Expression::ArrayAsteriskFilter(
                    Box::new(expr),
                    Box::new(inner),
                )))
            }
        }
    } else {
        Ok(None)
    }
}

/// <summary>
/// Try parse FUNCTION methods: MAP, FILTER, SORT, ...
/// </summary>
fn try_parse_function(
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    if tokenizer.current().typ != TokenType::Word {
        return Ok(None);
    }
    if tokenizer.look_ahead().typ != TokenType::OpenParenthesis {
        return Ok(None);
    }

    let token = tokenizer.current().value.to_uppercase();

    match token.as_str() {
        "MAP" => parse_function("MAP", BsonExpressionType::Map, tokenizer, parameters, scope),
        "FILTER" => parse_function(
            "FILTER",
            BsonExpressionType::Filter,
            tokenizer,
            parameters,
            scope,
        ),
        "SORT" => parse_function(
            "SORT",
            BsonExpressionType::Sort,
            tokenizer,
            parameters,
            scope,
        ),
        _ => Ok(None),
    }
}

/// <summary>
/// Parse expression functions, like MAP, FILTER or SORT.
/// MAP(items[*] => @.Name)
/// </summary>
fn parse_function(
    function_name: &'static str,
    r#type: BsonExpressionType,
    tokenizer: &mut Tokenizer,
    parameters: &BsonDocument,
    scope: DocumentScope,
) -> Result<Option<BsonExpression>> {
    // check if next token are ( otherwise returns null (is not a function)
    if tokenizer.look_ahead().typ != TokenType::OpenParenthesis {
        return Ok(None);
    }

    // read (
    tokenizer
        .read_token()
        .expect_type([TokenType::OpenParenthesis])?;

    let mut left = parse_single_expression(tokenizer, parameters, scope)?;

    // if left is a scalar expression, convert into enumerable expression (avoid to use [*] all the time)
    if left.is_scalar {
        left = convert_to_enumerable(left);
    }

    let mut args = vec![];

    let mut src = format!("{}({}", function_name, left.source);
    let mut is_immutable = left.is_immutable;
    let mut use_source = left.use_source;
    let mut fields = HashSet::new();

    //args.push(left.expression);
    fields.extend(left.fields);

    let mut closure = None;

    // read =>
    if tokenizer.look_ahead().typ == TokenType::Equals {
        tokenizer.read_token().expect_type([TokenType::Equals])?;
        tokenizer.read_token().expect_type([TokenType::Greater])?;

        let right = parse_full_expression(
            tokenizer,
            parameters,
            if left.r#type == BsonExpressionType::Source {
                DocumentScope::Source
            } else {
                DocumentScope::Current
            },
        )?;

        src.push_str("=>");
        src.push_str(&right.source);
        fields.extend(right.fields.iter().cloned());
        closure = Some(Box::new(right));
    }

    if tokenizer.look_ahead().typ != TokenType::CloseParenthesis {
        tokenizer.read_token().expect_type([TokenType::Comma])?;

        src.push(',');

        // try more parameters ,
        while !tokenizer.check_eof()? {
            let parameter = parse_full_expression(tokenizer, parameters, scope)?;

            // update is_immutable only when came false
            if !parameter.is_immutable {
                is_immutable = false;
            }
            if parameter.use_source {
                use_source = true;
            }

            args.push(parameter.expression);
            src.push_str(&parameter.source);
            fields.extend(parameter.fields);

            if tokenizer.look_ahead().typ == TokenType::Comma {
                src.push_str(&tokenizer.read_token().value);
                continue;
            }
            break;
        }
    }

    // read )
    tokenizer
        .read_token()
        .expect_type([TokenType::CloseParenthesis])?;
    src.push(')');

    Ok(Some(BsonExpression {
        r#type,
        //parameters: parameters,
        is_immutable,
        use_source,
        is_scalar: false,
        fields,
        expression: Expression::Function(function_name, Box::new(left.expression), closure, args),
        source: src,
        left: None,
        right: None,
    }))
}

/// <summary>
/// Create an array expression with 2 values (used only in BETWEEN statement)
/// </summary>
fn new_array(item0: BsonExpression, item1: BsonExpression) -> Result<BsonExpression> {
    let values = vec![item0.expression, item1.expression];

    // both values must be scalar expressions
    if !item0.is_scalar {
        return Err(LiteException::expr_error(&format!(
            "Expression `{}` must be a scalar expression",
            item0.source
        )));
    }
    if !item1.is_scalar {
        return Err(LiteException::expr_error(&format!(
            "Expression `{}` must be a scalar expression",
            item0.source
        )));
    }

    Ok(BsonExpression {
        r#type: BsonExpressionType::Array,
        //parameters: item0.parameters, // should be == item1.parameters
        is_immutable: item0.is_immutable && item1.is_immutable,
        use_source: item0.use_source || item1.use_source,
        is_scalar: true,
        fields: item0.fields.into_iter().chain(item1.fields).collect(),
        expression: Expression::ArrayInit(values),
        source: format!("{} AND {}", item0.source, item1.source),
        left: None,
        right: None,
    })
}

/// <summary>
/// Get field from simple \w regex or ['comp-lex'] - also, add into source. Can read empty field (root)
/// </summary>
fn read_field(tokenizer: &mut Tokenizer, source: &mut String) -> Result<String> {
    let mut field = String::new();

    // if field are complex
    if tokenizer.current().typ == TokenType::OpenBracket {
        field = tokenizer
            .read_token()
            .expect_type([TokenType::String])?
            .value
            .to_string();
        tokenizer
            .read_token()
            .expect_type([TokenType::CloseBracket])?;
    } else if tokenizer.current().typ == TokenType::Word {
        field = tokenizer.take_current().value.to_string();
    }

    if !field.is_empty() {
        source.push('.');

        // add bracket in result only if is complex type
        if field.is_word() {
            source.push_str(&field);
        } else {
            source.push('[');
            append_quoted(&field, source);
            source.push(']');
        }
    }

    Ok(field)
}

/// <summary>
/// Read key in document definition with single word or "comp-lex"
/// </summary>
pub fn read_key(tokenizer: &mut Tokenizer, source: &mut String) -> Result<String> {
    let token = tokenizer.read_token();
    let key = if token.typ == TokenType::String {
        tokenizer.take_current().value.into_owned()
    } else {
        tokenizer
            .take_current()
            .expect_type([TokenType::Word, TokenType::Int])?
            .value
            .into_owned()
    };

    if key.is_word() {
        source.push_str(&key);
    } else {
        append_quoted(&key, source);
    }

    Ok(key)
}

/// <summary>
/// Read next token as Operant with ANY|ALL keyword before - returns null if next token are not an operant
/// </summary>
fn read_operant(tokenizer: &mut Tokenizer) -> Result<Option<String>> {
    let mut token = tokenizer.look_ahead();

    if token.is_operand() {
        let operant = token.value.to_uppercase();
        tokenizer.read_token(); // consume operant

        return Ok(Some(operant));
    }

    if token.is("ALL") || token.is("ANY") {
        let key = token.value.to_uppercase();

        tokenizer.read_token(); // consume operant

        token = tokenizer.read_token();

        if !token.is_operand() {
            return Err(LiteException::unexpected_token(
                "Expected valid operand",
                token,
            ));
        }

        if token.value.starts_with(|x: char| x.is_ascii_alphabetic()) {
            return Ok(Some(format!("{} {}", key, token.value.to_uppercase())));
        } else {
            return Ok(Some(format!("{}{}", key, token.value)));
        }
    }

    Ok(None)
}

/// <summary>
/// Convert scalar expression into enumerable expression using ITEMS(...) method
/// Append [*] to path or ITEMS(..) in all others
/// </summary>
fn convert_to_enumerable(expr: BsonExpression) -> BsonExpression {
    let src = if expr.r#type == BsonExpressionType::Path {
        format!("{}[*]", expr.source)
    } else {
        format!("ITEMS({})", expr.source)
    };

    let expr_type = if expr.r#type == BsonExpressionType::Path {
        BsonExpressionType::Path
    } else {
        BsonExpressionType::Call
    };

    BsonExpression {
        r#type: expr_type,
        //parameters: expr.parameters,
        is_immutable: expr.is_immutable,
        use_source: expr.use_source,
        is_scalar: false,
        fields: expr.fields,
        expression: Expression::Items(Box::new(expr.expression)),
        source: src,
        left: None,
        right: None,
    }
}

/// <summary>
/// Convert enumerable expression into array using ARRAY(...) method
/// </summary>
fn convert_to_array(expr: BsonExpression) -> BsonExpression {
    BsonExpression {
        r#type: BsonExpressionType::Call,
        //parameters: expr.parameters,
        is_immutable: expr.is_immutable,
        use_source: expr.use_source,
        is_scalar: true,
        fields: expr.fields,
        expression: Expression::Method("ARRAY", vec![expr.expression]),
        source: format!("ARRAY({})", expr.source),
        left: None,
        right: None,
    }
}

/// <summary>
/// Create new logic (AND/OR) expression based in 2 expressions
/// </summary>
pub(super) fn create_logic_expression(
    r#type: BsonExpressionType,
    left: BsonExpression,
    right: BsonExpression,
) -> BsonExpression {
    // convert BsonValue into Boolean
    let bool_left = left.expression.clone();
    let bool_right = right.expression.clone();

    let expr = if r#type == BsonExpressionType::And {
        Expression::AndAlso(Box::new(bool_left), Box::new(bool_right))
    } else {
        Expression::OrElse(Box::new(bool_left), Box::new(bool_right))
    };

    let operator = if r#type == BsonExpressionType::And {
        "AND"
    } else {
        "OR"
    };

    // and convert back Boolean to BsonValue
    //let mut ctor = typeof(BsonValue)
    //    .GetConstructors()
    //    .First(x => x.GetParameters().FirstOrDefault()?.ParameterType == typeof(bool));

    // create new binary expression based in 2 other expressions
    BsonExpression {
        r#type,
        //parameters: left.parameters, // should be == right.parameters
        is_immutable: left.is_immutable && right.is_immutable,
        use_source: left.use_source || right.use_source,
        is_scalar: left.is_scalar && right.is_scalar,
        fields: left
            .fields
            .iter()
            .cloned()
            .chain(right.fields.iter().cloned())
            .collect(),
        expression: expr,
        source: format!("{} {} {}", left.source, operator, right.source),
        left: inner(left),
        right: inner(right),
    }
}

/// <summary>
/// Create new conditional (IIF) expression. Execute expression only if True or False value
/// </summary>
pub(super) fn create_conditional_expression(
    test: BsonExpression,
    if_true: BsonExpression,
    if_false: BsonExpression,
) -> BsonExpression {
    // convert BsonValue into Boolean
    let expr = Expression::Condition(
        Box::new(test.expression),
        Box::new(if_true.expression),
        Box::new(if_false.expression),
    );

    // create new binary expression based in 2 other expressions
    BsonExpression {
        r#type: BsonExpressionType::Call, // there is not specific Conditional
        //parameters: test.parameters, // should be == if_true|if_false parameters
        is_immutable: test.is_immutable && if_true.is_immutable || if_false.is_immutable,
        use_source: test.use_source || if_true.use_source || if_false.use_source,
        is_scalar: test.is_scalar && if_true.is_scalar && if_false.is_scalar,
        fields: test
            .fields
            .into_iter()
            .chain(if_true.fields)
            .chain(if_false.fields)
            .collect(),
        expression: expr,
        source: format!(
            "IIF({},{},{})",
            test.source, if_true.source, if_false.source
        ),
        left: None,
        right: None,
    }
}
