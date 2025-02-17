use super::*;
use crate::bson::{DateTime, TotalOrd, Value};
use std::marker::PhantomData;
use std::str::FromStr;

pub(super) struct Methods;

// this is treated as function invocation macro in rustfmt so this will be formtted correctly
macro_rules! methods {
    (
        $name: ident,
        |$ctx: pat_param$(, $param_name: ident: $param_type: ident)*$(,)?| -> scalar $body: block
    ) => {
        methods!(@body_impl
            attr: #[allow(non_snake_case)],
            access: pub(in super::super),
            name: $name,
            ctx: &'ctx ctx,
            built_args: [],
            building_args: [$($param_name: $param_type,)*],
            body: {
                let $ctx = ctx;
                Ok($body)
            },
            return_type: [crate::Result<&'ctx Value>],
        );
    };

    (@body_impl
        attr: $(#[$attr: meta])*,
        access: $access: vis,
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        #[allow(unused_mut)]
        $(#[$attr])*
        $access fn $name<$ctx_lifetime>($ctx: &ExecutionContext<$ctx_lifetime>, $($built_args)*) -> $($return_type)* {
            $body
        }
    };
    (@body_impl
        attr: $(#[$attr: meta])*,
        access: $access: vis,
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [$arg_name: ident: sequence, $($building_args: tt)*] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        methods!(@body_impl
            attr: $(#[$attr])*,
            access: $access,
            name: $name,
            ctx: &$ctx_lifetime $ctx,
            built_args: [$($built_args)* mut $arg_name: ValueIterator<$ctx_lifetime, $ctx_lifetime>, ],
            building_args: [$($building_args)*],
            body: $body,
            return_type: [$($return_type)*],
        );
    };
    (@body_impl
        attr: $(#[$attr: meta])*,
        access: $access: vis,
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [$arg_name: ident: scalar, $($building_args: tt)*] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        methods!(@body_impl
            attr: $(#[$attr])*,
            access: $access,
            name: $name,
            ctx: &$ctx_lifetime $ctx,
            built_args: [$($built_args)* mut $arg_name: &$ctx_lifetime Value, ],
            building_args: [$($building_args)*],
            body: $body,
            return_type: [$($return_type)*],
        );
    };
}

struct FromBsonExpressionResult<Gen, T> {
    expression: Gen,
    source: String,
    _phantom: PhantomData<T>,
}

impl<Gen, T> FromBsonExpressionResult<Gen, T> {
    // helper for type inference in macro
    pub fn t(&self) -> T {
        unreachable!()
    }
}

trait FromBsonExpression: Sized {
    type Expression;
    fn from_bson_expr(_: BsonExpression) -> FromBsonExpressionResult<Self::Expression, Self>;
}

impl FromBsonExpression for &'_ Value {
    type Expression = ScalarExpr;
    fn from_bson_expr(expr: BsonExpression) -> FromBsonExpressionResult<Self::Expression, Self> {
        let expr = expr.into_scalar();
        FromBsonExpressionResult {
            expression: expr.expression,
            source: expr.source,
            _phantom: PhantomData,
        }
    }
}

impl FromBsonExpression for ValueIterator<'_, '_> {
    type Expression = SequenceExpr;
    fn from_bson_expr(expr: BsonExpression) -> FromBsonExpressionResult<Self::Expression, Self> {
        let expr = expr.into_sequence();
        FromBsonExpressionResult {
            expression: expr.expression,
            source: expr.source,
            _phantom: PhantomData,
        }
    }
}

macro_rules! method_info2 {
    ($name: ident ($($args: ident),*)) => {
        method_info2!(@gen
            name: $name,
            bsonName: $name,
            args: ($($args),*),
            volatile: false,
        )
    };
    ($name: ident as $bsonName: ident ($($args: ident),*)) => {
        method_info2!(@gen
            name: $name,
            bsonName: $bsonName,
            args: ($($args),*),
            volatile: false,
        )
    };

    (volatile $name: ident ($($args: ident),*)) => {
        method_info2!(@gen
            name: $name,
            bsonName: $name,
            args: ($($args),*),
            volatile: true,
        )
    };
    (volatile $name: ident as $bsonName: ident ($($args: ident),*)) => {
        method_info2!(@gen
            name: $name,
            bsonName: $bsonName,
            args: ($($args),*),
            volatile: true,
        )
    };

    (@gen
        name: $name: ident,
        bsonName: $bsonName:ident,
        args: ($($args: ident),*),
        volatile: $volatile: expr,
    ) => {
        {
            const NAME: &str = stringify!($bsonName);
            const ARG_COUNT: usize = method_info2!(@count $($args),*);

            pub(super) fn expr_impl(args: Vec<BsonExpression>) -> (Expression, String) {

                let args_array: [BsonExpression; ARG_COUNT] = args.try_into().unwrap();

                let [$($args),*] = args_array;

                $(let $args = FromBsonExpression::from_bson_expr($args);)*

                #[allow(unreachable_code)]
                if false {
                    // for type inference
                    $name(unreachable!()$(, $args.t())*).ok();
                }

                let expr = scalar_expr(move |ctx| $name(ctx$(, ($args.expression)(ctx)?)*));

                let source = method_info2!(@source NAME$(, $args.source)*);

                (expr.into(), source)
            }

            MethodInfo {
                name: NAME,
                arg_count: ARG_COUNT,
                volatile: $volatile,
                create_expression: expr_impl,
            }
        }
    };

    (@source $name: expr) => { format!("{}()", $name) };
    (@source $name: expr, $arg0: expr $(, $args: expr)*) => {
        format!(
            concat!("{}({}"$(, method_info2!(@dummy ",{}", $args))*, ")"),
            $name,
            $arg0,
            $($args, )*
        )
    };

    (@dummy $value: expr, $tt: expr) => { $value };

    (@count) => { 0 };
    (@count $arg0: ident) => {1};
    (@count $arg0: ident, $arg1: ident) => { 2 };
    (@count $arg0: ident, $arg1: ident, $arg2: ident) => { 3 };
    (@count $arg0: ident, $arg1: ident, $arg2: ident, $arg3: ident) => { 4 };
    (@count $arg0: ident, $arg1: ident, $arg2: ident, $arg3: ident, $arg4: ident) => { 5 };
}

macro_rules! overflow {
    ($expr: expr) => {
        crate::Error::expr_run_error(&concat!("overflow converting to ", $expr))
    };
}

//region aggregate
mod methods {
    use super::*;

    methods!(COUNT, |ctx, values: sequence| -> scalar {
        let mut count = 0;
        while let Some(_) = values.next().transpose()? {
            count += 1;
        }
        ctx.arena(Value::Int32(count))
    });

    methods!(MIN, |_, values: sequence| -> scalar {
        let mut min = &Value::MaxValue;

        while let Some(value) = values.next().transpose()? {
            if value.total_cmp(&min).is_lt() {
                min = value;
            }
        }

        if min == &Value::MaxValue {
            &Value::MinValue
        } else {
            min
        }
    });

    methods!(MAX, |_, values: sequence| -> scalar {
        let mut min = &Value::MinValue;

        while let Some(value) = values.next().transpose()? {
            if value.total_cmp(min).is_gt() {
                min = value;
            }
        }

        if min == &Value::MinValue {
            &Value::MaxValue
        } else {
            min
        }
    });

    methods!(FIRST, |_, values: sequence| -> scalar {
        let mut values = values;
        values.next().transpose()?.unwrap_or(&Value::Null)
    });

    methods!(LAST, |_, values: sequence| -> scalar {
        let mut last = &Value::Null;
        while let Some(value) = values.next().transpose()? {
            last = value;
        }
        last
    });

    methods!(AVG, |ctx, values: sequence| -> scalar {
        let mut sum = Value::Int32(0);
        let mut count = 0;

        while let Some(value) = values.next().transpose()? {
            if value.is_number() {
                sum = &sum + value;
                count += 1;
            }
        }

        if count == 0 {
            &Value::Int32(0)
        } else {
            ctx.arena(&sum / &Value::Int32(count))
        }
    });

    methods!(SUM, |ctx, values: sequence| -> scalar {
        let mut sum = Value::Int32(0);

        while let Some(value) = values.next().transpose()? {
            if value.is_number() {
                sum = &sum + value;
            }
        }

        ctx.arena(sum)
    });

    methods!(ANY, |ctx, values: sequence| -> scalar {
        ctx.bool(values.next().transpose()?.is_some())
    });

    //endregion

    //region data types

    //region new instance
    methods!(MINVALUE, |_| -> scalar { &Value::MinValue });
    methods!(OBJECTID_NEW, |ctx| -> scalar {
        ctx.arena(Value::ObjectId(bson::ObjectId::new()))
    });
    methods!(GUID_NEW, |ctx| -> scalar {
        ctx.arena(Value::Guid(bson::Guid::new()))
    });
    methods!(NOW, |ctx| -> scalar {
        ctx.arena(Value::DateTime(bson::DateTime::now()))
    });
    methods!(NOW_UTC, |ctx| -> scalar {
        ctx.arena(Value::DateTime(bson::DateTime::now()))
    });
    methods!(TODAY, |ctx| -> scalar {
        ctx.arena(Value::DateTime(bson::DateTime::today()))
    });
    methods!(MAXVALUE, |_| -> scalar { &Value::MaxValue });
    //endregion

    //region DATATYPE
    methods!(INT32, |ctx, value: scalar| -> scalar {
        match *value {
            Value::Int32(_) => value,
            ref v if v.is_number() => {
                ctx.arena(Value::Int32(v.to_i32().ok_or_else(|| overflow!("INT32"))?))
            }
            Value::String(ref str) => {
                if let Ok(v) = i32::from_str(str) {
                    ctx.arena(Value::Int32(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(INT64, |ctx, value: scalar| -> scalar {
        match *value {
            Value::Int64(_) => value,
            ref v if v.is_number() => {
                ctx.arena(Value::Int64(v.to_i64().ok_or_else(|| overflow!("INT64"))?))
            }
            Value::String(ref str) => {
                if let Ok(v) = i64::from_str(str) {
                    ctx.arena(Value::Int64(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(DOUBLE, |ctx, value: scalar| -> scalar {
        match *value {
            Value::Double(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Double(
                v.to_f64().ok_or_else(|| overflow!("Double"))?,
            )),
            Value::String(ref str) => {
                // TODO: culture
                if let Ok(v) = f64::from_str(str) {
                    ctx.arena(Value::Double(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(DOUBLE_CULTURE, |ctx,
                              value: scalar,
                              culture: scalar|
     -> scalar {
        // TODO: culture
        let _ = culture;
        match *value {
            Value::Double(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Double(
                v.to_f64().ok_or_else(|| overflow!("Double"))?,
            )),
            Value::Decimal(v) => ctx.arena(Value::Double(v.to_f64())),
            Value::String(ref str) => {
                if let Ok(v) = f64::from_str(str) {
                    ctx.arena(Value::Double(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(DECIMAL, |ctx, value: scalar| -> scalar {
        match *value {
            Value::Decimal(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Decimal(
                v.to_decimal().ok_or_else(|| overflow!("Decimal"))?,
            )),
            Value::String(ref str) => {
                // TODO: culture
                if let Some(v) = bson::Decimal128::parse(str) {
                    ctx.arena(Value::Decimal(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(DECIMAL_CULTURE, |ctx,
                               value: scalar,
                               culture: scalar|
     -> scalar {
        // TODO: culture
        let _ = culture;
        match *value {
            Value::Decimal(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Decimal(
                v.to_decimal().ok_or_else(|| overflow!("Decimal"))?,
            )),
            Value::String(ref str) => {
                if let Some(v) = bson::Decimal128::parse(str) {
                    ctx.arena(Value::Decimal(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(STRING, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(_) => value,
            _ => ctx.arena(Value::String(string_impl(value))),
        }
    });

    // ==> there is no convert to BsonDocument, must use { .. } syntax

    methods!(ARRAY, |ctx, values: sequence| -> scalar {
        ctx.arena(Value::Array(bson::Array::from(
            values
                .map_ok(|x| x.clone())
                .collect::<Result<Vec<_>, _>>()?,
        )))
    });

    methods!(BINARY, |_, value: scalar| -> scalar {
        match value {
            Value::Binary(_) => value,
            Value::String(_) => {
                // parse base64
                todo!()
            }
            _ => &Value::Null,
        }
    });

    methods!(OBJECTID, |_, value: scalar| -> scalar {
        match value {
            Value::ObjectId(_) => value,
            Value::String(_) => {
                // parse hex
                todo!()
            }
            _ => &Value::Null,
        }
    });

    methods!(GUID, |_, value: scalar| -> scalar {
        match value {
            Value::Guid(_) => value,
            Value::String(_) => {
                // parse hex
                todo!()
            }
            _ => &Value::Null,
        }
    });

    methods!(BOOLEAN, |_, value: scalar| -> scalar {
        match value {
            Value::Boolean(_) => value,
            Value::String(str) => {
                let str = str.trim().trim_matches(|c: char| c == '\0');
                if str.eq_ignore_ascii_case("true") {
                    &Value::Boolean(true)
                } else if str.eq_ignore_ascii_case("false") {
                    &Value::Boolean(false)
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(DATETIME, |ctx, value: scalar| -> scalar {
        match value {
            Value::DateTime(_) => value,
            Value::String(str) => {
                // TODO: culture and more format
                ctx.arena(Value::DateTime(
                    bson::DateTime::parse_rfc3339(str)
                        .ok_or_else(|| Error::expr_run_error("invalid date"))?,
                ))
            }
            _ => &Value::Null,
        }
    });

    methods!(DATETIME_CULTURE, |ctx,
                                value: scalar,
                                culture: scalar|
     -> scalar {
        match value {
            Value::DateTime(_) => value,
            Value::String(str) => {
                // TODO: culture and more format
                ctx.arena(Value::DateTime(
                    bson::DateTime::parse_rfc3339(str)
                        .ok_or_else(|| Error::expr_run_error("invalid date"))?,
                ))
            }
            _ => &Value::Null,
        }
    });

    methods!(DATETIME_YMD, |ctx,
                            year: scalar,
                            month: scalar,
                            day: scalar|
     -> scalar {
        if year.is_number() && month.is_number() && day.is_number() {
            let year = year
                .to_i32()
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| overflow!("Int32"))?;
            let month = month
                .to_i32()
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| overflow!("Int32"))?;
            let day = day
                .to_i32()
                .and_then(|x| x.try_into().ok())
                .ok_or_else(|| overflow!("Int32"))?;

            ctx.arena(Value::DateTime(
                DateTime::from_ymd(year, month, day).ok_or_else(|| overflow!("DateTime"))?,
            ))
        } else {
            &Value::Null
        }
    });

    //endregion

    //region IS_DATETYPE
    methods!(IS_MINVALUE, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::MinValue))
    });
    methods!(IS_NULL, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Null))
    });
    methods!(IS_INT32, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Int32(_)))
    });
    methods!(IS_INT64, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Int64(_)))
    });
    methods!(IS_DOUBLE, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Double(_)))
    });
    methods!(IS_DECIMAL, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Decimal(_)))
    });
    methods!(IS_NUMBER, |ctx, value: scalar| -> scalar {
        ctx.bool(value.is_number())
    });
    methods!(IS_STRING, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::String(_)))
    });
    methods!(IS_DOCUMENT, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Document(_)))
    });
    methods!(IS_ARRAY, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Array(_)))
    });
    methods!(IS_BINARY, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Binary(_)))
    });
    methods!(IS_OBJECTID, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::ObjectId(_)))
    });
    methods!(IS_GUID, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Guid(_)))
    });
    methods!(IS_BOOLEAN, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::Boolean(_)))
    });
    methods!(IS_DATETIME, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::DateTime(_)))
    });
    methods!(IS_MAXVALUE, |ctx, value: scalar| -> scalar {
        ctx.bool(matches!(value, Value::MaxValue))
    });
    //endregion

    //endregion
}

pub(super) use methods::*;

const METHODS: &[MethodInfo] = &[
    method_info2!(COUNT(values)),
    method_info2!(MIN(values)),
    method_info2!(MAX(values)),
    method_info2!(FIRST(values)),
    method_info2!(LAST(values)),
    method_info2!(AVG(values)),
    method_info2!(SUM(values)),
    method_info2!(ANY(values)),
    method_info2!(MINVALUE()),
    method_info2!(volatile OBJECTID_NEW as OBJECTID()),
    method_info2!(volatile GUID_NEW as GUID()),
    method_info2!(volatile NOW()),
    method_info2!(volatile NOW_UTC()),
    method_info2!(volatile TODAY()),
    method_info2!(MAXVALUE()),
    method_info2!(INT32(value)),
    method_info2!(INT64(value)),
    method_info2!(DOUBLE(value)),
    method_info2!(DOUBLE_CULTURE as DOUBLE(value, culture)),
    method_info2!(DECIMAL(value)),
    method_info2!(DECIMAL_CULTURE as DECIMAL(value, culture)),
    method_info2!(STRING(value)),
    method_info2!(ARRAY(value)),
    method_info2!(BINARY(value)),
    method_info2!(OBJECTID(value)),
    method_info2!(GUID(value)),
    method_info2!(BOOLEAN(value)),
    method_info2!(DATETIME as DATETIME(value)),
    method_info2!(DATETIME_CULTURE as DATETIME(value, culture)),
    method_info2!(DATETIME as DATETIME_UTC(value)),
    method_info2!(DATETIME_CULTURE as DATETIME_UTC(value, culture)),
    method_info2!(DATETIME_YMD as DATETIME(year, month, day)),
    method_info2!(DATETIME_YMD as DATETIME_UTC(year, month, day)),
    method_info2!(IS_MINVALUE(value)),
    method_info2!(IS_NULL(value)),
    method_info2!(IS_INT32(value)),
    method_info2!(IS_INT64(value)),
    method_info2!(IS_DOUBLE(value)),
    method_info2!(IS_DECIMAL(value)),
    method_info2!(IS_NUMBER(value)),
    method_info2!(IS_STRING(value)),
    method_info2!(IS_DOCUMENT(value)),
    method_info2!(IS_ARRAY(value)),
    method_info2!(IS_BINARY(value)),
    method_info2!(IS_OBJECTID(value)),
    method_info2!(IS_GUID(value)),
    method_info2!(IS_BOOLEAN(value)),
    method_info2!(IS_DATETIME(value)),
    method_info2!(IS_MAXVALUE(value)),
    method_info2!(INT32 as INT(value)),
    method_info2!(INT64 as LONG(value)),
    method_info2!(BOOLEAN as BOOL(value)),
    method_info2!(DATETIME as DATE(value)),
    method_info2!(DATETIME_CULTURE as DATE(value, culture)),
    method_info2!(DATETIME as DATE_UTC(value)),
    method_info2!(DATETIME_CULTURE as DATE_UTC(value, culture)),
    method_info2!(DATETIME_YMD as DATE(year, month, day)),
    method_info2!(DATETIME_YMD as DATE_UTC(year, month, day)),
    method_info2!(IS_INT32 as IS_INT(value)),
    method_info2!(IS_INT64 as IS_LONG(value)),
    method_info2!(IS_BOOLEAN as IS_BOOL(value)),
    method_info2!(IS_DATETIME as IS_DATE(value)),
];

struct MethodInfo {
    name: &'static str,
    arg_count: usize,
    volatile: bool,
    create_expression: fn(Vec<BsonExpression>) -> (Expression, String),
}

// TODO: implement other methods

pub(super) fn string_impl(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => bson::to_json(other),
    }
}
