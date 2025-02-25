#![allow(clippy::module_inception)]

use super::*;
use crate::bson::Value;
use std::marker::PhantomData;

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
    (
        $name: ident,
        |$ctx: pat_param$(, $param_name: ident: $param_type: ident)*$(,)?| -> sequence $body: block
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
                $body
            },
            return_type: [ValueIterator<'ctx, 'ctx>],
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
            _phantom: PhantomData,
        }
    }
}

struct CreateExpr<T> {
    _phantom: PhantomData<T>,
}

impl<T> CreateExpr<T> {
    #[inline]
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
    #[inline]
    fn infer(&self, _: T) {
        unreachable!()
    }
}

trait CreateExpression<T> {
    type Expression;
    fn create(self, _: T) -> Self::Expression;
}

impl<T: for<'ctx> Fn(&ExecutionContext<'ctx>) -> crate::Result<&'ctx bson::Value> + 'static>
    CreateExpression<T> for CreateExpr<crate::Result<&'_ bson::Value>>
{
    type Expression = ScalarExpr;

    #[inline]
    fn create(self, expr: T) -> Self::Expression {
        scalar_expr(expr)
    }
}

impl<T: for<'ctx> Fn(&ExecutionContext<'ctx>) -> crate::Result<ValueIterator<'ctx, 'ctx>> + 'static>
    CreateExpression<T> for CreateExpr<ValueIterator<'_, '_>>
{
    type Expression = SequenceExpr;

    #[inline]
    fn create(self, expr: T) -> Self::Expression {
        sequence_expr(expr)
    }
}

trait MakeResult {
    type Result;
    fn make(self) -> Self::Result;
}

impl MakeResult for crate::Result<&'_ bson::Value> {
    type Result = Self;
    #[inline]
    fn make(self) -> Self::Result {
        self
    }
}

impl<'ctx> MakeResult for ValueIterator<'ctx, 'ctx> {
    type Result = crate::Result<ValueIterator<'ctx, 'ctx>>;
    #[inline]
    fn make(self) -> Self::Result {
        Ok(self)
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

            pub(super) fn expr_impl(args: Vec<BsonExpression>) -> Expression {

                let args_array: [BsonExpression; ARG_COUNT] = args.try_into().unwrap();

                let create_expr = CreateExpr::new();
                let [$($args),*] = args_array;

                $(let $args = FromBsonExpression::from_bson_expr($args);)*

                #[allow(unreachable_code)]
                #[allow(clippy::diverging_sub_expression)]
                if false {
                    // expression for type inference
                    create_expr.infer($name(unreachable!()$(, $args.t())*));
                }

                let expr = create_expr.create(move |ctx| MakeResult::make($name(ctx$(, ($args.expression)(ctx)?)*)));

                expr.into()
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

#[cfg(feature = "expression-methods")]
mod expression_methods {
    use super::*;
    use crate::bson::{DateTime, TotalOrd};
    use crate::utils::{CSharpStringUtils, OrdBsonValue};
    use base64::prelude::*;
    use std::collections::BTreeSet;
    use std::str::FromStr;

    macro_rules! overflow {
        ($expr: expr) => {
            crate::Error::expr_run_error(&concat!("overflow converting to ", $expr))
        };
    }

    //region aggregate

    methods!(COUNT, |ctx, values: sequence| -> scalar {
        let mut count = 0;
        while values.next().transpose()?.is_some() {
            count += 1;
        }
        ctx.arena(Value::Int32(count))
    });

    methods!(MIN, |_, values: sequence| -> scalar {
        let mut min = &Value::MaxValue;

        while let Some(value) = values.next().transpose()? {
            if value.total_cmp(min).is_lt() {
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

    // ARRAY is in basic methods

    methods!(BINARY, |ctx, value: scalar| -> scalar {
        match value {
            Value::Binary(_) => value,
            Value::String(base64) => {
                // parse base64
                let bytes = BASE64_STANDARD
                    .decode(base64)
                    .map_err(|_| Error::expr_run_error("bad base64"))?;
                ctx.arena(bson::Binary::new(bytes).into())
            }
            _ => &Value::Null,
        }
    });

    methods!(OBJECTID, |ctx, value: scalar| -> scalar {
        match value {
            Value::ObjectId(_) => value,
            Value::String(hex) => {
                // parse hex
                let mut bytes = [0u8; 12];
                hex::decode_to_slice(hex, &mut bytes)
                    .map_err(|_| Error::expr_run_error("bad object id"))?;
                ctx.arena(bson::ObjectId::from_bytes(bytes).into())
            }
            _ => &Value::Null,
        }
    });

    methods!(GUID, |ctx, value: scalar| -> scalar {
        match value {
            Value::Guid(_) => value,
            Value::String(hex) => {
                // parse hex
                let mut bytes = [0u8; 16];
                hex::decode_to_slice(hex, &mut bytes)
                    .map_err(|_| Error::expr_run_error("bad object id"))?;
                ctx.arena(bson::Guid::from_bytes(bytes).into())
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
                let _ = culture;
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

    //region Date

    //region Year/Month/Day/Hour/Minute/Second

    macro_rules! date {
        ($name: ident, $f: ident) => {
            methods!($name, |ctx, value: scalar| -> scalar {
                if let Value::DateTime(dt) = value {
                    ctx.arena((dt.$f() as i32).into())
                } else {
                    &Value::Null
                }
            });
        };
    }

    date!(YEAR, year);
    date!(MONTH, month);
    date!(DAY, day);
    date!(HOUR, hour);
    date!(MINUTE, minute);
    date!(SECOND, second);

    //endregion

    //region Date Functions
    enum Part {
        Year,
        Month,
        Day,
        Hour,
        Minute,
        Second,
    }

    impl Part {
        fn from_str(s: &str) -> Option<Part> {
            use Part::*;
            match s {
                "y" | "Y" => Some(Year),
                "M" => Some(Month),
                "d" | "D" => Some(Month),
                "h" | "H" => Some(Hour),
                "m" => Some(Minute),
                "s" | "S" => Some(Second),
                _ if s.eq_ignore_ascii_case("year") => Some(Year),
                _ if s.eq_ignore_ascii_case("month") => Some(Month),
                _ if s.eq_ignore_ascii_case("day") => Some(Day),
                _ if s.eq_ignore_ascii_case("hour") => Some(Hour),
                _ if s.eq_ignore_ascii_case("minute") => Some(Minute),
                _ if s.eq_ignore_ascii_case("second") => Some(Second),
                _ => None,
            }
        }
    }

    methods!(DATEADD, |ctx,
                       interval: scalar,
                       number: scalar,
                       value: scalar|
     -> scalar {
        let Value::String(date_part) = interval else {
            return Ok(&Value::Null);
        };
        if !number.is_number() {
            return Ok(&Value::Null);
        };
        let Value::DateTime(date) = value else {
            return Ok(&Value::Null);
        };

        let numb = number.to_i32().ok_or_else(|| overflow!("INT32"))?;

        let value = match Part::from_str(date_part) {
            Some(Part::Year) => date.add_years(numb).ok_or_else(|| overflow!("DateTime"))?,
            Some(Part::Month) => date.add_months(numb).ok_or_else(|| overflow!("DateTime"))?,
            Some(Part::Day) => date.add_days(numb).ok_or_else(|| overflow!("DateTime"))?,
            Some(Part::Hour) => date.add_hours(numb).ok_or_else(|| overflow!("DateTime"))?,
            Some(Part::Minute) => date
                .add_minutes(numb)
                .ok_or_else(|| overflow!("DateTime"))?,
            Some(Part::Second) => date
                .add_seconds(numb)
                .ok_or_else(|| overflow!("DateTime"))?,
            _ => return Ok(&Value::Null),
        };

        ctx.arena(Value::DateTime(value))
    });

    macro_rules! date_diff {
        ($start: expr, $end: expr, $step: expr) => {{
            let diff = $end.ticks() as i64 - $start.ticks() as i64;
            (diff / $step) as i32
        }};
    }

    methods!(DATEDIFF, |ctx,
                        interval: scalar,
                        starts: scalar,
                        ends: scalar|
     -> scalar {
        let Value::String(date_part) = interval else {
            return Ok(&Value::Null);
        };
        let Value::DateTime(starts) = starts else {
            return Ok(&Value::Null);
        };
        let Value::DateTime(ends) = ends else {
            return Ok(&Value::Null);
        };

        const TICKS_PER_SECOND: i64 = 10_000_000;
        const TICKS_PER_MINUTE: i64 = TICKS_PER_SECOND * 60;
        const TICKS_PER_HOUR: i64 = TICKS_PER_MINUTE * 60;
        const TICKS_PER_DAY: i64 = TICKS_PER_MINUTE * 24;

        let value = match Part::from_str(date_part) {
            Some(Part::Year) => {
                // https://stackoverflow.com/a/28444291/3286260
                let mut years = ends.year() as i32 - starts.year() as i32;

                // if the start month and the end month are the same
                // BUT the end day is less than the start day
                if (starts.month() == ends.month() && ends.day() < starts.day())
                    || ends.month() < starts.month()
                // if the end month is less than the start month
                {
                    years -= 1;
                }
                years
            }
            Some(Part::Month) => {
                // https://stackoverflow.com/a/1526116/3286260
                (ends.month() + ends.year() * 12) as i32
                    - (starts.month() + starts.year() * 12) as i32
            }
            Some(Part::Day) => date_diff!(starts, ends, TICKS_PER_DAY),
            Some(Part::Hour) => date_diff!(starts, ends, TICKS_PER_HOUR),
            Some(Part::Minute) => date_diff!(starts, ends, TICKS_PER_MINUTE),
            Some(Part::Second) => date_diff!(starts, ends, TICKS_PER_SECOND),
            _ => return Ok(&Value::Null),
        };

        ctx.arena(Value::Int32(value))
    });
    //endregion

    //endregion

    //region Math

    methods!(ABS, |ctx, value: scalar| -> scalar {
        match value {
            Value::Int32(v) => ctx.arena(v.abs().into()),
            Value::Int64(v) => ctx.arena(v.abs().into()),
            Value::Double(v) => ctx.arena(v.abs().into()),
            Value::Decimal(v) => ctx.arena(v.abs().into()),
            _ => &Value::Null,
        }
    });

    methods!(ROUND, |ctx, value: scalar, digits: scalar| -> scalar {
        if digits.is_number() {
            let digits = digits.to_i32().ok_or_else(|| overflow!("INT32"))?;
            match value {
                Value::Int32(_) => value,
                Value::Int64(_) => value,
                Value::Double(v) => {
                    if !(0..=15).contains(&digits) {
                        return Err(crate::Error::expr_run_error("overflow round digits"));
                    }
                    let pow = 10f64.powi(digits);
                    ctx.arena(((v * pow).round() / pow).into())
                }
                Value::Decimal(v) => {
                    if !(0..=28).contains(&digits) {
                        return Err(crate::Error::expr_run_error("overflow round digits"));
                    }
                    ctx.arena(v.round_digits(digits as u32).into())
                }
                _ => &Value::Null,
            }
        } else {
            &Value::Null
        }
    });

    methods!(POW, |ctx, x: scalar, y: scalar| -> scalar {
        if x.is_number() && y.is_number() {
            ctx.arena(x.to_f64().unwrap().powf(y.to_f64().unwrap()).into())
        } else {
            &Value::Null
        }
    });

    //endregion

    //region Misc

    // JSON is unsupported

    methods!(EXTEND, |ctx, source: scalar, extend: scalar| -> scalar {
        match (source, extend) {
            (Value::Document(source), Value::Document(extend)) => {
                let mut new = bson::Document::new();
                for (k, v) in source.iter() {
                    new.insert(k.into(), v.clone());
                }
                for (k, v) in extend.iter() {
                    new.insert(k.into(), v.clone());
                }
                ctx.arena(new.into())
            }
            _ => &Value::Null,
        }
    });

    // ITEMS is in basic methods

    methods!(CONCAT, |_, first: sequence, second: sequence| -> sequence {
        Box::new(first.chain(second))
    });

    methods!(KEYS, |ctx, document: scalar| -> sequence {
        match document {
            Value::Document(v) => {
                let ctx = ctx.clone();
                Box::new(
                    v.iter()
                        .map(move |(k, _)| Ok(ctx.arena(k.to_string().into()))),
                )
            }
            _ => Box::new(std::iter::empty()),
        }
    });

    methods!(VALUES, |_, document: scalar| -> sequence {
        match document {
            Value::Document(v) => Box::new(v.iter().map(|(_, v)| Ok(v))),
            _ => Box::new(std::iter::empty()),
        }
    });

    methods!(OID_CREATIONTIME, |ctx, object_id: scalar| -> scalar {
        match object_id {
            Value::ObjectId(v) => ctx.arena(
                DateTime::from_unix_milliseconds(v.unix_timestamp() as i64 * 1000)
                    .unwrap()
                    .into(),
            ),
            _ => &Value::Null,
        }
    });

    methods!(COALESCE, |_, left: scalar, right: scalar| -> scalar {
        if matches!(left, Value::Null) {
            right
        } else {
            left
        }
    });

    methods!(LENGTH, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(str) => ctx.arena(Value::Int32(str.chars().count() as i32)),
            Value::Binary(bin) => ctx.arena(Value::Int32(bin.bytes().len() as i32)),
            Value::Array(array) => ctx.arena(Value::Int32(array.len() as i32)),
            Value::Document(doc) => ctx.arena(Value::Int32(doc.len() as i32)),
            Value::Null => &Value::Int32(0),
            _ => &Value::Null,
        }
    });

    methods!(TOP, |_, values: sequence, num: scalar| -> sequence {
        match *num {
            Value::Int32(n) if n > 0 => Box::new(values.take(n as usize)),
            Value::Int64(n) if n > 0 => Box::new(values.take(n as usize)),
            _ => Box::new(std::iter::empty()),
        }
    });

    methods!(UNION, |_, left: sequence, right: sequence| -> sequence {
        let mut set = BTreeSet::new();
        Box::new(
            left.chain(right)
                .filter_ok(move |&x| set.insert(OrdBsonValue(x))),
        )
    });

    methods!(EXCEPT, |_, left: sequence, right: sequence| -> sequence {
        Box::new(
            std::iter::once_with(move || {
                let mut set = right
                    .clone()
                    .map_ok(OrdBsonValue)
                    .collect::<Result<BTreeSet<_>, _>>()?;
                Ok(left
                    .clone()
                    .filter_ok(move |&x| set.insert(OrdBsonValue(x))))
            })
            .flatten_ok()
            .map(|x| x.and_then(|y| y)),
        )
    });

    methods!(DISTINCT, |_, values: sequence| -> sequence {
        let mut set = BTreeSet::new();
        Box::new(values.filter_ok(move |&x| set.insert(OrdBsonValue(x))))
    });

    methods!(RANDOM, |ctx| -> scalar {
        ctx.arena((rand::random::<i32>() & 0x7FFFFFFF).into())
    });

    methods!(RANDOM_RANGE, |ctx, min: scalar, max: scalar| -> scalar {
        match (min, max) {
            (&Value::Int32(min), &Value::Int32(max)) => {
                ctx.arena(rand::random_range(min..=max).into())
            }
            _ => &Value::Null,
        }
    });

    //endregion

    //region String

    methods!(LOWER, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(str) => ctx.arena(str.to_lower_invariant().into()),
            _ => &Value::Null,
        }
    });

    methods!(UPPER, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(str) => ctx.arena(str.to_upper_invariant().into()),
            _ => &Value::Null,
        }
    });

    methods!(LTRIM, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(str) => ctx.arena(str.trim_start().into()),
            _ => &Value::Null,
        }
    });

    methods!(RTRIM, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(str) => ctx.arena(str.trim_end().into()),
            _ => &Value::Null,
        }
    });

    methods!(TRIM, |ctx, value: scalar| -> scalar {
        match value {
            Value::String(str) => ctx.arena(str.trim().into()),
            _ => &Value::Null,
        }
    });

    methods!(INDEXOF, |ctx, value: scalar, search: scalar| -> scalar {
        match (value, search) {
            (Value::String(str), Value::String(search)) => {
                ctx.arena(str.find(search).map(|x| x as i32).unwrap_or(-1).into())
            }
            _ => &Value::Null,
        }
    });

    methods!(INDEXOF_START, |ctx,
                             value: scalar,
                             search: scalar,
                             start_idx: scalar|
     -> scalar {
        match (value, search, start_idx) {
            (Value::String(str), Value::String(search), start_idx) if start_idx.is_number() => {
                let start_idx = start_idx.to_i32().ok_or_else(|| overflow!("INT32"))?;
                if start_idx < 0 || start_idx as usize > str.len() {
                    return Err(Error::expr_run_error("indexof start index out of range"));
                }
                let start = start_idx as usize;
                ctx.arena(
                    str[start..]
                        .find(search)
                        .map(|x| x as i32 + start_idx)
                        .unwrap_or(-1)
                        .into(),
                )
            }
            _ => &Value::Null,
        }
    });

    methods!(SUBSTRING, |ctx,
                         value: scalar,
                         start_idx: scalar|
     -> scalar {
        match (value, start_idx) {
            (Value::String(str), start_idx) if start_idx.is_number() => {
                let start_idx = start_idx.to_i32().ok_or_else(|| overflow!("INT32"))?;
                if start_idx < 0 || start_idx as usize > str.len() {
                    return Err(Error::expr_run_error("substring start index out of range"));
                }
                let start = start_idx as usize;
                ctx.arena(str[start..].to_string().into())
            }
            _ => &Value::Null,
        }
    });

    methods!(SUBSTRING_RANGE, |ctx,
                               value: scalar,
                               start_idx: scalar,
                               length: scalar|
     -> scalar {
        match (value, start_idx, length) {
            (Value::String(str), start_idx, length)
                if start_idx.is_number() & length.is_number() =>
            {
                let start_idx = start_idx.to_i32().ok_or_else(|| overflow!("INT32"))?;
                if start_idx < 0 || start_idx as usize > str.len() {
                    return Err(Error::expr_run_error("substring start index out of range"));
                }
                let start_idx = start_idx as usize;

                let length = length.to_i32().ok_or_else(|| overflow!("INT32"))?;
                if length < 0 || start_idx + length as usize > str.len() {
                    return Err(Error::expr_run_error("substring start index out of range"));
                }
                let length = length as usize;
                ctx.arena(str[start_idx..][..length].to_string().into())
            }
            _ => &Value::Null,
        }
    });

    methods!(REPLACE, |ctx,
                       value: scalar,
                       search: scalar,
                       replace: scalar|
     -> scalar {
        match (value, search, replace) {
            (Value::String(str), Value::String(search), Value::String(replace)) => {
                ctx.arena(str.replace(search, replace).into())
            }
            _ => &Value::Null,
        }
    });

    methods!(LPAD, |ctx,
                    value: scalar,
                    width: scalar,
                    padding_char: scalar|
     -> scalar {
        match (value, width, padding_char) {
            (Value::String(str), width, Value::String(padding_char)) if width.is_number() => {
                let Some(padding_char) = padding_char.chars().next() else {
                    return Err(Error::expr_run_error("padding char is empty"));
                };

                let width = width.to_i32().ok_or_else(|| overflow!("INT32"))?;
                if width < 0 {
                    return Err(Error::expr_run_error("LPAD size is negative"));
                }
                let width = width as usize;

                let len = str.chars().count();
                if len <= width {
                    value
                } else {
                    let chars = std::iter::repeat(padding_char).take(width - len);
                    ctx.arena(chars.chain(str.chars()).collect::<String>().into())
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(RPAD, |ctx,
                    value: scalar,
                    width: scalar,
                    padding_char: scalar|
     -> scalar {
        match (value, width, padding_char) {
            (Value::String(str), width, Value::String(padding_char)) if width.is_number() => {
                let Some(padding_char) = padding_char.chars().next() else {
                    return Err(Error::expr_run_error("padding char is empty"));
                };

                let width = width.to_i32().ok_or_else(|| overflow!("INT32"))?;
                if width < 0 {
                    return Err(Error::expr_run_error("LPAD size is negative"));
                }
                let width = width as usize;

                let len = str.chars().count();
                if len <= width {
                    value
                } else {
                    let chars = std::iter::repeat(padding_char).take(width - len);
                    ctx.arena(str.chars().chain(chars).collect::<String>().into())
                }
            }
            _ => &Value::Null,
        }
    });

    methods!(SPLIT, |ctx, value: scalar, separator: scalar| -> sequence {
        match (value, separator) {
            (Value::String(str), Value::String(separator)) => {
                let ctx = ctx.clone();
                Box::new(
                    str.split(separator)
                        .map(move |x| Ok(ctx.arena(x.to_string().into()))),
                )
            }
            _ => Box::new(std::iter::empty()),
        }
    });

    // No Regex version support
    // FORMAT support

    methods!(JOIN, |ctx, values: sequence| -> scalar {
        ctx.arena(
            values
                .map_ok(string_impl)
                .collect::<Result<String, _>>()?
                .into(),
        )
    });

    methods!(JOIN_SEPARATOR, |ctx,
                              values: sequence,
                              separator: scalar|
     -> scalar {
        match separator {
            Value::String(separator) => ctx.arena(
                values
                    .map_ok(string_impl)
                    .reduce(|l, r| {
                        let mut l = l?;
                        let r = r?;
                        l.push_str(separator);
                        l.push_str(&r);
                        Ok(l)
                    })
                    .unwrap_or(Ok(String::new()))?
                    .into(),
            ),
            _ => &Value::Null,
        }
    });

    // IS_MATCH: no regex
    // MATCH: no regex

    //endregion

    pub(super) const METHODS: &[MethodInfo] = &[
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
        method_info2!(YEAR(value)),
        method_info2!(MONTH(value)),
        method_info2!(DAY(value)),
        method_info2!(HOUR(value)),
        method_info2!(MINUTE(value)),
        method_info2!(SECOND(value)),
        method_info2!(DATEADD(interval, time, val)),
        method_info2!(DATEDIFF(interval, starts, ends)),
        method_info2!(ABS(value)),
        method_info2!(ROUND(value, digits)),
        method_info2!(POW(left, right)),
        method_info2!(EXTEND(source, extend)),
        method_info2!(CONCAT(first, second)),
        method_info2!(KEYS(document)),
        method_info2!(VALUES(document)),
        method_info2!(OID_CREATIONTIME(object_id)),
        method_info2!(COALESCE(left, right)),
        method_info2!(LENGTH(value)),
        method_info2!(TOP(values, num)),
        method_info2!(UNION(left, right)),
        method_info2!(EXCEPT(left, right)),
        method_info2!(DISTINCT(values)),
        method_info2!(volatile RANDOM()),
        method_info2!(volatile RANDOM_RANGE as RANDOM(min, max)),
        method_info2!(LOWER(value)),
        method_info2!(UPPER(value)),
        method_info2!(LTRIM(value)),
        method_info2!(RTRIM(value)),
        method_info2!(TRIM(value)),
        method_info2!(INDEXOF(value, search)),
        method_info2!(INDEXOF_START as INDEXOF(value, search, start)),
        method_info2!(SUBSTRING(value, start)),
        method_info2!(SUBSTRING_RANGE as SUBSTRING(value, start, idx)),
        method_info2!(REPLACE(value, search, replace)),
        method_info2!(LPAD(value, width, char)),
        method_info2!(RPAD(value, width, char)),
        method_info2!(SPLIT(value, separator)),
        method_info2!(JOIN(value)),
        method_info2!(JOIN_SEPARATOR(value, separator)),
    ];
}

pub(super) use basic_methods::*;
mod basic_methods {
    use super::*;

    methods!(ITEMS, |ctx, array: scalar| -> sequence {
        match array {
            Value::Array(v) => Box::new(v.as_slice().iter().map(Ok)),
            Value::Binary(v) => {
                let ctx = ctx.clone();
                Box::new(
                    v.bytes()
                        .iter()
                        .map(move |&x| Ok(ctx.arena((x as i32).into()))),
                )
            }
            _ => Box::new(std::iter::once(array).map(Ok)),
        }
    });

    methods!(ARRAY, |ctx, values: sequence| -> scalar {
        ctx.arena(Value::Array(bson::Array::from(
            values
                .map_ok(|x| x.clone())
                .collect::<Result<Vec<_>, _>>()?,
        )))
    });

    pub(super) const METHODS: &[MethodInfo] =
        &[method_info2!(ITEMS(array)), method_info2!(ARRAY(value))];
}

pub(super) const METHODS: &[&[MethodInfo]] = &[
    #[cfg(feature = "expression-methods")]
    expression_methods::METHODS,
    basic_methods::METHODS,
];

pub(super) struct MethodInfo {
    pub name: &'static str,
    pub arg_count: usize,
    pub volatile: bool,
    pub create_expression: fn(Vec<BsonExpression>) -> Expression,
}

pub(super) fn string_impl(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => bson::to_json(other),
    }
}
