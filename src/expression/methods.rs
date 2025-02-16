use std::str::FromStr;
use super::*;
use crate::bson::{DateTime, TotalOrd, Value};

pub(super) struct Methods;

macro_rules! method_info {
    (
        $([$($attr: ident)+])*
        fn $name: ident($ctx: pat$(, $param_name: ident: $param_type: ident)*$(,)?) -> scalar {
            $($body:tt)*
        }
    ) => {{
        const NAME: &str = stringify!($name);
        const ARG_COUNT: usize = method_info!(@count $($param_name)*);

        method_info!(@body_impl
            attr: ,
            access: ,
            name: body_impl,
            ctx: &'ctx ctx,
            built_args: [],
            building_args: [$($param_name: $param_type,)*],
            body: {
                let $ctx = ctx;
                Ok({$($body)*})
            },
            return_type: [crate::Result<&'ctx Value>],
        );

        method_info!(@method_define
            attr: [$([$($attr)+])*],
            name: $name,
            ctx: &'ctx ctx,
            built_args: [],
            building_args: [$($param_name: $param_type,)*],
            body: {
                body_impl(ctx$(, $param_name)*)
            },
            return_type: [crate::Result<&'ctx Value>],
        );

        pub(super) fn expr_impl(args: Vec<BsonExpression>) -> (Expression, String) {

            let args_array: [BsonExpression; ARG_COUNT] = args.try_into().unwrap();

            let [$($param_name),*] = args_array;

            $( let $param_name = method_info!(@into_type $param_name, $param_type); )*

            let expr = {
                $( let $param_name = $param_name.expression; )*
                scalar_expr(move |ctx| body_impl(ctx$(, $param_name(ctx)? )*))
            };
            let source = method_info!(@format
                args_pattern: "",
                args: [NAME],
                building: [$($param_name.source,)*],
                delim: "",
            );

            (expr.into(), source)
        }

        MethodInfo {
            name: NAME,
            arg_count: ARG_COUNT,
            volatile: method_info!(@volatile [$([$($attr)+])*]),
            create_expression: expr_impl,
        }
    }};

    (@count) => {
        0
    };
    (@count $param_name0: ident $($param_name: ident)*) => {
        1 + method_info!(@count $($param_name)*)
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
        method_info!(@body_impl
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
        method_info!(@body_impl
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

    (@into_type $expr: expr, sequence) => {
        $expr.into_sequence()
    };
    (@into_type $expr: expr, scalar) => {
        $expr.into_scalar()
    };

    (@format
        args_pattern: $args_pattern: expr,
        args: [$($args: tt)*],
        building: [],
        delim: $delim: expr,
    ) => {
        format!(concat!("{}(", $args_pattern, ")"), $($args)*)
    };
    (@format
        args_pattern: $args_pattern: expr,
        args: [$($args: tt)*],
        building: [$current: expr, $($building: tt)*],
        delim: $delim: expr,
    ) => {
        method_info!(@format
            args_pattern: concat!($args_pattern, $delim, "{}"),
            args: [$($args)*, $current],
            building: [$($building)*],
            delim: $delim,
        )
    };

    (@volatile ) => { false };
    (@volatile [volatile] $($other: tt)*) => { true };
    (@volatile $prefix: tt $($other: tt)*) => { method_info!(@volatile $($other)*) };

    (@method_define 
        attr: [],
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [$($building_args: tt)*] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        #[allow(non_local_definitions)]
        impl Methods {
            method_info!(@body_impl
                attr: #[allow(non_snake_case)],
                access: pub,
                name: $name,
                ctx: &$ctx_lifetime $ctx,
                built_args: [$($built_args)*],
                building_args: [$($building_args)*],
                body: $body,
                return_type: [$($return_type)*],
            );
        }
    };
    (@method_define 
        attr: [[method $new_name: ident]],
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [$($building_args: tt)*] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        #[allow(non_local_definitions)]
        impl Methods {
            method_info!(@body_impl
                attr: #[allow(non_snake_case)],
                access: pub,
                name: $new_name,
                ctx: &$ctx_lifetime $ctx,
                built_args: [$($built_args)*],
                building_args: [$($building_args)*],
                body: $body,
                return_type: [$($return_type)*],
            );
        }
    };
    (@method_define 
        attr: [[no_method] $($other: tt)*],
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [$($building_args: tt)*] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        // no method
    };
    (@method_define 
        attr: [$prefix: tt $($other: tt)*],
        name: $name: ident,
        ctx: &$ctx_lifetime: lifetime $ctx: ident,
        built_args: [$($built_args: tt)*],
        building_args: [$($building_args: tt)*] ,
        body: $body: expr,
        return_type: [$($return_type: tt)*],
    ) => {
        method_info!(@method_define
            attr: [$($other)*],
            name: $name,
            ctx: &$ctx_lifetime $ctx,
            built_args: [$($built_args)*],
            building_args: [$($building_args)*],
            body: $body,
            return_type: [$($return_type)*],
        );
    };
}

macro_rules! overflow {
    ($expr: expr) => {
        crate::Error::expr_run_error(&concat!("overflow converting to ", $expr))
    };
}

const METHODS: &[MethodInfo] = &[
    //region aggregate
    method_info!(fn COUNT(ctx, values: sequence) -> scalar {
        let mut count = 0;
        while let Some(_) = values.next().transpose()? {
            count += 1;
        }
        ctx.arena(Value::Int32(count))
    }),
    method_info!(fn MIN(_, values: sequence) -> scalar {
        let mut min = &Value::MaxValue;

        while let Some(value) = values.next().transpose()? {
            if value.total_cmp(&min).is_lt()
            {
                min = value;
            }
        }

        if min == &Value::MaxValue {
            &Value::MinValue
        } else {
            min
        }
    }),
    method_info!(fn MAX(_, values: sequence) -> scalar {
        let mut min = &Value::MinValue;

        while let Some(value) = values.next().transpose()? {
            if value.total_cmp(min).is_gt()
            {
                min = value;
            }
        }

        if min == &Value::MinValue {
            &Value::MaxValue
        } else {
            min
        }
    }),
    method_info!(fn FIRST(_, values: sequence) -> scalar {
        let mut values = values;
        values.next().transpose()?.unwrap_or(&Value::Null)
    }),
    method_info!(fn LAST(_, values: sequence) -> scalar {
        let mut last = &Value::Null;
        while let Some(value) = values.next().transpose()? {
            last = value;
        }
        last
    }),
    method_info!(fn AVG(ctx, values: sequence) -> scalar {
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
    }),
    method_info!(fn SUM(ctx, values: sequence) -> scalar {
        let mut sum = Value::Int32(0);

        while let Some(value) = values.next().transpose()? {
            if value.is_number() {
                sum = &sum + value;
            }
        }

        ctx.arena(sum)
    }),
    method_info!(fn ANY(ctx, values: sequence) -> scalar {
        ctx.bool(values.next().transpose()?.is_some())
    }),
    //endregion
    //region data types

    //region new instance
    method_info!(fn MINVALUE(_) -> scalar { &Value::MinValue }),
    method_info!([no_method] [volatile] fn OBJECTID(ctx) -> scalar { ctx.arena(Value::ObjectId(bson::ObjectId::new())) }),
    method_info!([no_method] [volatile] fn GUID(ctx) -> scalar { ctx.arena(Value::Guid(bson::Guid::new())) }),
    method_info!([no_method] [volatile] fn NOW(ctx) -> scalar { ctx.arena(Value::DateTime(bson::DateTime::now())) }),
    method_info!([no_method] [volatile] fn NOW_UTC(ctx) -> scalar { ctx.arena(Value::DateTime(bson::DateTime::now())) }), // rust always have UTC
    method_info!([no_method] [volatile] fn TODAY(ctx) -> scalar { ctx.arena(Value::DateTime(bson::DateTime::today())) }),
    method_info!(fn MAXVALUE(_) -> scalar { &Value::MaxValue }),
    //endregion

    //region DATATYPE

    method_info!(fn INT32(ctx, value: scalar) -> scalar {
        match *value {
            Value::Int32(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Int32(v.to_i32().ok_or_else(||overflow!("INT32"))?)),
            Value::String(ref str) => {
                if let Ok(v) = i32::from_str(str) {
                    ctx.arena(Value::Int32(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    }),

    method_info!(fn INT64(ctx, value: scalar) -> scalar {
        match *value {
            Value::Int64(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Int64(v.to_i64().ok_or_else(||overflow!("INT64"))?)),
            Value::String(ref str) => {
                if let Ok(v) = i64::from_str(str) {
                    ctx.arena(Value::Int64(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    }),

    method_info!(fn DOUBLE(ctx, value: scalar) -> scalar {
        match *value {
            Value::Double(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Double(v.to_f64().ok_or_else(|| overflow!("Double"))?)),
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
    }),

    method_info!([no_method] fn DOUBLE(ctx, value: scalar, culture: scalar) -> scalar {
        // TODO: culture
        let _ = culture;
        match *value {
            Value::Double(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Double(v.to_f64().ok_or_else(|| overflow!("Double"))?)),
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
    }),

    method_info!([no_method] fn DECIMAL(ctx, value: scalar) -> scalar {
        match *value {
            Value::Decimal(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Decimal(v.to_decimal().ok_or_else(|| overflow!("Decimal"))?)),
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
    }),

    method_info!([no_method] fn DECIMAL(ctx, value: scalar, culture: scalar) -> scalar {
        // TODO: culture
        let _ = culture;
        match *value {
            Value::Decimal(_) => value,
            ref v if v.is_number() => ctx.arena(Value::Decimal(v.to_decimal().ok_or_else(|| overflow!("Decimal"))?)),
            Value::String(ref str) => {
                if let Some(v) = bson::Decimal128::parse(str) {
                    ctx.arena(Value::Decimal(v))
                } else {
                    &Value::Null
                }
            }
            _ => &Value::Null,
        }
    }),

    method_info!(fn STRING(ctx, value: scalar) -> scalar {
        match value {
            Value::String(_) => value,
            _ => ctx.arena(Value::String(string_impl(value))),
        }
    }),

    // ==> there is no convert to BsonDocument, must use { .. } syntax

    method_info!(fn ARRAY(ctx, values: sequence) -> scalar {
        ctx.arena(Value::Array(bson::Array::from(values.map_ok(|x| x.clone()).collect::<Result<Vec<_>, _>>()?)))
    }),

    method_info!(fn BINARY(ctx, value: scalar) -> scalar {
        match value {
            Value::Binary(_) => value,
            Value::String(str) => {
                // parse base64
                todo!()
            }
            _ => &Value::Null,
        }
    }),

    method_info!(fn OBJECTID(ctx, value: scalar) -> scalar {
        match value {
            Value::ObjectId(_) => value,
            Value::String(str) => {
                // parse hex
                todo!()
            }
            _ => &Value::Null,
        }
    }),

    method_info!(fn GUID(ctx, value: scalar) -> scalar {
        match value {
            Value::Guid(_) => value,
            Value::String(str) => {
                // parse hex
                todo!()
            }
            _ => &Value::Null,
        }
    }),

    method_info!(fn BOOLEAN(_, value: scalar) -> scalar {
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
    }),

    method_info!([method DATETIME] fn DATETIME(ctx, value: scalar) -> scalar {
        match value {
            Value::DateTime(_) => value,
            Value::String(str) => {
                // TODO: culture and more format
                ctx.arena(Value::DateTime(bson::DateTime::parse_rfc3339(str)
                    .ok_or_else(|| Error::expr_run_error("invalid date"))?))
            }
            _ => &Value::Null,
        }
    }),

    method_info!([method DATETIME_CULTURE] fn DATETIME(ctx, value: scalar, culture: scalar) -> scalar {
        match value {
            Value::DateTime(_) => value,
            Value::String(str) => {
                // TODO: culture and more format
                ctx.arena(Value::DateTime(bson::DateTime::parse_rfc3339(str)
                    .ok_or_else(|| Error::expr_run_error("invalid date"))?))
            }
            _ => &Value::Null,
        }
    }),

    method_info!([method DATETIME_UTC] fn DATETIME_UTC(ctx, value: scalar) -> scalar {
        match value {
            Value::DateTime(_) => value,
            Value::String(str) => {
                // TODO: culture and more format
                ctx.arena(Value::DateTime(bson::DateTime::parse_rfc3339(str)
                    .ok_or_else(|| Error::expr_run_error("invalid date"))?))
            }
            _ => &Value::Null,
        }
    }),

    method_info!([method DATETIME_UTC_CULTURE] fn DATETIME_UTC(ctx, value: scalar, culture: scalar) -> scalar {
        match value {
            Value::DateTime(_) => value,
            Value::String(str) => {
                // TODO: culture and more format
                ctx.arena(Value::DateTime(bson::DateTime::parse_rfc3339(str)
                    .ok_or_else(|| Error::expr_run_error("invalid date"))?))
            }
            _ => &Value::Null,
        }
    }),

    method_info!([method DATETIME_YMD] fn DATETIME(ctx, year: scalar, month: scalar, day: scalar) -> scalar {
        if year.is_number() && month.is_number() && day.is_number() {
            let year = year.to_i32().and_then(|x| x.try_into().ok()).ok_or_else(|| overflow!("Int32"))?;
            let month = month.to_i32().and_then(|x| x.try_into().ok()).ok_or_else(|| overflow!("Int32"))?;
            let day = day.to_i32().and_then(|x| x.try_into().ok()).ok_or_else(|| overflow!("Int32"))?;

            ctx.arena(Value::DateTime(DateTime::from_ymd(year, month, day).ok_or_else(|| overflow!("DateTime"))?))
        } else {
            &Value::Null
        }
    }),

    method_info!([method DATETIME_UTC_YMD] fn DATETIME_UTC(ctx, year: scalar, month: scalar, day: scalar) -> scalar {
        if year.is_number() && month.is_number() && day.is_number() {
            let year = year.to_i32().and_then(|x| x.try_into().ok()).ok_or_else(|| overflow!("Int32"))?;
            let month = month.to_i32().and_then(|x| x.try_into().ok()).ok_or_else(|| overflow!("Int32"))?;
            let day = day.to_i32().and_then(|x| x.try_into().ok()).ok_or_else(|| overflow!("Int32"))?;

            ctx.arena(Value::DateTime(DateTime::from_ymd(year, month, day).ok_or_else(|| overflow!("DateTime"))?))
        } else {
            &Value::Null
        }
    }),

    //endregion

    //region IS_DATETYPE
    method_info!(fn IS_MINVALUE(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::MinValue)) }),
    method_info!(fn IS_NULL(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Null)) }),
    method_info!(fn IS_INT32(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Int32(_))) }),
    method_info!(fn IS_INT64(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Int64(_))) }),
    method_info!(fn IS_DOUBLE(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Double(_))) }),
    method_info!(fn IS_DECIMAL(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Decimal(_))) }),
    method_info!(fn IS_NUMBER(ctx, value: scalar) -> scalar { ctx.bool(value.is_number()) }),
    method_info!(fn IS_STRING(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::String(_))) }),
    method_info!(fn IS_DOCUMENT(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Document(_))) }),
    method_info!(fn IS_ARRAY(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Array(_))) }),
    method_info!(fn IS_BINARY(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Binary(_))) }),
    method_info!(fn IS_OBJECTID(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::ObjectId(_))) }),
    method_info!(fn IS_GUID(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Guid(_))) }),
    method_info!(fn IS_BOOLEAN(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::Boolean(_))) }),
    method_info!(fn IS_DATETIME(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::DateTime(_))) }),
    method_info!(fn IS_MAXVALUE(ctx, value: scalar) -> scalar { ctx.bool(matches!(value, Value::MaxValue)) }),
    //endregion

    //region ALIAS

    method_info!(fn INT(ctx, value: scalar) -> scalar { Methods::INT32(ctx, value)? }),
    method_info!(fn LONG(ctx, value: scalar) -> scalar { Methods::INT64(ctx, value)? }),
    method_info!(fn BOOL(ctx, value: scalar) -> scalar { Methods::BOOLEAN(ctx, value)? }),

    method_info!(fn DATE(ctx, value: scalar) -> scalar { Methods::DATETIME(ctx, value)? }),
    method_info!(fn DATE(ctx, value: scalar, culture: scalar) -> scalar { Methods::DATETIME_CULTURE(ctx, value, culture)? }),
    method_info!(fn DATE_UTC(ctx, value: scalar) -> scalar { Methods::DATETIME_UTC(ctx, value)? }),
    method_info!(fn DATE_UTC(ctx, value: scalar, culture: scalar) -> scalar { Methods::DATETIME_UTC(ctx, value, culture)? }),
    method_info!(fn DATE(ctx, year: scalar, month: scalar, day: scalar) -> scalar { Methods::DATETIME_YMD(ctx, year, month, day)? }),
    method_info!(fn DATE_UTC(ctx, year: scalar, month: scalar, day: scalar) -> scalar { Methods::DATETIME_UTC_YMD(ctx, year, month, day)? }),

    method_info!(fn IS_INT(ctx, value: scalar) -> scalar { Methods::IS_INT32(ctx, value)? }),
    method_info!(fn IS_LONG(ctx, value: scalar) -> scalar { Methods::IS_INT64(ctx, value)? }),
    method_info!(fn IS_BOOL(ctx, value: scalar) -> scalar { Methods::IS_BOOLEAN(ctx, value)? }),
    method_info!(fn IS_DATE(ctx, value: scalar) -> scalar { Methods::IS_DATE(ctx, value)? }),

    //endregion

    //endregion
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
