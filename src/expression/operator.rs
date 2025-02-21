use super::*;
use crate::bson::Value;

// non operators

pub(super) fn root() -> ScalarExpr {
    scalar_expr(|ctx| {
        ctx.root
            .ok_or_else(|| Error::expr_run_error("Field is invalid here"))
    })
}

pub(super) fn current() -> ScalarExpr {
    scalar_expr(|ctx| {
        ctx.current
            .ok_or_else(|| Error::expr_run_error("Field is invalid here"))
    })
}

// operators

//region Arithmetic

macro_rules! binary {
    ($name: ident, |$left: ident, $right: ident| $value: expr) => {
        pub(super) fn $name(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
            scalar_expr(move |ctx| {
                let $left = left(ctx)?;
                let $right = right(ctx)?;
                let value = $value.into();
                Ok(ctx.arena(value))
            })
        }
    };
}

binary!(add, |left, right| {
    match (left, right) {
        // if both sides are string, concat
        (Value::String(l), Value::String(r)) => Value::String(format!("{l}{r}")),
        // if any sides are string, concat casting both to string
        (l, Value::String(r)) => Value::String(format!("{}{r}", methods::string_impl(l))),
        (Value::String(l), r) => Value::String(format!("{l}{}", methods::string_impl(r))),
        // if any side are DateTime and another is number, add days in date
        (&Value::DateTime(t), v) if v.is_number() => Value::DateTime(
            t.add_ticks(
                v.to_i64()
                    .ok_or_else(|| Error::expr_run_error("overflows"))?,
            ),
        ),
        (v, &Value::DateTime(t)) if v.is_number() => Value::DateTime(
            t.add_ticks(
                v.to_i64()
                    .ok_or_else(|| Error::expr_run_error("overflows"))?,
            ),
        ),

        // if both sides are number, add as math
        (l, r) => l + r,
    }
});

binary!(minus, |left, right| {
    match (left, right) {
        // if any side are DateTime and another is number, add days in date
        (Value::DateTime(t), v) if v.is_number() => Value::DateTime(
            t.add_ticks(
                -v.to_i64()
                    .ok_or_else(|| Error::expr_run_error("overflows"))?,
            ),
        ),
        (v, Value::DateTime(t)) if v.is_number() => Value::DateTime(
            t.add_ticks(
                -v.to_i64()
                    .ok_or_else(|| Error::expr_run_error("overflows"))?,
            ),
        ),
        // if both sides are number, minus as math
        (l, r) => l - r,
    }
});

binary!(multiply, |left, right| left * right);

binary!(divide, |left, right| left / right);

binary!(r#mod, |left, right| {
    let left = if left.is_number() {
        left.to_i32()
            .ok_or_else(|| Error::expr_run_error("overflows"))?
    } else {
        return Ok(&Value::Null);
    };

    let right = if right.is_number() {
        right
            .to_i32()
            .ok_or_else(|| Error::expr_run_error("overflows"))?
    } else {
        return Ok(&Value::Null);
    };

    Value::Int32(left % right)
});

//endregion

//region Predicates

macro_rules! predicates {
    ($simple: ident, $all: ident, $any: ident, |$ctx: ident, $left: ident, $right: ident| $compare: expr) => {
        pub(super) fn $simple(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
            scalar_expr(move |$ctx| {
                let $left = left($ctx)?;
                let $right = right($ctx)?;
                let result = $compare;
                Ok($ctx.bool(result))
            })
        }

        pub(super) fn $all(left: SequenceExpr, right: ScalarExpr) -> ScalarExpr {
            scalar_expr(move |$ctx| {
                let mut left = left($ctx)?;
                let $right = right($ctx)?;
                while let Some($left) = left.next().transpose()? {
                    if !$compare {
                        return Ok($ctx.bool(false));
                    }
                }
                return Ok($ctx.bool(true));
            })
        }

        pub(super) fn $any(left: SequenceExpr, right: ScalarExpr) -> ScalarExpr {
            scalar_expr(move |$ctx| {
                let mut left = left($ctx)?;
                let $right = right($ctx)?;
                while let Some($left) = left.next().transpose()? {
                    if $compare {
                        return Ok($ctx.bool(true));
                    }
                }
                return Ok($ctx.bool(false));
            })
        }
    };
}

predicates!(eq, eq_all, eq_any, |ctx, left, right| {
    ctx.collation.compare(left, right).is_eq()
});
predicates!(gt, gt_all, gt_any, |ctx, left, right| {
    ctx.collation.compare(left, right).is_gt()
});
predicates!(gte, gte_all, gte_any, |ctx, left, right| {
    ctx.collation.compare(left, right).is_ge()
});
predicates!(lt, lt_all, lt_any, |ctx, left, right| {
    ctx.collation.compare(left, right).is_lt()
});
predicates!(lte, lte_all, lte_any, |ctx, left, right| {
    ctx.collation.compare(left, right).is_le()
});
predicates!(neq, neq_all, neq_any, |ctx, left, right| {
    ctx.collation.compare(left, right).is_ne()
});

//predicates!(like, like_all, like_any, |ctx, left, right| {
//    left.as_str().zip(right.as_str()).map(|l, r| ctx.collation.sql_like(l, r)).unwrap_or(false)
//});

predicates!(between, between_all, between_any, |ctx, left, right| {
    let [start, end] = right.as_array().unwrap().as_slice() else {
        unreachable!()
    };
    ctx.collation.compare(start, left).is_ge() && ctx.collation.compare(left, end).is_le()
});

predicates!(r#in, in_all, in_any, |ctx, left, right| {
    if let Some(array) = right.as_array() {
        array
            .as_slice()
            .iter()
            .any(|x| ctx.collation.compare(x, left).is_eq())
    } else {
        false
    }
});

//endregion

// region Path Navigation

pub(super) fn parameter_path(name: String) -> ScalarExpr {
    scalar_expr(move |ctx| Ok(ctx.parameters.get(&name)))
}

pub(super) fn member_path(expr: ScalarExpr, path: String) -> ScalarExpr {
    if path.is_empty() {
        expr
    } else {
        scalar_expr(move |ctx| {
            let value = expr(ctx)?;
            Ok(value
                .as_document()
                .map(|x| x.get(&path))
                .unwrap_or(&Value::Null))
        })
    }
}

//endregion

//region Array Index/Filter

pub(super) fn array_index_positive(expr: ScalarExpr, index: usize) -> ScalarExpr {
    scalar_expr(move |ctx| {
        Ok(expr(ctx)?
            .as_array()
            .and_then(|array| array.as_slice().get(index))
            .unwrap_or(&Value::Null))
    })
}

pub(super) fn array_index_negative(expr: ScalarExpr, index: usize) -> ScalarExpr {
    scalar_expr(move |ctx| {
        Ok(expr(ctx)?
            .as_array()
            .and_then(|array| {
                array
                    .len()
                    .checked_sub(index)
                    .and_then(|idx| array.as_slice().get(idx))
            })
            .unwrap_or(&Value::Null))
    })
}

pub(super) fn array_index_expr(expr: ScalarExpr, index: ScalarExpr) -> ScalarExpr {
    scalar_expr(move |ctx| {
        let value = expr(ctx)?;
        let index = index(ctx)?;

        let Some(array) = value.as_array() else {
            return Ok(&Value::Null);
        };

        let Some(index) = index.as_i32() else {
            return Err(Error::expr_run_error(
                "Parameter expression must return number when called inside an array",
            ));
        };

        let index = if index < 0 {
            array.len() as isize - index as isize
        } else {
            index as isize
        };

        if 0 <= index && index < array.len() as isize {
            Ok(&array.as_slice()[index as usize])
        } else {
            Ok(&Value::Null)
        }
    })
}

pub(super) fn array_filter_star(value: ScalarExpr) -> SequenceExpr {
    sequence_expr(move |ctx| {
        Ok(Box::new(
            value(ctx)?
                .as_array()
                .map(|x| x.as_slice())
                .into_iter()
                .flatten()
                .map(Ok),
        ))
    })
}

pub(super) fn array_filter_expr(value: ScalarExpr, filter: BsonExpression) -> SequenceExpr {
    sequence_expr(move |ctx| {
        let expression = filter.expression.clone();
        let ctx = ctx.clone();
        Ok(Box::new(
            value(&ctx)?
                .as_array()
                .map(|x| x.as_slice())
                .into_iter()
                .flatten()
                .filter_map(move |x| {
                    match expression.execute_scalar(ctx.subcontext_root_item(x)) {
                        Err(e) => Some(Err(e)),
                        Ok(Value::Boolean(true)) => Some(Ok(x)),
                        _ => None,
                    }
                }),
        ))
    })
}

// endregion

//region Object Creation

pub(super) fn document_init(keys: Vec<String>, values: Vec<ScalarExpr>) -> ScalarExpr {
    scalar_expr(move |ctx| {
        let mut values = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| Ok::<_, crate::Error>((k, v(ctx)?)));
        let mut result = bson::Document::new();

        while let Some((key, value)) = values.next().transpose()? {
            result.insert(key.clone(), value.clone());
        }

        Ok(ctx.arena(result.into()))
    })
}

pub(super) fn array_init(values: Vec<ScalarExpr>) -> ScalarExpr {
    scalar_expr(move |ctx| {
        Ok(ctx.arena(
            bson::Array::from(
                values
                    .iter()
                    .map(|f| f(ctx).cloned())
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into(),
        ))
    })
}

// endregion
