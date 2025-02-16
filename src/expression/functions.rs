use super::*;
use crate::bson::Value;

pub(super) fn map(input: SequenceExpr, map_expr: BsonExpression) -> SequenceExpr {
    sequence_expr(move |ctx| {
        let input = input(ctx)?;
        let ctx = ctx.clone();
        let map_expr = map_expr.expression.clone();
        Ok(Box::new(
            input
                .map_ok(move |x| map_expr.clone().execute(ctx.subcontext_root_item(x)))
                .flatten_ok()
                .map(|x| x.and_then(|x| x)),
        ))
    })
}

pub(super) fn filter(input: SequenceExpr, map_expr: BsonExpression) -> SequenceExpr {
    sequence_expr(move |ctx| {
        let input = input(ctx)?;
        let ctx = ctx.clone();
        let map_expr = map_expr.expression.clone();
        Ok(Box::new(
            input
                .filter_map_ok(move |x| {
                    match map_expr.execute_scalar(ctx.subcontext_root_item(x)) {
                        Err(e) => Some(Err(e)),
                        Ok(Value::Boolean(true)) => Some(Ok(x)),
                        _ => None,
                    }
                })
                .map(|x| x.and_then(|x| x)),
        ))
    })
}

pub(super) fn sort(
    input: SequenceExpr,
    map_expr: BsonExpression,
    sort: ScalarExpr,
) -> SequenceExpr {
    sequence_expr(move |ctx| {
        let input = input(ctx)?;
        let sort = sort(ctx)?;
        let ctx = ctx.clone();
        let map_expr = map_expr.expression.clone();

        let ascending = match sort {
            Value::Int32(1..) => true,
            Value::String(s) if s.eq_ignore_ascii_case("asc") => true,
            _ => false,
        };

        Ok(Box::new(
            std::iter::once_with(move || {
                let collation = ctx.collation;
                input
                    .map_ok(move |x| {
                        map_expr
                            .execute_scalar(ctx.subcontext_root_item(x))
                            .map(|k| (x, k))
                    })
                    .map(|x| x.and_then(|x| x))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|mut vec| {
                        if ascending {
                            vec.sort_by(|(_, l), (_, r)| collation.compare(l, r))
                        } else {
                            vec.sort_by(|(_, l), (_, r)| collation.compare(l, r).reverse())
                        }
                        vec.into_iter().map(|(x, _)| x)
                    })
            })
            .flatten_ok(),
        ))
    })
}

pub(super) fn sort_no_order(input: SequenceExpr, map_expr: BsonExpression) -> SequenceExpr {
    sort(input, map_expr, scalar_expr(|_| Ok(&Value::Int32(1))))
}
