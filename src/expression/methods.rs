use super::*;
use crate::bson::Value;

//region
pub(super) fn count(values: SequenceExpr) -> ScalarExpr {
    scalar_expr(move |ctx| Ok(ctx.arena(Value::Int32(values(ctx)?.count() as i32))))
}

// TODO: implement other methods
//endregion

pub(super) fn string_impl(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => bson::to_json(other),
    }
}
