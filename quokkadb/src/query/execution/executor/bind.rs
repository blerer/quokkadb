use crate::error::Result;
use crate::query::{BsonValue, Expr, Parameters};
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

pub(super) fn bind_key_range_parameters(
    range: &Interval<Arc<Expr>>,
    parameters: &Parameters,
) -> Result<Interval<Vec<u8>>> {
    let start = bind_key_bound_parameter(range.start_bound(), parameters)?;
    let end = bind_key_bound_parameter(range.end_bound(), parameters)?;
    Ok(Interval::new(start, end))
}

fn bind_key_bound_parameter(
    bound: Bound<&Arc<Expr>>,
    parameters: &Parameters,
) -> Result<Bound<Vec<u8>>> {
    let bound = match bound {
        Bound::Included(expr) => Bound::Included(bind_parameter(expr, parameters).try_into_key()?),
        Bound::Excluded(expr) => Bound::Excluded(bind_parameter(expr, parameters).try_into_key()?),
        Bound::Unbounded => Bound::Unbounded,
    };
    Ok(bound)
}

pub(super) fn bind_key_parameter(expr: &Expr, parameters: &Parameters) -> Result<Vec<u8>> {
    if let Expr::Placeholder(idx) = expr {
        Ok(parameters.get(*idx).try_into_key()?)
    } else {
        unreachable!("Expecting placeholder but was: {:?}", expr);
    }
}

pub(super) fn bind_parameter(expr: &Expr, parameters: &Parameters) -> BsonValue {
    if let Expr::Placeholder(idx) = expr {
        parameters.get(*idx).clone()
    } else {
        unreachable!("Expecting placeholder but was: {:?}", expr)
    }
}
