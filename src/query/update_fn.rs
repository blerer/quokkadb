use crate::query::expr_fn::lit;
use crate::query::update::{
    CurrentDateType, EachOrSingle, PopFrom, PullCriterion, PushSort, PushSpec,
    UpdateExpr, UpdateOp, UpdatePathComponent,
};
use crate::query::{BsonValue, Expr};
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn update<O>(ops: O) -> UpdateExpr
where
    O: IntoIterator<Item = UpdateOp> {
    UpdateExpr { ops: ops.into_iter().collect(), array_filters: BTreeMap::new()  }
}

pub fn update_with_filters<O, F>(ops: O, array_filters: F) -> UpdateExpr
where
    O: IntoIterator<Item = UpdateOp>,
    F: IntoIterator<Item = (String, Arc<Expr>)>, {
    UpdateExpr {
        ops: ops.into_iter().collect(),
        array_filters: array_filters.into_iter().map(|e| (e.0.to_string(), e.1.clone())).collect()
    }
}

pub fn set<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Set { path: path.into_iter().collect(), value: lit(value) }
}

pub fn set_on_insert<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::SetOnInsert { path: path.into_iter().collect(), value: lit(value) }
}

pub fn unset<T>(path: T) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Unset { path: path.into_iter().collect() }
}

pub fn inc<T>(path: T, amount: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Inc { path: path.into_iter().collect(), amount: lit(amount) }
}

pub fn mul<T>(path: T, amount: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Mul { path: path.into_iter().collect(), factor: lit(amount) }
}

pub fn min<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Min { path: path.into_iter().collect(), value: lit(value) }
}

pub fn max<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Max { path: path.into_iter().collect(), value: lit(value) }
}

pub fn rename<T>(from: T, to: T) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Rename { from: from.into_iter().collect(), to: to.into_iter().collect() }
}

pub fn current_date<T>(path: T, type_hint: CurrentDateType) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::CurrentDate {
        path: path.into_iter().collect(),
        type_hint,
    }
}

pub fn add_to_set_single<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::AddToSet { path: path.into_iter().collect(), values: EachOrSingle::Single(lit(value)) }
}

pub fn push_single<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Push {
        path: path.into_iter().collect(),
        spec: PushSpec {
            values: EachOrSingle::Single(lit(value)),
            position: None,
            slice: None,
            sort: None,
        },
    }
}

pub fn push_spec<T>(path: T, spec: PushSpec<Arc<Expr>>) -> UpdateOp where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Push { path: path.into_iter().collect(), spec }
}

pub fn push_each<T, V, B>(path: T, values: V) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent>,
    V: IntoIterator<Item = B>,
    B: Into<BsonValue>,
{
    UpdateOp::Push {
        path: path.into_iter().collect(),
        spec: PushSpec {
            values: EachOrSingle::Each(values.into_iter().map(lit).collect()),
            position: None,
            slice: None,
            sort: None,
        },
    }
}

pub fn pop<T>(path: T, from: PopFrom) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Pop { path: path.into_iter().collect(), from }
}

pub fn push_each_spec<V, B>(
    values: V,
    position: Option<i32>,
    slice: Option<i32>,
    sort: Option<PushSort>,
) -> PushSpec<Arc<Expr>>
where
    V: IntoIterator<Item = B>,
    B: Into<BsonValue>,{

    PushSpec {
        values: EachOrSingle::Each(values.into_iter().map(lit).collect()),
        position,
        slice,
        sort,
    }
}

pub fn by_fields_sort(fields: BTreeMap<String, i32>) -> PushSort {
    PushSort::ByFields(fields)
}

pub fn pull_eq<T>(path: T, value: impl Into<BsonValue>) -> UpdateOp where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Pull { path: path.into_iter().collect(), criterion: PullCriterion::Equals(lit(value)) }
}

pub fn pull_matches<T>(path: T, criterion: Arc<Expr>) -> UpdateOp where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Pull { path: path.into_iter().collect(), criterion: PullCriterion::Matches(criterion) }
}

pub fn pull_all<T, V, B>(path: T, values: V) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent>,
    V: IntoIterator<Item = B>,
    B: Into<BsonValue>,
{
    UpdateOp::PullAll {
        path: path.into_iter().collect(),
        values: values.into_iter().map(lit).collect(),
    }
}

pub fn add_to_set_each<T, V, B>(path: T, values: V) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent>,
    V: IntoIterator<Item = B>,
    B: Into<BsonValue>, {
    UpdateOp::AddToSet { path: path.into_iter().collect(), values: EachOrSingle::Each(values.into_iter().map(lit).collect()) }
}

pub fn bit<T>(path: T, and: Option<i64>, or: Option<i64>, xor: Option<i64>) -> UpdateOp
where
    T: IntoIterator<Item = UpdatePathComponent> {
    UpdateOp::Bit { path: path.into_iter().collect(), and, or, xor }
}

pub fn field_name(name: &str) -> UpdatePathComponent {
    UpdatePathComponent::FieldName(name.to_string())
}

pub fn filter(identifier: &str) -> UpdatePathComponent {
    UpdatePathComponent::Filtered(identifier.to_string())
}

pub fn all() -> UpdatePathComponent {
    UpdatePathComponent::AllElements
}
