use crate::query::logical::{BsonType, BsonValue, ComparisonOperator, Expr, PathComponent};
use std::rc::Rc;

pub fn field<T, U>(name: T) -> Rc<Expr>
where
    T: IntoIterator<Item = U>,
    U: Into<PathComponent>,
{
    Rc::new(Expr::Field(name.into_iter().map(|c| c.into()).collect()))
}

pub fn wildcard_field<T, U>(name: T) -> Rc<Expr>
where
    T: IntoIterator<Item = U>,
    U: Into<PathComponent>,
{
    Rc::new(Expr::WildcardField(
        name.into_iter().map(|c| c.into()).collect(),
    ))
}

pub fn lit(value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Literal(value.into()))
}

pub fn exists(exists: bool) -> Rc<Expr> {
    Rc::new(Expr::Exists(exists))
}

pub fn has_type(bson_type: BsonType, negated: bool) -> Rc<Expr> {
    Rc::new(Expr::Type { bson_type, negated })
}

pub fn size(size: usize, negated: bool) -> Rc<Expr> {
    Rc::new(Expr::Size { size, negated })
}

pub fn field_filters<T>(field: Rc<Expr>, predicates: T) -> Rc<Expr>
where
    T: IntoIterator<Item = Rc<Expr>>,
{
    Rc::new(Expr::FieldFilters {
        field,
        filters: predicates.into_iter().collect(),
    })
}

pub fn gt(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::Gt,
        value: lit(bson_value),
    })
}

pub fn gte(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::Gte,
        value: lit(bson_value),
    })
}

pub fn lt(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::Lt,
        value: lit(bson_value),
    })
}

pub fn lte(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::Lte,
        value: lit(bson_value),
    })
}

pub fn within(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::In,
        value: lit(bson_value),
    })
}

pub fn eq(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::Eq,
        value: lit(bson_value),
    })
}

pub fn ne(bson_value: impl Into<BsonValue>) -> Rc<Expr> {
    Rc::new(Expr::Comparison {
        operator: ComparisonOperator::Ne,
        value: lit(bson_value),
    })
}

pub fn all<T, U>(values: T) -> Rc<Expr>
where
    T: IntoIterator<Item = U>,
    U: Into<BsonValue>,
{
    Rc::new(Expr::All(values.into_iter().map(|v| v.into()).collect()))
}

pub fn elem_match<T>(predicates: T) -> Rc<Expr>
where
    T: IntoIterator<Item = Rc<Expr>>,
{
    Rc::new(Expr::ElemMatch(predicates.into_iter().collect()))
}

pub fn and<T>(predicates: T) -> Rc<Expr>
where
    T: IntoIterator<Item = Rc<Expr>>,
{
    Rc::new(Expr::And(predicates.into_iter().collect()))
}

pub fn or<T>(predicates: T) -> Rc<Expr>
where
    T: IntoIterator<Item = Rc<Expr>>,
{
    Rc::new(Expr::Or(predicates.into_iter().collect()))
}

pub fn nor<T>(predicates: T) -> Rc<Expr>
where
    T: IntoIterator<Item = Rc<Expr>>,
{
    Rc::new(Expr::Nor(predicates.into_iter().collect()))
}

pub fn not(predicate: Rc<Expr>) -> Rc<Expr> {
    Rc::new(Expr::Not(predicate))
}
