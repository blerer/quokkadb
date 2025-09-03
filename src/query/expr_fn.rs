use crate::query::{BsonValue, ComparisonOperator, Expr, PathComponent, Projection, ProjectionExpr, SortField, SortOrder};
use std::sync::Arc;
use crate::util::interval::Interval;

pub fn field<T, U>(name: T) -> Arc<Expr>
where
    T: IntoIterator<Item = U>,
    U: Into<PathComponent>,
{
    Arc::new(Expr::Field(name.into_iter().map(|c| c.into()).collect()))
}

pub fn lit(value: impl Into<BsonValue>) -> Arc<Expr> {
    Arc::new(Expr::Literal(value.into()))
}

pub fn placeholder(idx: u32) -> Arc<Expr> {
    Arc::new(Expr::Placeholder(idx))
}

pub fn exists(exists: bool) -> Arc<Expr> {
    Arc::new(Expr::Exists(exists))
}

pub fn has_type(bson_type: Arc<Expr>, negated: bool) -> Arc<Expr> {
    Arc::new(Expr::Type { bson_type, negated })
}

pub fn size(size: Arc<Expr>, negated: bool) -> Arc<Expr> {
    Arc::new(Expr::Size { size, negated })
}

pub fn field_filters<T>(field: Arc<Expr>, predicates: T) -> Arc<Expr>
where
    T: IntoIterator<Item = Arc<Expr>>,
{
    Arc::new(Expr::FieldFilters {
        field,
        filters: predicates.into_iter().collect(),
    })
}

pub fn gt(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Gt,
        value: bson_value,
    })
}

pub fn gte(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Gte,
        value: bson_value,
    })
}

pub fn lt(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Lt,
        value: bson_value,
    })
}

pub fn lte(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Lte,
        value: bson_value,
    })
}

pub fn within(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::In,
        value: bson_value,
    })
}

pub fn nin(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Nin,
        value: bson_value,
    })
}

pub fn eq(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Eq,
        value: bson_value,
    })
}

pub fn ne(bson_value: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Comparison {
        operator: ComparisonOperator::Ne,
        value: bson_value,
    })
}

pub fn all(array: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::All(array))
}

pub fn elem_match<T>(predicates: T) -> Arc<Expr>
where
    T: IntoIterator<Item = Arc<Expr>>,
{
    Arc::new(Expr::ElemMatch(predicates.into_iter().collect()))
}

pub fn and<T>(predicates: T) -> Arc<Expr>
where
    T: IntoIterator<Item = Arc<Expr>>,
{
    Arc::new(Expr::And(predicates.into_iter().collect()))
}

pub fn or<T>(predicates: T) -> Arc<Expr>
where
    T: IntoIterator<Item = Arc<Expr>>,
{
    Arc::new(Expr::Or(predicates.into_iter().collect()))
}

pub fn nor<T>(predicates: T) -> Arc<Expr>
where
    T: IntoIterator<Item = Arc<Expr>>,
{
    Arc::new(Expr::Nor(predicates.into_iter().collect()))
}

pub fn not(predicate: Arc<Expr>) -> Arc<Expr> {
    Arc::new(Expr::Not(predicate))
}

pub fn interval(interval: Interval<Arc<Expr>>) -> Arc<Expr>
{
    Arc::new(Expr::Interval(interval))
}

pub fn proj_field() -> Arc<ProjectionExpr> {
    Arc::new(ProjectionExpr::Field)
}

pub fn proj_positional_field() -> Arc<ProjectionExpr> {
    Arc::new(ProjectionExpr::PositionalFieldRef)
}

pub fn proj_slice(skip: Option<i32>, limit: i32) -> Arc<ProjectionExpr> {
    Arc::new(ProjectionExpr::Slice { skip, limit })
}

pub fn proj_elem_match(filter: Arc<Expr>) -> Arc<ProjectionExpr> {
    Arc::new(ProjectionExpr::ElemMatch { filter })
}

pub fn proj_fields<T, U>(children: T) -> Arc<ProjectionExpr>
where
    T: IntoIterator<Item = (U, Arc<ProjectionExpr>)>,
    U: Into<PathComponent>,
{
    Arc::new(ProjectionExpr::Fields {
        children: children.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    })
}

pub fn proj_array_elements<T, U>(children: T) -> Arc<ProjectionExpr>
where
    T: IntoIterator<Item = (U, Arc<ProjectionExpr>)>,
    U: Into<PathComponent>,
{
    Arc::new(ProjectionExpr::ArrayElements {
        children: children.into_iter().map(|(k, v)| (k.into(), v)).collect(),
    })
}

pub fn include(proj_expr: Arc<ProjectionExpr>) -> Arc<Projection> {
    Arc::new(Projection::Include(proj_expr))
}

pub fn exclude(proj_expr: Arc<ProjectionExpr>) -> Arc<Projection> {
    Arc::new(Projection::Exclude(proj_expr))
}

pub fn sort_asc(field: Arc<Expr>) ->SortField {
    SortField::asc(field)
}


