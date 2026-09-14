use crate::error::Error;
use crate::query::update::{
    CurrentDateType, EachOrSingle, PopFrom, PullCriterion, PushSort, PushSpec, UpdateExpr,
    UpdateOp, UpdatePathComponent,
};
use crate::query::{
    BsonValue, ComparisonOperator, IndexKeyField, IndexKeySpec, PathComponent, Projection,
    ProjectionExpr, parser,
};
use crate::query::{Expr, SortField, SortOrder, format_path};
use bson::{Document, serialize_to_bson};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

pub use crate::query::ReturnDocument;

/// A Serde value whose named fields can be described by the typed query API.
///
/// Unlike [`QuokkaDocument`], an embedded type has no collection identity and
/// therefore does not require an `_id` field.
pub trait QuokkaType: Serialize + DeserializeOwned + Send + Sync + 'static {
    type Fields<D>;

    fn fields<D>(path: TypedPath) -> Self::Fields<D>;
}

/// Maps a Rust value type to its typed-query field proxy.
pub trait QueryFieldType {
    type Field<D>;

    #[doc(hidden)]
    fn field<D>(path: TypedPath) -> Self::Field<D>;
}

/// A BSON leaf value supported by the typed query API.
///
/// Implement this trait for a custom Serde value to expose it as a typed field.
/// A type must not implement both `QuokkaScalar` and [`QuokkaType`].
pub trait QuokkaScalar: Serialize + DeserializeOwned + Send + Sync + 'static {}

/// A scalar value that supports `$inc` updates.
pub trait NumericValue: QuokkaScalar {}

/// An integer value that supports `$bit` updates.
pub trait BitwiseValue: QuokkaScalar {}

macro_rules! value_operations {
    ($value:ty) => {
        /// Builds a filter that matches fields equal to `value`.
        pub fn eq(&self, value: impl Into<$value>) -> Filter<D> {
            comparison_filter(self.path.clone(), ComparisonOperator::Eq, value.into())
        }

        /// Builds a filter that matches fields not equal to `value`.
        pub fn ne(&self, value: impl Into<$value>) -> Filter<D> {
            comparison_filter(self.path.clone(), ComparisonOperator::Ne, value.into())
        }

        /// Builds a filter that matches fields greater than `value` in BSON order.
        pub fn gt(&self, value: impl Into<$value>) -> Filter<D> {
            comparison_filter(self.path.clone(), ComparisonOperator::Gt, value.into())
        }

        /// Builds a filter that matches fields greater than or equal to `value` in BSON order.
        pub fn gte(&self, value: impl Into<$value>) -> Filter<D> {
            comparison_filter(self.path.clone(), ComparisonOperator::Gte, value.into())
        }

        /// Builds a filter that matches fields less than `value` in BSON order.
        pub fn lt(&self, value: impl Into<$value>) -> Filter<D> {
            comparison_filter(self.path.clone(), ComparisonOperator::Lt, value.into())
        }

        /// Builds a filter that matches fields less than or equal to `value` in BSON order.
        pub fn lte(&self, value: impl Into<$value>) -> Filter<D> {
            comparison_filter(self.path.clone(), ComparisonOperator::Lte, value.into())
        }

        /// Creates a `$min` update using BSON comparison order.
        ///
        /// The update replaces an existing field only when `value` is smaller,
        /// and sets a missing field to `value`.
        pub fn min(&self, value: impl Into<$value>) -> Update<D> {
            value_update(
                self.path.clone(),
                |path, value| UpdateOp::Min { path, value },
                value.into(),
            )
        }

        /// Creates a `$max` update using BSON comparison order.
        ///
        /// The update replaces an existing field only when `value` is greater,
        /// and sets a missing field to `value`.
        pub fn max(&self, value: impl Into<$value>) -> Update<D> {
            value_update(
                self.path.clone(),
                |path, value| UpdateOp::Max { path, value },
                value.into(),
            )
        }

        /// Creates a `$set` update that replaces this field with `value`.
        pub fn set(&self, value: impl Into<$value>) -> Update<D> {
            value_update(
                self.path.clone(),
                |path, value| UpdateOp::Set { path, value },
                value.into(),
            )
        }

        /// Creates a `$setOnInsert` update that sets this field only during an upsert insert.
        pub fn set_on_insert(&self, value: impl Into<$value>) -> Update<D> {
            value_update(
                self.path.clone(),
                |path, value| UpdateOp::SetOnInsert { path, value },
                value.into(),
            )
        }

        /// Creates an ascending sort specification for this field.
        pub fn asc(&self) -> Sort<D> {
            path_sort(self.path.clone(), crate::query::SortOrder::Ascending)
        }

        /// Creates a descending sort specification for this field.
        pub fn desc(&self) -> Sort<D> {
            path_sort(self.path.clone(), crate::query::SortOrder::Descending)
        }

        /// Creates an ascending index specification for this field.
        pub fn index_asc(&self) -> Index<D> {
            path_index(self.path.clone(), crate::query::SortOrder::Ascending)
        }

        /// Creates a descending index specification for this field.
        pub fn index_desc(&self) -> Index<D> {
            path_index(self.path.clone(), crate::query::SortOrder::Descending)
        }
    };
}

pub trait QuokkaDocument: QuokkaType {
    type Id: DeserializeOwned + 'static;

    fn id(&self) -> &Self::Id;

    /// Returns field proxies rooted at this document's top level.
    fn root_fields() -> Self::Fields<Self>
    where
        Self: Sized,
    {
        Self::fields(TypedPath::empty())
    }

    fn update(f: impl FnOnce(&Self::Fields<Self>) -> Update<Self>) -> Update<Self>
    where
        Self: Sized,
    {
        f(&Self::root_fields())
    }
}

/// A field-like value with a BSON path that can be used in typed projections.
#[doc(hidden)]
pub trait TypedQueryField<D> {
    type Value;

    fn query_path(&self) -> TypedPath;
}

/// A composable BSON path used by typed field handles.
///
/// Typed field proxies construct this owned representation as they extend a path.
#[allow(dead_code)] // Used by the nested and collection proxies built on this path layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedPath {
    components: Vec<PathComponent>,
}

#[allow(dead_code)] // Used by the nested and collection proxies built on this path layer.
impl TypedPath {
    pub fn empty() -> Self {
        Self { components: vec![] }
    }

    pub fn root(name: impl Into<String>) -> Self {
        Self {
            components: vec![PathComponent::FieldName(name.into())],
        }
    }

    pub fn field(mut self, name: impl Into<String>) -> Self {
        self.components.push(PathComponent::FieldName(name.into()));
        self
    }

    pub(crate) fn array_element(mut self, index: usize) -> Self {
        self.components.push(PathComponent::ArrayElement(index));
        self
    }

    fn as_slice(&self) -> &[PathComponent] {
        &self.components
    }

    fn into_update_path(self) -> Vec<UpdatePathComponent> {
        self.components
            .into_iter()
            .map(|component| match component {
                PathComponent::FieldName(name) => UpdatePathComponent::FieldName(name),
                PathComponent::ArrayElement(index) => UpdatePathComponent::ArrayElement(index),
            })
            .collect()
    }
}

/// A typed scalar field backed by a BSON path.
#[allow(dead_code)] // Exposed through nested and collection proxies in the next layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field<D, V: QuokkaScalar> {
    path: TypedPath,
    _marker: PhantomData<fn() -> (D, V)>,
}

impl<D, V: QuokkaScalar> Field<D, V> {
    pub fn new(path: TypedPath) -> Self {
        Self {
            path,
            _marker: PhantomData,
        }
    }
}

impl<D, V: QuokkaScalar> Field<D, V> {
    value_operations!(V);

    /// Builds a filter that matches fields equal to any supplied value.
    pub fn in_values<I, U>(&self, values: I) -> Filter<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<V>,
    {
        let path = self.path.clone();
        let operator = ComparisonOperator::In;
        let values1 = values.into_iter().map(Into::into).collect::<Vec<V>>();
        comparison_filter(path, operator, values1)
    }

    /// Builds a filter that matches fields unequal to every supplied value.
    pub fn nin<I, U>(&self, values: I) -> Filter<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<V>,
    {
        let path = self.path.clone();
        let operator = ComparisonOperator::Nin;
        let values1 = values.into_iter().map(Into::into).collect::<Vec<V>>();
        comparison_filter(path, operator, values1)
    }
}

impl<D, V: NumericValue> Field<D, V> {
    /// Creates a `$inc` update that adds `amount` to this field.
    pub fn inc(&self, amount: impl Into<V>) -> Update<D> {
        value_update(
            self.path.clone(),
            |path, amount| UpdateOp::Inc { path, amount },
            amount.into(),
        )
    }

    /// Creates a `$mul` update that multiplies this field by `factor`.
    pub fn mul(&self, factor: impl Into<V>) -> Update<D> {
        value_update(
            self.path.clone(),
            |path, factor| UpdateOp::Mul { path, factor },
            factor.into(),
        )
    }
}

impl<D, V: BitwiseValue> Field<D, V> {
    /// Creates a `$bit` update using any combination of AND, OR, and XOR masks.
    pub fn bit(&self, and: Option<i64>, or: Option<i64>, xor: Option<i64>) -> Update<D> {
        Update::from_op(UpdateOp::Bit {
            path: self.path.clone().into_update_path(),
            and,
            or,
            xor,
        })
    }
}

impl<D> Field<D, bson::DateTime> {
    /// Creates a `$currentDate` update that sets this field to the current BSON date.
    pub fn current_date(&self) -> Update<D> {
        current_date_update(self.path.clone(), CurrentDateType::Date)
    }
}

impl<D> Field<D, bson::Timestamp> {
    /// Creates a `$currentDate` update that sets this field to the current BSON timestamp.
    pub fn current_timestamp(&self) -> Update<D> {
        current_date_update(self.path.clone(), CurrentDateType::Timestamp)
    }
}

/// A nullable typed field that preserves `Option` in selections while exposing
/// the query API of its contained value.
pub struct OptionalField<D, V: QueryFieldType> {
    path: TypedPath,
    inner: V::Field<D>,
}

impl<D, V: QueryFieldType> OptionalField<D, V> {
    #[doc(hidden)]
    pub fn new(path: TypedPath) -> Self {
        Self {
            path: path.clone(),
            inner: V::field(path),
        }
    }

    /// Builds a filter that matches documents where this field exists.
    pub fn exists(&self) -> Filter<D> {
        path_exists(self.path.clone())
    }

    /// Creates an `$unset` update that removes this field.
    pub fn unset(&self) -> Update<D> {
        path_unset(self.path.clone())
    }
}

impl<D, V: QueryFieldType> Deref for OptionalField<D, V> {
    type Target = V::Field<D>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// A typed array field backed by a BSON path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayField<D, T> {
    path: TypedPath,
    _marker: PhantomData<fn() -> (D, T)>,
}

impl<D, T> ArrayField<D, T> {
    #[doc(hidden)]
    pub fn new(path: TypedPath) -> Self {
        Self {
            path,
            _marker: PhantomData,
        }
    }

    pub fn at(&self, index: usize) -> T::Field<D>
    where
        T: QueryFieldType,
    {
        T::field(self.path.clone().array_element(index))
    }

    pub fn len_eq(&self, length: usize) -> Filter<D> {
        array_size_filter(self.path.clone(), length)
    }
}

impl<D, T: Serialize> ArrayField<D, T> {
    pub fn any_eq(&self, value: impl Into<T>) -> Filter<D> {
        array_any_eq_filter(self.path.clone(), value.into())
    }

    pub fn all<I, U>(&self, values: I) -> Filter<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<T>,
    {
        array_all_filter(
            self.path.clone(),
            values.into_iter().map(Into::into).collect::<Vec<T>>(),
        )
    }

    value_operations!(Vec<T>);

    /// Creates an `$addToSet` update that adds `value` when it is not already present.
    pub fn add_to_set(&self, value: impl Into<T>) -> Update<D> {
        array_values_update(
            self.path.clone(),
            |path, values| UpdateOp::AddToSet {
                path,
                values: EachOrSingle::Single(values.into_iter().next().unwrap()),
            },
            [value.into()],
        )
    }

    /// Creates an `$addToSet` update that adds every unique value from `values`.
    pub fn add_to_set_each<I, U>(&self, values: I) -> Update<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<T>,
    {
        array_values_update(
            self.path.clone(),
            |path, values| UpdateOp::AddToSet {
                path,
                values: EachOrSingle::Each(values),
            },
            values.into_iter().map(Into::into),
        )
    }

    /// Creates a `$push` update that appends `value` to this array.
    pub fn push(&self, value: impl Into<T>) -> Update<D> {
        array_values_update(
            self.path.clone(),
            |path, values| UpdateOp::Push {
                path,
                spec: PushSpec {
                    values: EachOrSingle::Single(values.into_iter().next().unwrap()),
                    position: None,
                    slice: None,
                    sort: None,
                },
            },
            [value.into()],
        )
    }

    /// Creates a `$push` update with `$each` and the supplied modifiers.
    pub fn push_each<I, U>(&self, values: I, options: PushOptions<D, T>) -> Update<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<T>,
    {
        array_values_update(
            self.path.clone(),
            move |path, values| UpdateOp::Push {
                path,
                spec: PushSpec {
                    values: EachOrSingle::Each(values),
                    position: options.position,
                    slice: options.slice,
                    sort: options.sort,
                },
            },
            values.into_iter().map(Into::into),
        )
    }

    /// Creates a `$push` update with `$each` and modifiers configured for this array's elements.
    pub fn push_each_with<I, U>(
        &self,
        values: I,
        configure: impl FnOnce(PushOptions<D, T>) -> PushOptions<D, T>,
    ) -> Update<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<T>,
    {
        self.push_each(values, configure(PushOptions::new()))
    }

    /// Creates a `$pop` update that removes the first array element.
    pub fn pop_first(&self) -> Update<D> {
        array_path_update(self.path.clone(), |path| UpdateOp::Pop {
            path,
            from: PopFrom::First,
        })
    }

    /// Creates a `$pop` update that removes the last array element.
    pub fn pop_last(&self) -> Update<D> {
        array_path_update(self.path.clone(), |path| UpdateOp::Pop {
            path,
            from: PopFrom::Last,
        })
    }

    /// Creates a `$pull` update that removes every element equal to `value`.
    pub fn pull(&self, value: impl Into<T>) -> Update<D> {
        let value = literal_expr(value.into());
        array_path_update(self.path.clone(), |path| UpdateOp::Pull {
            path,
            criterion: PullCriterion::Equals(value),
        })
    }

    /// Creates a `$pullAll` update that removes every element equal to any supplied value.
    pub fn pull_all<I, U>(&self, values: I) -> Update<D>
    where
        I: IntoIterator<Item = U>,
        U: Into<T>,
    {
        array_values_update(
            self.path.clone(),
            |path, values| UpdateOp::PullAll { path, values },
            values.into_iter().map(Into::into),
        )
    }
}

impl<D, T: QuokkaScalar> ArrayField<D, T> {
    /// Builds an `$elemMatch` filter for scalar array elements.
    pub fn any_where(&self, predicate: impl FnOnce(&Field<D, T>) -> Filter<D>) -> Filter<D> {
        let element = Field::new(TypedPath::empty());
        scalar_array_any_filter(self.path.clone(), predicate(&element))
    }
}

impl<D, T: QuokkaType> ArrayField<D, T> {
    pub fn any(&self, predicate: impl FnOnce(&T::Fields<D>) -> Filter<D>) -> Filter<D> {
        array_any_filter(self.path.clone(), predicate(&T::fields(TypedPath::empty())))
    }

    /// Creates a `$pull` update that removes embedded elements matching `predicate`.
    pub fn pull_where(&self, predicate: impl FnOnce(&T::Fields<D>) -> Filter<D>) -> Update<D> {
        let criterion = predicate(&T::fields(TypedPath::empty())).into_expr();
        array_path_update(self.path.clone(), |path| UpdateOp::Pull {
            path,
            criterion: PullCriterion::Matches(criterion),
        })
    }
}

/// Configures `$push` modifiers for a typed array update.
pub struct PushOptions<D, T> {
    position: Option<i32>,
    slice: Option<i32>,
    sort: Option<PushSort>,
    _marker: PhantomData<fn() -> (D, T)>,
}

impl<D, T> Default for PushOptions<D, T> {
    fn default() -> Self {
        Self {
            position: None,
            slice: None,
            sort: None,
            _marker: PhantomData,
        }
    }
}

impl<D, T> PushOptions<D, T> {
    /// Creates an empty `$push` modifier set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `$position` at which `$each` values are inserted.
    pub fn position(mut self, position: i32) -> Self {
        self.position = Some(position);
        self
    }

    /// Sets the `$slice` limit applied after values are inserted and sorted.
    pub fn slice(mut self, slice: i32) -> Self {
        self.slice = Some(slice);
        self
    }

    /// Sorts array values in ascending BSON order after insertion.
    pub fn sort_ascending(mut self) -> Self {
        self.sort = Some(PushSort::Ascending);
        self
    }

    /// Sorts array values in descending BSON order after insertion.
    pub fn sort_descending(mut self) -> Self {
        self.sort = Some(PushSort::Descending);
        self
    }
}

impl<D, T: QuokkaType> PushOptions<D, T> {
    /// Sorts embedded array elements by the supplied typed fields after insertion.
    pub fn sort_by(mut self, sort: impl FnOnce(T::Fields<D>) -> Sort<D>) -> Self {
        let fields = sort(T::fields(TypedPath::empty())).into_fields();
        let sort = fields
            .into_iter()
            .map(|field| {
                let Expr::Field(path) = field.field.as_ref() else {
                    panic!("typed push sort fields must be BSON paths");
                };
                let order = match field.order {
                    SortOrder::Ascending => 1,
                    SortOrder::Descending => -1,
                };
                (format_path(path), order)
            })
            .collect();
        self.sort = Some(PushSort::ByFields(sort));
        self
    }
}

/// A typed string-keyed map field backed by a BSON path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapField<D, V> {
    path: TypedPath,
    _marker: PhantomData<fn() -> (D, V)>,
}

impl<D, V> MapField<D, V> {
    #[doc(hidden)]
    pub fn new(path: TypedPath) -> Self {
        Self {
            path,
            _marker: PhantomData,
        }
    }

    pub fn key(&self, key: &str) -> V::Field<D>
    where
        V: QueryFieldType,
    {
        V::field(self.path.clone().field(key))
    }
}

impl<D, V: Serialize> MapField<D, V> {
    value_operations!(BTreeMap<String, V>);
}

/// A typed proxy for an embedded [`QuokkaType`] value.
pub struct ObjectField<D, V: QuokkaType> {
    path: TypedPath,
    fields: V::Fields<D>,
}

impl<D, V: QuokkaType> ObjectField<D, V> {
    #[doc(hidden)]
    pub fn from_path(path: TypedPath) -> Self {
        Self {
            path: path.clone(),
            fields: V::fields(path),
        }
    }

    value_operations!(V);
}

impl<D, V: QuokkaType> Deref for ObjectField<D, V> {
    type Target = V::Fields<D>;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

macro_rules! impl_scalar {
    ($($type:ty),+ $(,)?) => {$(
        impl QuokkaScalar for $type {
        }
    )+};
}

macro_rules! impl_numeric_value {
    ($($type:ty),+ $(,)?) => {$(
        impl NumericValue for $type {}
    )+};
}

impl_scalar!(
    bool,
    String,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    bson::DateTime,
    bson::Timestamp,
    bson::Decimal128,
    bson::Binary,
    bson::oid::ObjectId,
);

impl_numeric_value!(
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    bson::Decimal128,
);

impl BitwiseValue for i32 {}
impl BitwiseValue for i64 {}

impl<T: QuokkaScalar> QueryFieldType for T {
    type Field<D> = Field<D, Self>;

    fn field<D>(path: TypedPath) -> Self::Field<D> {
        Field::new(path)
    }
}

impl<T: QueryFieldType> QueryFieldType for Option<T> {
    type Field<D> = OptionalField<D, T>;

    fn field<D>(path: TypedPath) -> Self::Field<D> {
        OptionalField::new(path)
    }
}

impl<T: QueryFieldType> QueryFieldType for Vec<T> {
    type Field<D> = ArrayField<D, T>;
    fn field<D>(path: TypedPath) -> Self::Field<D> {
        ArrayField::new(path)
    }
}

impl<T: QueryFieldType> QueryFieldType for BTreeMap<String, T> {
    type Field<D> = MapField<D, T>;
    fn field<D>(path: TypedPath) -> Self::Field<D> {
        MapField::new(path)
    }
}

impl<D, V: QuokkaScalar> TypedQueryField<D> for &Field<D, V> {
    type Value = V;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V: QuokkaScalar> TypedQueryField<D> for Field<D, V> {
    type Value = V;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, T> TypedQueryField<D> for &ArrayField<D, T> {
    type Value = Vec<T>;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, T> TypedQueryField<D> for ArrayField<D, T> {
    type Value = Vec<T>;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V> TypedQueryField<D> for &MapField<D, V> {
    type Value = BTreeMap<String, V>;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V> TypedQueryField<D> for MapField<D, V> {
    type Value = BTreeMap<String, V>;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V: QueryFieldType> TypedQueryField<D> for &OptionalField<D, V> {
    type Value = Option<V>;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V: QueryFieldType> TypedQueryField<D> for OptionalField<D, V> {
    type Value = Option<V>;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V: QuokkaType> TypedQueryField<D> for &ObjectField<D, V> {
    type Value = V;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

impl<D, V: QuokkaType> TypedQueryField<D> for ObjectField<D, V> {
    type Value = V;

    fn query_path(&self) -> TypedPath {
        self.path.clone()
    }
}

fn path_exists<D>(path: TypedPath) -> Filter<D> {
    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::Exists(true))],
    }))
}

fn path_unset<D>(path: TypedPath) -> Update<D> {
    Update::from_op(UpdateOp::Unset {
        path: path.into_update_path(),
    })
}

fn path_sort<D>(path: TypedPath, order: crate::query::SortOrder) -> Sort<D> {
    let field = Arc::new(Expr::Field(path.components));
    let field = match order {
        crate::query::SortOrder::Ascending => SortField::asc(field),
        crate::query::SortOrder::Descending => SortField::desc(field),
    };
    Sort::from_fields(vec![field])
}

fn path_index<D>(path: TypedPath, order: crate::query::SortOrder) -> Index<D> {
    let field = match order {
        crate::query::SortOrder::Ascending => IndexKeyField::asc(path.components),
        crate::query::SortOrder::Descending => IndexKeyField::desc(path.components),
    };
    Index::from_fields(vec![field])
}

fn comparison_filter<D>(
    path: TypedPath,
    operator: ComparisonOperator,
    value: impl Serialize,
) -> Filter<D> {
    let bson = serialize_to_bson(&value).expect("typed filter value must serialize into BSON");
    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::Comparison {
            operator,
            value: Arc::new(Expr::Literal(BsonValue(bson))),
        })],
    }))
}

fn value_update<D>(
    path: TypedPath,
    op: impl FnOnce(Vec<UpdatePathComponent>, Arc<Expr>) -> UpdateOp,
    value: impl Serialize,
) -> Update<D> {
    Update::from_op(op(path.into_update_path(), literal_expr(value)))
}

fn literal_expr(value: impl Serialize) -> Arc<Expr> {
    let bson = serialize_to_bson(&value).expect("typed update value must serialize into BSON");
    Arc::new(Expr::Literal(BsonValue(bson)))
}

fn current_date_update<D>(path: TypedPath, type_hint: CurrentDateType) -> Update<D> {
    Update::from_op(UpdateOp::CurrentDate {
        path: path.into_update_path(),
        type_hint,
    })
}

fn array_path_update<D>(
    path: TypedPath,
    op: impl FnOnce(Vec<UpdatePathComponent>) -> UpdateOp,
) -> Update<D> {
    Update::from_op(op(path.into_update_path()))
}

fn array_values_update<D, T: Serialize>(
    path: TypedPath,
    op: impl FnOnce(Vec<UpdatePathComponent>, Vec<Arc<Expr>>) -> UpdateOp,
    values: impl IntoIterator<Item = T>,
) -> Update<D> {
    Update::from_op(op(
        path.into_update_path(),
        values.into_iter().map(literal_expr).collect(),
    ))
}

fn array_any_eq_filter<D>(path: TypedPath, value: impl Serialize) -> Filter<D> {
    let bson = serialize_to_bson(&value).expect("typed filter value must serialize into BSON");
    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::ElemMatch(vec![Arc::new(
            Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Literal(BsonValue(bson))),
            },
        )]))],
    }))
}

fn array_any_filter<D>(path: TypedPath, predicate: Filter<D>) -> Filter<D> {
    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::ElemMatch(vec![predicate.into_expr()]))],
    }))
}

fn scalar_array_any_filter<D>(path: TypedPath, predicate: Filter<D>) -> Filter<D> {
    let predicate = scalar_array_element_expr(predicate.into_expr());

    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::ElemMatch(vec![predicate]))],
    }))
}

fn scalar_array_element_expr(expr: Arc<Expr>) -> Arc<Expr> {
    match expr.as_ref() {
        Expr::FieldFilters {
            field: field_expr,
            filters,
        } if matches!(field_expr.as_ref(), Expr::Field(field_path) if field_path.is_empty()) => {
            match filters.as_slice() {
                [filter] => filter.clone(),
                _ => Arc::new(Expr::And(filters.clone())),
            }
        }
        Expr::And(children) => Arc::new(Expr::And(
            children
                .iter()
                .cloned()
                .map(scalar_array_element_expr)
                .collect(),
        )),
        Expr::Or(children) => Arc::new(Expr::Or(
            children
                .iter()
                .cloned()
                .map(scalar_array_element_expr)
                .collect(),
        )),
        Expr::Nor(children) => Arc::new(Expr::Nor(
            children
                .iter()
                .cloned()
                .map(scalar_array_element_expr)
                .collect(),
        )),
        Expr::Not(child) => Arc::new(Expr::Not(scalar_array_element_expr(child.clone()))),
        _ => expr,
    }
}

fn array_all_filter<D>(path: TypedPath, values: impl Serialize) -> Filter<D> {
    let bson = serialize_to_bson(&values).expect("typed filter value must serialize into BSON");
    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::All(Arc::new(Expr::Literal(BsonValue(
            bson,
        )))))],
    }))
}

fn array_size_filter<D>(path: TypedPath, length: usize) -> Filter<D> {
    let length = i64::try_from(length).expect("typed array length must fit BSON i64");
    Filter::from_expr(Arc::new(Expr::FieldFilters {
        field: Arc::new(Expr::Field(path.components)),
        filters: vec![Arc::new(Expr::Size {
            size: Arc::new(Expr::Literal(BsonValue(bson::Bson::Int64(length)))),
            negated: false,
        })],
    }))
}

impl Projection {
    pub(crate) fn typed_include(paths: Vec<Vec<PathComponent>>) -> Arc<Self> {
        Self::typed(paths, true, true)
    }

    pub(crate) fn typed_include_without_id(paths: Vec<Vec<PathComponent>>) -> Arc<Self> {
        Self::typed(paths, true, false)
    }

    pub(crate) fn typed_exclude(paths: Vec<Vec<PathComponent>>) -> Arc<Self> {
        Self::typed(paths, false, false)
    }

    fn typed(paths: Vec<Vec<PathComponent>>, includes: bool, include_id: bool) -> Arc<Self> {
        let id_path = vec![PathComponent::FieldName("_id".to_string())];
        let includes_id = paths.iter().any(|path| path == &id_path);
        let mut fields = ProjectionExpr::Fields {
            children: BTreeMap::new(),
        };

        for path in paths {
            fields
                .add_expr(&path, 0, Arc::new(ProjectionExpr::Field))
                .expect("typed projection fields must have valid paths");
        }
        if includes && include_id && !includes_id {
            fields
                .add_expr(&id_path, 0, Arc::new(ProjectionExpr::Field))
                .expect("the _id projection path must be valid");
        }

        let fields = Arc::new(fields);
        Arc::new(if includes {
            Projection::Include(fields)
        } else {
            Projection::Exclude(fields)
        })
    }
}

/// A source-typed selection that determines a query result type.
pub trait TypedSelection<D>: 'static {
    type Output: DeserializeOwned + 'static;

    #[doc(hidden)]
    fn projection_paths(&self) -> Vec<Vec<PathComponent>>;

    #[doc(hidden)]
    fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
        self.projection_paths()
    }

    #[doc(hidden)]
    fn decode(&self, document: &Document) -> Result<Self::Output>;
}

impl<D: 'static, V: QuokkaScalar> TypedSelection<D> for Field<D, V> {
    type Output = V;

    fn projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![self.path.components.clone()]
    }

    fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![selection_projection_path(&self.path.components)]
    }

    fn decode(&self, document: &Document) -> Result<Self::Output> {
        let value = crate::query::get_path_value(document, &self.path.components)
            .map(|value| value.0.clone())
            .ok_or_else(|| {
                Error::DeserializationError(format!(
                    "Selected field '{}' is missing",
                    self.path
                        .components
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(".")
                ))
            })?;
        Ok(bson::deserialize_from_bson(value)?)
    }
}

impl<D: 'static, T: DeserializeOwned + 'static> TypedSelection<D> for ArrayField<D, T> {
    type Output = Vec<T>;

    fn projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![self.path.components.clone()]
    }

    fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![selection_projection_path(&self.path.components)]
    }

    fn decode(&self, document: &Document) -> Result<Self::Output> {
        decode_required_selection(document, &self.path)
    }
}

impl<D: 'static, V: DeserializeOwned + 'static> TypedSelection<D> for MapField<D, V> {
    type Output = BTreeMap<String, V>;

    fn projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![self.path.components.clone()]
    }

    fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![selection_projection_path(&self.path.components)]
    }

    fn decode(&self, document: &Document) -> Result<Self::Output> {
        decode_required_selection(document, &self.path)
    }
}

impl<D: 'static, V: QueryFieldType + DeserializeOwned + 'static> TypedSelection<D>
    for OptionalField<D, V>
{
    type Output = Option<V>;

    fn projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![self.path.components.clone()]
    }

    fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![selection_projection_path(&self.path.components)]
    }

    fn decode(&self, document: &Document) -> Result<Self::Output> {
        let value = crate::query::get_path_value(document, &self.path.components)
            .map(|value| value.0.clone())
            .unwrap_or(bson::Bson::Null);
        Ok(bson::deserialize_from_bson(value)?)
    }
}

impl<D: 'static, V: QuokkaType> TypedSelection<D> for ObjectField<D, V> {
    type Output = V;

    fn projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![self.path.components.clone()]
    }

    fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
        vec![selection_projection_path(&self.path.components)]
    }

    fn decode(&self, document: &Document) -> Result<Self::Output> {
        decode_required_selection(document, &self.path)
    }
}

fn decode_required_selection<T: DeserializeOwned>(
    document: &Document,
    path: &TypedPath,
) -> Result<T> {
    let value = crate::query::get_path_value(document, &path.components)
        .map(|value| value.0.clone())
        .ok_or_else(|| {
            Error::DeserializationError(format!(
                "Selected field '{}' is missing",
                path.components
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(".")
            ))
        })?;
    Ok(bson::deserialize_from_bson(value)?)
}

fn selection_projection_path(path: &[PathComponent]) -> Vec<PathComponent> {
    path.iter()
        .position(|component| matches!(component, PathComponent::ArrayElement(_)))
        .map_or_else(|| path.to_vec(), |index| path[..index].to_vec())
}

macro_rules! impl_tuple_selection {
    ($(($first_field:ident: $first_value:ident $(, $field:ident: $value:ident)*)),+ $(,)?) => {
        $(
            impl<D: 'static, $first_value: TypedSelection<D>, $($value: TypedSelection<D>),*>
                TypedSelection<D> for ($first_value, $($value,)*)
            {
                type Output = (
                    <$first_value as TypedSelection<D>>::Output,
                    $(<$value as TypedSelection<D>>::Output,)*
                );

                fn projection_paths(&self) -> Vec<Vec<PathComponent>> {
                    let ($first_field, $($field,)*) = self;
                    let mut paths = $first_field.projection_paths();
                    $(paths.extend($field.projection_paths());)*
                    paths
                }

                fn selection_projection_paths(&self) -> Vec<Vec<PathComponent>> {
                    let ($first_field, $($field,)*) = self;
                    let mut paths = $first_field.selection_projection_paths();
                    $(paths.extend($field.selection_projection_paths());)*
                    paths
                }

                fn decode(&self, document: &Document) -> Result<Self::Output> {
                    let ($first_field, $($field,)*) = self;
                    Ok(($first_field.decode(document)?, $($field.decode(document)?,)*))
                }
            }
        )+
    };
}

impl_tuple_selection!(
    (first: First, second: Second),
    (first: First, second: Second, third: Third),
    (first: First, second: Second, third: Third, fourth: Fourth),
    (first: First, second: Second, third: Third, fourth: Fourth, fifth: Fifth),
    (first: First, second: Second, third: Third, fourth: Fourth, fifth: Fifth, sixth: Sixth),
    (
        first: First,
        second: Second,
        third: Third,
        fourth: Fourth,
        fifth: Fifth,
        sixth: Sixth,
        seventh: Seventh
    ),
    (
        first: First,
        second: Second,
        third: Third,
        fourth: Fourth,
        fifth: Fifth,
        sixth: Sixth,
        seventh: Seventh,
        eighth: Eighth
    ),
);

#[derive(Debug, Clone, PartialEq)]
pub struct Filter<D> {
    expr: Arc<Expr>,
    _marker: PhantomData<fn() -> D>,
}

impl<D> Filter<D> {
    pub fn raw(raw: Document) -> Result<Self> {
        Ok(Self {
            expr: parser::parse_conditions(&raw)?,
            _marker: PhantomData,
        })
    }

    pub(crate) fn from_expr(expr: Arc<Expr>) -> Self {
        Self {
            expr,
            _marker: PhantomData,
        }
    }

    pub fn and(self, other: Self) -> Self {
        Self::from_expr(Arc::new(Expr::And(vec![self.expr, other.expr])))
    }

    pub fn or(self, other: Self) -> Self {
        Self::from_expr(Arc::new(Expr::Or(vec![self.expr, other.expr])))
    }

    /// Builds a filter that excludes documents matching either condition.
    pub fn nor(self, other: Self) -> Self {
        Self::from_expr(Arc::new(Expr::Nor(vec![self.expr, other.expr])))
    }

    pub(crate) fn into_expr(self) -> Arc<Expr> {
        self.expr
    }
}

/// Builds a filter that excludes documents matching `filter`.
pub fn not<D>(filter: Filter<D>) -> Filter<D> {
    Filter::from_expr(Arc::new(Expr::Not(filter.expr)))
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sort<D> {
    fields: Vec<SortField>,
    _marker: PhantomData<fn() -> D>,
}

impl<D> Sort<D> {
    pub fn raw(raw: Document) -> Result<Self> {
        Ok(Self::from_fields(parser::parse_sort(&raw)?))
    }

    pub(crate) fn from_fields(fields: Vec<SortField>) -> Self {
        Self {
            fields,
            _marker: PhantomData,
        }
    }

    pub fn then(mut self, other: Self) -> Self {
        self.fields.extend(other.fields);
        self
    }

    pub(crate) fn into_fields(self) -> Vec<SortField> {
        self.fields
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index<D> {
    fields: Vec<IndexKeyField>,
    _marker: PhantomData<fn() -> D>,
}

impl<D> Index<D> {
    fn from_fields(fields: Vec<IndexKeyField>) -> Self {
        Self {
            fields,
            _marker: PhantomData,
        }
    }

    pub fn then(mut self, other: Self) -> Self {
        self.fields.extend(other.fields);
        self
    }

    pub(crate) fn into_key_spec(self) -> Result<IndexKeySpec> {
        let spec = IndexKeySpec::new(self.fields);
        spec.validate()?;
        Ok(spec)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update<D> {
    expr: UpdateExpr,
    _marker: PhantomData<fn() -> D>,
}

impl<D> Update<D> {
    fn from_op(op: UpdateOp) -> Self {
        Self {
            expr: UpdateExpr {
                ops: vec![op],
                array_filters: Default::default(),
            },
            _marker: PhantomData,
        }
    }

    pub(crate) fn into_update_expr(self) -> UpdateExpr {
        self.expr
    }

    /// Combines this update with another update.
    pub fn and(mut self, other: Self) -> Self {
        self.expr.ops.extend(other.expr.ops);
        self.expr.array_filters.extend(other.expr.array_filters);
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::expr_fn::{
        and, elem_match, eq, exists, field, field_filters, gt, lit, lt, nin, nor, not as expr_not,
        or, within,
    };
    use crate::query::update_fn::{field_name, inc, set, unset};
    use bson::{Binary, DateTime, Decimal128, doc, oid::ObjectId};
    use quokkadb::query::update_fn;
    use quokkadb_derive::{QuokkaDocument, QuokkaType};
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, QuokkaDocument)]
    struct User {
        #[quokka(id)]
        #[serde(rename = "_id")]
        id: u64,
        name: String,
        age: i32,
        active: bool,
        email: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, QuokkaType)]
    struct Address {
        city: String,
        postal_code: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, QuokkaDocument)]
    struct OptionalUser {
        #[quokka(id)]
        #[serde(rename = "_id")]
        id: u64,
        address: Option<Address>,
        email: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, QuokkaDocument)]
    struct CompoundUser {
        #[quokka(id)]
        #[serde(rename = "_id")]
        id: u64,
        address: Address,
        tags: Vec<String>,
        metadata: BTreeMap<String, String>,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct CustomScalar(String);

    impl QuokkaScalar for CustomScalar {}

    fn build_filter<D: QuokkaDocument>(f: impl FnOnce(&D::Fields<D>) -> Filter<D>) -> Filter<D> {
        f(&D::root_fields())
    }

    #[test]
    fn bson_leaf_types_expose_typed_field_capabilities() {
        fn assert_scalar<T: QuokkaScalar + QueryFieldType>() {}
        fn assert_numeric<T: NumericValue>() {}
        fn assert_quokka_type<T: QuokkaType>() {}

        assert_scalar::<bool>();
        assert_scalar::<String>();
        assert_scalar::<i32>();
        assert_scalar::<i64>();
        assert_scalar::<f32>();
        assert_scalar::<f64>();
        assert_scalar::<DateTime>();
        assert_scalar::<Decimal128>();
        assert_scalar::<Binary>();
        assert_scalar::<ObjectId>();
        assert_scalar::<CustomScalar>();
        assert_numeric::<i32>();
        assert_numeric::<Decimal128>();
        assert_quokka_type::<Address>();
    }

    #[test]
    fn typed_filter_closure_builds_expected_expr_filters() {
        assert_eq!(
            build_filter::<User>(|u| u.age.gt(18)).into_expr(),
            field_filters(field(["age"]), [gt(lit(18))]),
        );

        assert_eq!(
            build_filter::<User>(|u| u.age.gt(18).and(u.active.eq(true))).into_expr(),
            and([
                field_filters(field(["age"]), [gt(lit(18))]),
                field_filters(field(["active"]), [eq(lit(true))]),
            ])
        );

        assert_eq!(
            build_filter::<User>(|u| u.age.lt(18).or(u.age.gt(65))).into_expr(),
            or([
                field_filters(field(["age"]), [lt(lit(18))]),
                field_filters(field(["age"]), [gt(lit(65))]),
            ])
        );

        assert_eq!(
            build_filter::<User>(|u| not(u.age.lt(18))).into_expr(),
            expr_not(field_filters(field(["age"]), [lt(lit(18))]))
        );

        assert_eq!(
            build_filter::<User>(|u| u.age.lt(18).nor(u.age.gt(65))).into_expr(),
            nor([
                field_filters(field(["age"]), [lt(lit(18))]),
                field_filters(field(["age"]), [gt(lit(65))]),
            ])
        );

        assert_eq!(
            build_filter::<User>(|u| u.age.in_values([18, 21, 65])).into_expr(),
            field_filters(field(["age"]), [within(lit(vec![18, 21, 65]))]),
        );

        assert_eq!(
            build_filter::<CompoundUser>(|u| {
                u.tags
                    .any_where(|tag| tag.eq("database").or(tag.eq("storage")))
            })
            .into_expr(),
            field_filters(
                field(["tags"]),
                [elem_match([or([eq(lit("database")), eq(lit("storage"))])])],
            )
        );

        assert_eq!(
            build_filter::<User>(|u| u.age.nin([18, 21, 65])).into_expr(),
            field_filters(field(["age"]), [nin(lit(vec![18, 21, 65]))]),
        );

        assert_eq!(
            build_filter::<User>(|u| u.email.exists()).into_expr(),
            field_filters(field(["email"]), [exists(true)]),
        );
    }

    #[test]
    fn optional_fields_preserve_nullability_and_nested_traversal() {
        assert_eq!(
            build_filter::<OptionalUser>(|user| user.email.exists()).into_expr(),
            field_filters(field(["email"]), [exists(true)]),
        );
        assert_eq!(
            build_filter::<OptionalUser>(|user| user.address.city.eq("Zurich")).into_expr(),
            field_filters(field(["address", "city"]), [eq(lit("Zurich"))]),
        );

        let email =
            <Option<String> as QueryFieldType>::field::<OptionalUser>(TypedPath::root("email"));
        assert_eq!(email.decode(&doc! {}).unwrap(), None);
        assert_eq!(
            email
                .decode(&doc! { "email": "alice@example.com" })
                .unwrap(),
            Some("alice@example.com".to_string()),
        );
    }

    #[test]
    fn derived_quokka_document_exposes_id_type_and_accessor() {
        let user = User {
            id: 7,
            name: "Alice".to_string(),
            age: 30,
            active: true,
            email: None,
        };

        let id: &u64 = user.id();
        assert_eq!(id, &7);
    }

    #[test]
    fn path_field_composes_nested_paths_for_query_builders() {
        let path = TypedPath::root("addresses").array_element(0).field("city");
        let city = Field::<User, String>::new(path.clone());

        assert_eq!(
            path.as_slice(),
            [
                PathComponent::FieldName("addresses".to_string()),
                PathComponent::ArrayElement(0),
                PathComponent::FieldName("city".to_string()),
            ]
        );
        assert_eq!(
            city.eq("Zurich").into_expr(),
            field_filters(
                field([
                    PathComponent::FieldName("addresses".to_string()),
                    PathComponent::ArrayElement(0),
                    PathComponent::FieldName("city".to_string()),
                ]),
                [eq(lit("Zurich"))],
            ),
        );
        assert_eq!(
            city.set("Bern").into_update_expr(),
            update_fn::update([set(
                [
                    field_name("addresses"),
                    UpdatePathComponent::ArrayElement(0),
                    field_name("city"),
                ],
                "Bern",
            )]),
        );
        assert_eq!(
            city.asc().into_fields(),
            vec![crate::query::make_sort_field(
                path.components.clone(),
                crate::query::SortOrder::Ascending,
            )],
        );
        let indexed_city = Field::<User, String>::new(TypedPath::root("address").field("city"));
        assert_eq!(
            indexed_city.projection_paths(),
            vec![indexed_city.path.components.clone()]
        );
        assert!(matches!(
            Projection::typed_include(indexed_city.projection_paths()).as_ref(),
            Projection::Include(_)
        ));
        assert!(matches!(
            Projection::typed_exclude(indexed_city.projection_paths()).as_ref(),
            Projection::Exclude(_)
        ));
        assert_eq!(
            indexed_city.index_asc().into_key_spec().unwrap().fields,
            vec![IndexKeyField::asc(indexed_city.path.components)],
        );
    }

    #[test]
    fn quokka_document_update_closure_builds_expected_update_expr() {
        let update = User::update(|u| u.name.set("Bob").and(u.age.inc(1)).and(u.email.unset()))
            .into_update_expr();

        assert_eq!(
            update,
            update_fn::update([
                set([field_name("name")], "Bob"),
                inc([field_name("age")], 1),
                unset([field_name("email")]),
            ])
        );
        assert!(update.array_filters.is_empty());
    }

    #[test]
    fn compound_fields_expose_whole_value_comparisons_and_min_max_updates() {
        let address = Address {
            city: "Zurich".to_string(),
            postal_code: "8001".to_string(),
        };
        let tags = vec!["rust".to_string(), "database".to_string()];
        let metadata = BTreeMap::from([("tier".to_string(), "gold".to_string())]);

        assert_eq!(
            build_filter::<CompoundUser>(|user| user.address.gt(address.clone())).into_expr(),
            field_filters(
                field(["address"]),
                [gt(lit(doc! { "city": "Zurich", "postal_code": "8001" }))],
            ),
        );
        assert_eq!(
            build_filter::<CompoundUser>(|user| user.tags.lte(tags.clone())).into_expr(),
            field_filters(
                field(["tags"]),
                [crate::query::expr_fn::lte(lit(tags.clone()))]
            ),
        );
        assert_eq!(
            build_filter::<CompoundUser>(|user| user.metadata.ne(metadata.clone())).into_expr(),
            field_filters(
                field(["metadata"]),
                [crate::query::expr_fn::ne(lit(doc! { "tier": "gold" }))],
            ),
        );

        let update = CompoundUser::update(|user| {
            user.address
                .min(address)
                .and(user.tags.max(tags))
                .and(user.metadata.min(metadata))
        })
        .into_update_expr();
        assert_eq!(
            update,
            update_fn::update([
                update_fn::min(
                    [field_name("address")],
                    doc! { "city": "Zurich", "postal_code": "8001" },
                ),
                update_fn::max([field_name("tags")], vec!["rust", "database"],),
                update_fn::min([field_name("metadata")], doc! { "tier": "gold" }),
            ]),
        );

        build_filter::<CompoundUser>(|user| {
            user.address.ne(Address {
                city: "Bern".to_string(),
                postal_code: "3000".to_string(),
            })
        });
        build_filter::<CompoundUser>(|user| {
            user.address.gte(Address {
                city: "Bern".to_string(),
                postal_code: "3000".to_string(),
            })
        });
        build_filter::<CompoundUser>(|user| {
            user.address.lt(Address {
                city: "Bern".to_string(),
                postal_code: "3000".to_string(),
            })
        });
        build_filter::<CompoundUser>(|user| {
            user.address.lte(Address {
                city: "Bern".to_string(),
                postal_code: "3000".to_string(),
            })
        });
        CompoundUser::update(|user| {
            user.address.max(Address {
                city: "Bern".to_string(),
                postal_code: "3000".to_string(),
            })
        });
        build_filter::<CompoundUser>(|user| user.tags.eq(vec!["rust".to_string()]));
        build_filter::<CompoundUser>(|user| user.tags.ne(vec!["rust".to_string()]));
        build_filter::<CompoundUser>(|user| user.tags.gt(vec!["rust".to_string()]));
        build_filter::<CompoundUser>(|user| user.tags.gte(vec!["rust".to_string()]));
        build_filter::<CompoundUser>(|user| user.tags.lt(vec!["rust".to_string()]));
        CompoundUser::update(|user| user.tags.min(vec!["rust".to_string()]));
        build_filter::<CompoundUser>(|user| user.metadata.eq(BTreeMap::new()));
        build_filter::<CompoundUser>(|user| user.metadata.gt(BTreeMap::new()));
        build_filter::<CompoundUser>(|user| user.metadata.gte(BTreeMap::new()));
        build_filter::<CompoundUser>(|user| user.metadata.lt(BTreeMap::new()));
        build_filter::<CompoundUser>(|user| user.metadata.lte(BTreeMap::new()));
        CompoundUser::update(|user| user.metadata.max(BTreeMap::new()));
    }

    #[test]
    fn typed_update_uses_stored_field_names() {
        let update = User::update(|u| u.id.set(7_u64)).into_update_expr();

        assert_eq!(update, update_fn::update([set([field_name("_id")], 7_i64)]));
    }
}
