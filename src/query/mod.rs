use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind, Result};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use bson::{Bson, Document};
use crate::error;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::query::tree_node::TreeNode;
use crate::util::bson_utils;
use crate::util::bson_utils::BsonKey;
use crate::util::interval::Interval;

pub(crate) mod execution;
pub (crate) mod optimizer;
pub(crate) mod parser;
pub(crate) mod logical_plan;
pub(crate) mod physical_plan;
mod tree_node;

#[cfg(test)]
pub(crate) mod expr_fn;

#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub enum Expr {
    /// Field reference
    Field(Vec<PathComponent>),
    ///  Positional projection in queries (e.g. "array.$")
    PositionalField(Vec<PathComponent>),
    /// Literal values
    Literal(BsonValue),
    /// Placeholder for parameters. After normalization, literals would be replaced by placeholders
    /// and the parameters captured. Allowing optimizations output to be cached and reused.
    Placeholder(u32),
    /// Multiple filters on the same field (e.g., `{ "price": { "$ne": 1.99, "$exists": true } }`)
    FieldFilters {
        field: Arc<Expr>,
        filters: Vec<Arc<Expr>>, // List of filters on the same field
    },
    /// A single comparison (e.g., `$gt: 5`, `$eq: "Alice"`)
    Comparison {
        operator: ComparisonOperator, // `$eq`, `$ne`, `$gt`, `$lt`, etc.
        value: Arc<Expr>,              // The literal value
    },
    /// Represents an interval for range queries
    Interval(Interval<Arc<Expr>>),
    And(Vec<Arc<Expr>>),
    Or(Vec<Arc<Expr>>),
    Not(Arc<Expr>),
    Nor(Vec<Arc<Expr>>),
    /// Field existence
    Exists(bool),
    /// Type check
    Type {
        bson_type: Arc<Expr>,
        negated: bool, // If the expression has been negated (e.g. $not: { $type : "string" })
    },
    // Array-specific operations
    Size {
        size: Arc<Expr>,
        negated: bool,
    },
    All(Arc<Expr>),
    ElemMatch(Vec<Arc<Expr>>),
    /// Represents an expression that is always true (e.g. $and: [])
    AlwaysTrue,
    /// Represents an expression that is always false (e.g. $or: [])
    AlwaysFalse,
}

impl TreeNode for Expr {
    type Child = Expr;

    /// Return references to the children of the current node
    fn children(&self) -> Vec<Arc<Self::Child>> {
        match self {
            Expr::FieldFilters {
                field,
                filters: predicates,
                ..
            } => {
                let mut children = Vec::with_capacity(predicates.len() + 1);
                children.push(field.clone());
                children.extend(predicates.into_iter().cloned());
                children
            }
            Expr::Comparison { value, .. } => vec![value.clone()],
            Expr::And(elements) => elements.iter().cloned().collect(),
            Expr::Or(elements) => elements.iter().cloned().collect(),
            Expr::Not(expr) => vec![expr.clone()],
            Expr::Nor(elements) => elements.iter().cloned().collect(),
            Expr::ElemMatch(predicates) => predicates.iter().cloned().collect(),
            Expr::Interval(interval) => {
                let mut children = Vec::new();
                if let Some(child) = interval.start_bound_value() {
                    children.push(child);
                }
                if let Some(child) = interval.end_bound_value() {
                    children.push(child);
                }
                children
            }
            Expr::Type { bson_type, .. } => vec![bson_type.clone()],
            Expr::Size { size, .. } => vec![size.clone()],
            Expr::All(values) => vec![values.clone()],
            _ => vec![], // Leaf nodes have no children
        }
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self::Child>>) -> Arc<Self> {
        match self.as_ref() {
            Expr::FieldFilters { .. } => {
                let mut iter = children.into_iter();
                let field = iter.next().unwrap();
                let predicates = iter.collect();
                Arc::new(Expr::FieldFilters {
                    field,
                    filters: predicates,
                })
            }
            Expr::Comparison { operator, .. } => Arc::new(Expr::Comparison {
                operator: operator.clone(),
                value: Self::get_first(children),
            }),
            Expr::And(_) => Arc::new(Expr::And(children)),
            Expr::Or(_) => Arc::new(Expr::Or(children)),
            Expr::Not(_) => Arc::new(Expr::Not(Self::get_first(children))),
            Expr::Nor(_) => Arc::new(Expr::Nor(children)),
            Expr::ElemMatch { .. } => Arc::new(Expr::ElemMatch(children)),
            Expr::Interval(interval) => {

                let mut iter = children.into_iter();

                let start_bound = match interval.start_bound() {
                    Bound::Included(..) => Bound::Included(iter.next().unwrap()),
                    Bound::Excluded(..) => Bound::Excluded(iter.next().unwrap()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let end_bound = match interval.end_bound() {
                    Bound::Included(..) => Bound::Included(iter.next().unwrap()),
                    Bound::Excluded(..) => Bound::Excluded(iter.next().unwrap()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                Arc::new(Expr::Interval(Interval::new(start_bound, end_bound)))
            },
            Expr::Type { negated, .. } => Arc::new(Expr::Type {
                bson_type: Self::get_first(children),
                negated: *negated,
            }),
            Expr::Size { negated, .. } => Arc::new(Expr::Size {
                size: Self::get_first(children),
                negated: *negated,
            }),
            Expr::All(..) => {
                assert_eq!(children.len(), 1, "All operator should have exactly one child");
                Arc::new(Expr::All(Self::get_first(children)))
            }
            _ => self, // No changes needed for leaf nodes
        }
    }
}

impl Expr {
    fn get_first(children: Vec<Arc<Expr>>) -> Arc<Expr> {
        children.into_iter().next().unwrap()
    }

    pub fn negate(&self) -> Arc<Expr> {
        match self {
            Expr::Not(expr) => expr.clone(),
            Expr::Nor(exprs) => Arc::new(Expr::Or(exprs.iter().cloned().collect())),
            Expr::And(exprs) => Arc::new(Expr::Or(exprs.iter().map(|e| e.negate()).collect())),
            Expr::Or(exprs) => Arc::new(Expr::And(exprs.iter().map(|e| e.negate()).collect())),
            Expr::FieldFilters {
                field,
                filters: predicates,
            } => {
                if predicates.len() == 1 {
                    return Arc::new(Expr::FieldFilters {
                        field: field.clone(),
                        filters: predicates.iter().map(|e| e.negate()).collect(),
                    });
                }

                // If there are multiple expression we need to apply De Morgan's Laws
                // not(x and y) simplifies to not(x) or not(y)
                let field_predicates = predicates
                    .iter()
                    .map(|e| {
                        Arc::new(Expr::FieldFilters {
                            field: field.clone(),
                            filters: vec![e.negate()],
                        })
                    })
                    .collect();

                Arc::new(Expr::FieldFilters {
                    field: field.clone(),
                    filters: vec![Arc::new(Expr::Or(field_predicates))],
                })
            }
            Expr::Comparison { operator, value } => Arc::new(Expr::Comparison {
                operator: operator.negate(),
                value: value.clone(),
            }),
            Expr::Exists(bool) => Arc::new(Expr::Exists(!*bool)),
            Expr::All(values) => {
                Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Nin,
                    value: values.clone(),
                })
            }
            Expr::Type {
                bson_type,
                negated: not,
            } => Arc::new(Expr::Type {
                bson_type: bson_type.clone(),
                negated: !*not,
            }),
            Expr::Size { size, negated } => Arc::new(Expr::Size {
                size: size.clone(),
                negated: !*negated,
            }),
            Expr::AlwaysTrue => Arc::new(Expr::AlwaysFalse),
            Expr::AlwaysFalse => Arc::new(Expr::AlwaysTrue),
            Expr::Literal(bson) => {
                if let BsonValue(Bson::Boolean(b)) = bson {
                    if *b {
                        Arc::new(Expr::Literal(false.into()))
                    } else {
                        Arc::new(Expr::Literal(true.into()))
                    }
                } else {
                    Arc::new(Expr::Not(Arc::new(self.clone())))
                }
            }
            _ => Arc::new(Expr::Not(Arc::new(self.clone())))
        }
    }
}

impl Serializable for Expr {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            1 => {
                let expr = Arc::new(Self::read_from(reader)?);
                Ok(Expr::Not(expr))
            }
            2 => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::Nor(exprs))
            }
            3 => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::And(exprs))
            }
            4 => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::Or(exprs))
            }
            5 => {
                let field = Arc::new(Self::read_from(reader)?);
                let filters = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::FieldFilters { field, filters })
            }
            6 => {
                let operator = ComparisonOperator::read_from(reader)?;
                let value = Arc::new(Self::read_from(reader)?);
                Ok(Expr::Comparison { operator, value })
            }
            7 => {
                let exists = reader.read_u8()? == 1;
                Ok(Expr::Exists(exists))
            }
            8 => {
                let values = Arc::new(Self::read_from(reader)?);
                Ok(Expr::All(values))
            }
            9 => {
                let bson_type = Arc::new(Self::read_from(reader)?);
                let negated = reader.read_u8()? == 1;
                Ok(Expr::Type {
                    bson_type,
                    negated,
                })
            }
            10 => {
                let size = Arc::new(Self::read_from(reader)?);
                let negated = reader.read_u8()? == 1;
                Ok(Expr::Size { size, negated })
            }
            11 => Ok(Expr::AlwaysTrue),
            12 => Ok(Expr::AlwaysFalse),
            13 => {
                let path = Vec::<PathComponent>::read_from(reader)?;
                Ok(Expr::Field(path))
            }
            14 => {
                let path = Vec::<PathComponent>::read_from(reader)?;
                Ok(Expr::PositionalField(path))
            }
            15 => {
                let idx = reader.read_varint_u32()?;
                Ok(Expr::Placeholder(idx))
            }
            16 => {
                let predicates = Vec::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::ElemMatch(predicates))
            }
            17 => {
                let interval = Interval::<Arc<Expr>>::read_from(reader)?;
                Ok(Expr::Interval(interval))
            }
            _ => panic!("Unknown Expr tag: {}", tag),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            Expr::Not(expr) => {
                writer.write_u8(1);
                expr.write_to(writer);
            },
            Expr::Nor(exprs) => {
                writer.write_u8(2);
                exprs.write_to(writer);
            },
            Expr::And(exprs) => {
                writer.write_u8(3);
                exprs.write_to(writer);
            },
            Expr::Or(exprs) => {
                writer.write_u8(4);
                exprs.write_to(writer);
            },
            Expr::FieldFilters {
                field,
                filters,
            } => {
                writer.write_u8(5);
                field.write_to(writer);
                filters.write_to(writer);
            }
            Expr::Comparison { operator, value } => {
                writer.write_u8(6);
                operator.write_to(writer);
                value.write_to(writer);
            },
            Expr::Exists(bool) => {
                writer.write_u8(7);
                writer.write_u8(if *bool { 1 } else { 0 });
            },
            Expr::All(values) => {
                writer.write_u8(8);
                values.write_to(writer);
            }
            Expr::Type {
                bson_type,
                negated,
            } => {
                writer.write_u8(9);
                bson_type.write_to(writer);
                writer.write_u8(if *negated { 1 } else { 0 });
            },
            Expr::Size { size, negated } => {
                writer.write_u8(10);
                size.write_to(writer);
                writer.write_u8(if *negated { 1 } else { 0 });
            },
            Expr::AlwaysTrue => {
                writer.write_u8(11);
            },
            Expr::AlwaysFalse => {
                writer.write_u8(12);
            },
            Expr::Field(path) => {
                writer.write_u8(13);
                path.write_to(writer);
            }
            Expr::PositionalField(path) => {
                writer.write_u8(14);
                path.write_to(writer);
            }
            Expr::Placeholder(idx) => {
                writer.write_u8(15);
                writer.write_varint_u32(*idx); // Write the placeholder index for extra safety
            }
            Expr::ElemMatch(predicates) => {
                writer.write_u8(16);
                predicates.write_to(writer);
            }
            Expr::Literal(_) => {
                panic!("LogicalPlans should never be serialized before parametrization.");
            }
            Expr::Interval(interval) => {
                writer.write_u8(17);
                interval.write_to(writer);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionExpr {
    Fields { children: BTreeMap<PathComponent, Arc<ProjectionExpr>> },
    ArrayElements { children: BTreeMap<PathComponent, Arc<ProjectionExpr>> },
    Field,
    /// Positional field reference (e.g. "array.$")
    /// This is used in queries to refer to the first matching element in an array.
    PositionalFieldRef,
    /// Positional field reference associated to the query filter to which it is corresponding
    PositionalField { filter: Arc<Expr> },
    Slice { skip: Option<i32>, limit: i32 },
    ElemMatch { filter: Arc<Expr>, },
}

impl ProjectionExpr {

    fn get_children_for_component_mut<'a, 'b: 'a>(&'a mut self, path: &'b [PathComponent], component: usize) -> error::Result<&'a mut BTreeMap<PathComponent, Arc<ProjectionExpr>>> {
        match self {
            ProjectionExpr::Fields { children } => {
                if component < path.len() && matches!(&path[component], &PathComponent::ArrayElement(_)) {
                    return Err(error::Error::InvalidRequest(format!("Cannot use the array index {} in path: {}. Expecting a field name.", &path[component], format_path(path))));
                }
                Ok(children)
            }
            ProjectionExpr::ArrayElements { children } => {
                if component < path.len() && matches!(&path[component], &PathComponent::FieldName(_)) {
                    return Err(error::Error::InvalidRequest(format!("Cannot use the field name {} in path: {}. Expecting an array element.", &path[component], format_path(path))));
                }
                Ok(children)
            }
            _ => Err(error::Error::InvalidRequest(format!("Invalid projection specification for path {}", format_path(path)))),
        }
    }

    pub fn add_expr(&mut self, path: &[PathComponent], component: usize, expr: Arc<ProjectionExpr>) -> error::Result<()> {
        let children = self.get_children_for_component_mut(path, component)?;
        Self::add_to_children(path, component, expr, children)
    }

    pub fn remove_expr(&mut self, path: &[PathComponent], component: usize) -> error::Result<()> {
        let children = self.get_children_for_component_mut(path, component)?;
        Self::remove_from_children(path, component, children)
    }

    fn add_to_children(
        path: &[PathComponent],
        component: usize,
        expr: Arc<ProjectionExpr>,
        children: &mut BTreeMap<PathComponent, Arc<ProjectionExpr>>
    ) -> error::Result<()> {

        let current_component = &path[component];
        let existing = children.get_mut(&current_component);
        match existing {
            None => {
                if path.len() == component + 1 {
                    children.insert(current_component.clone(), expr);
                } else {
                    let mut sub_node = if matches!(path[1], PathComponent::FieldName(_)) {
                        ProjectionExpr::Fields { children: BTreeMap::new() }
                    } else {
                        ProjectionExpr::ArrayElements { children: BTreeMap::new() }
                    };
                    sub_node.add_expr(path, component + 1, expr)?;
                    children.insert(current_component.clone(), Arc::new(sub_node));
                }
            }
            Some(existing) => {
                match existing.as_ref() {
                    ProjectionExpr::Fields { children: _ } | ProjectionExpr::ArrayElements { children: _ } => {
                        if path.len() == component + 1 {
                            return Err(error::Error::InvalidRequest(format!("Invalid projection specification for path {}", format_path(path))))
                        } else if let Some(existing) = Arc::get_mut(existing) {
                            existing.add_expr(path, component + 1, expr)?;
                        } else {
                            panic!("Arc strong count is not 1 but should be ")
                        }
                    },
                    _ => return Err(error::Error::InvalidRequest(format!("Invalid projection specification for path {}", format_path(path))))
                }
            }
        }
        Ok(())
    }

    fn remove_from_children(path: &[PathComponent], component: usize, children: &mut BTreeMap<PathComponent, Arc<ProjectionExpr>>) -> error::Result<()> {
        let current_component = &path[component];

        if path.len() == component + 1 {
            children.remove(current_component);
            return Ok(());
        }

        if let Some(child_arc) = children.get_mut(current_component) {
            if let Some(child) = Arc::get_mut(child_arc) {
                child.remove_expr(path, component + 1)?;
                if child.is_empty() {
                    children.remove(current_component);
                }
            } else {
                panic!("Arc strong count is not 1 but should be ")
            }
        }
        Ok(())
    }

    pub fn children(&self) -> &BTreeMap<PathComponent, Arc<ProjectionExpr>> {
        match self {
            ProjectionExpr::Fields { children } => children,
            ProjectionExpr::ArrayElements { children } => children,
            _ => {
                static EMPTY: BTreeMap<PathComponent, Arc<ProjectionExpr>> = BTreeMap::new();
                &EMPTY
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ProjectionExpr::Fields { children } => children.is_empty(),
            ProjectionExpr::ArrayElements { children } => children.is_empty(),
            _ => true,
        }
    }
}

impl TreeNode for ProjectionExpr {
    type Child = ProjectionExpr;

    fn children(&self) -> Vec<Arc<Self>> {
        match self {
            ProjectionExpr::Fields { children } |
            ProjectionExpr::ArrayElements { children } => {
                children.values().cloned().collect()
            }
            _ => vec![],
        }
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self>>) -> Arc<Self> {
        match self.as_ref() {
            ProjectionExpr::Fields { children: old } => {
                if old.is_empty() { return self; }
                assert_eq!(old.len(), children.len(), "child count mismatch");
                let new_map = old.keys().cloned().zip(children.into_iter()).collect();
                Arc::new(ProjectionExpr::Fields { children: new_map })
            }
            ProjectionExpr::ArrayElements { children: old } => {
                if old.is_empty() { return self; }
                assert_eq!(old.len(), children.len(), "child count mismatch");
                let new_map = old.keys().cloned().zip(children.into_iter()).collect();
                Arc::new(ProjectionExpr::ArrayElements { children: new_map })
            }
            _ => self, // leaves
        }
    }
}

impl Serializable for ProjectionExpr {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(ProjectionExpr::Fields { children: BTreeMap::<PathComponent, Arc<ProjectionExpr>>::read_from(reader)? }),
            1 => Ok(ProjectionExpr::ArrayElements { children: BTreeMap::<PathComponent, Arc<ProjectionExpr>>::read_from(reader)? }),
            2 => Ok(ProjectionExpr::Field),
            3 => Ok(ProjectionExpr::PositionalFieldRef),
            4 => Ok(ProjectionExpr::PositionalField { filter: Arc::<Expr>::read_from(reader)? }),
            5 => Ok(ProjectionExpr::Slice { skip: Option::<i32>::read_from(reader)?, limit: reader.read_varint_i32()? }),
            6 => Ok(ProjectionExpr::ElemMatch { filter: Arc::<Expr>::read_from(reader)? }),
            _ => panic!("Invalid tag for ProjectionExpr: {}", tag),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            ProjectionExpr::Fields { children } => {
                writer.write_u8(0);
                children.write_to(writer);
            },
            ProjectionExpr::ArrayElements { children } => {
                writer.write_u8(1);
                children.write_to(writer);
            },
            ProjectionExpr::Field => {
                writer.write_u8(2);
            },
            ProjectionExpr::PositionalFieldRef => {
                writer.write_u8(3);
            },
            ProjectionExpr::PositionalField { filter } => {
                writer.write_u8(4);
                filter.write_to(writer);
            },
            ProjectionExpr::Slice { skip, limit } => {
                writer.write_u8(5);
                skip.write_to(writer);
                writer.write_varint_i32(*limit);
            },
            ProjectionExpr::ElemMatch { filter } => {
                writer.write_u8(6);
                filter.write_to(writer);
            },
        }
    }
}

/// Projection for included or excluded fields
/// This is used to specify which fields to include or exclude in the result set of a query.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Include(Arc<ProjectionExpr>), // Fields to include
    Exclude(Arc<ProjectionExpr>), // Fields to exclude
}

impl Serializable for Projection {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(Projection::Include(Arc::<ProjectionExpr>::read_from(reader)?)),
            1 => Ok(Projection::Exclude(Arc::<ProjectionExpr>::read_from(reader)?)),
            _ => panic!("Invalid tag for Projection: {}", tag),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            Projection::Include(exprs) => {
                writer.write_u8(0);
                exprs.write_to(writer);
            }
            Projection::Exclude(exprs) => {
                writer.write_u8(1);
                exprs.write_to(writer);
            }
        }
    }
}

/// Represents the sort order for a field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SortOrder {
    Ascending,
    Descending,
}

impl Serializable for SortOrder {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(SortOrder::Ascending),
            1 => Ok(SortOrder::Descending),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid tag for SortOrder",
            )),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            SortOrder::Ascending => 0,
            SortOrder::Descending => 1,
        };
        writer.write_u8(byte);
    }
}

/// Represents a sorting instruction for a query.
#[derive(Debug, Clone, PartialEq, Hash, Eq)]
pub struct SortField {
    pub field: Arc<Expr>,
    pub order: SortOrder,
}

impl SortField {

    pub fn asc(field: Arc<Expr>) -> SortField {
        SortField { field, order: SortOrder::Ascending }
    }

    pub fn desc(field: Arc<Expr>) -> SortField {
        SortField { field, order: SortOrder::Descending }
    }
}

impl Serializable for SortField {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let field = Arc::<Expr>::read_from(reader)?;
        let order = SortOrder::read_from(reader)?;
        Ok(SortField { field, order })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.field.write_to(writer);
        self.order.write_to(writer);
    }
}

#[cfg(test)]
pub fn make_sort_field(path: Vec<PathComponent>, order: SortOrder) -> SortField {
    SortField {
        field: Arc::new(Expr::Field(path)),
        order,
    }
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub enum ComparisonOperator {
    Eq,  // `$eq`
    Ne,  // `$ne`
    Gt,  // `$gt`
    Gte, // `$gte`
    Lt,  // `$lt`
    Lte, // `$lte`
    In,  // `$in`
    Nin, // `$nin`
}

impl ComparisonOperator {
    fn negate(&self) -> ComparisonOperator {
        match self {
            ComparisonOperator::Eq => ComparisonOperator::Ne,
            ComparisonOperator::Ne => ComparisonOperator::Eq,
            ComparisonOperator::Gt => ComparisonOperator::Lte,
            ComparisonOperator::Gte => ComparisonOperator::Lt,
            ComparisonOperator::Lt => ComparisonOperator::Gte,
            ComparisonOperator::Lte => ComparisonOperator::Gt,
            ComparisonOperator::In => ComparisonOperator::Nin,
            ComparisonOperator::Nin => ComparisonOperator::In,
        }
    }
}

impl Serializable for ComparisonOperator {

    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            0 => Ok(ComparisonOperator::Eq),
            1 => Ok(ComparisonOperator::Ne),
            2 => Ok(ComparisonOperator::Gt),
            3 => Ok(ComparisonOperator::Gte),
            4 => Ok(ComparisonOperator::Lt),
            5 => Ok(ComparisonOperator::Lte),
            6 => Ok(ComparisonOperator::In),
            7 => Ok(ComparisonOperator::Nin),
            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown comparison operator byte",)),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        let byte = match self {
            ComparisonOperator::Eq => 0,
            ComparisonOperator::Ne => 1,
            ComparisonOperator::Gt => 2,
            ComparisonOperator::Gte => 3,
            ComparisonOperator::Lt => 4,
            ComparisonOperator::Lte => 5,
            ComparisonOperator::In => 6,
            ComparisonOperator::Nin => 7,
        };
        writer.write_u8(byte);
    }
}

/// Represents a component in a field path
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub enum PathComponent {
    FieldName(String),   // A named field (e.g., "field" in "document.field")
    ArrayElement(usize), // An array index (e.g., "0" in "array.0")
}

impl PartialOrd for PathComponent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PathComponent {
    fn cmp(&self, other: &Self) -> Ordering {
        use PathComponent::*;
        match (self, other) {
            (FieldName(a), FieldName(b)) => a.cmp(b),
            (ArrayElement(a), ArrayElement(b)) => a.cmp(b),
            (FieldName(_), ArrayElement(_)) => Ordering::Less,
            (ArrayElement(_), FieldName(_)) => Ordering::Greater,
        }
    }
}

impl Serializable for PathComponent {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let byte = reader.read_u8()?;
        match byte {
            0 => {
                let name = reader.read_str()?;
                Ok(PathComponent::FieldName(name.to_string()))
            }
            1 => {
                let index = reader.read_varint_u32()? as usize;
                Ok(PathComponent::ArrayElement(index))
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "Unknown path component type",
            )),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            PathComponent::FieldName(name) => {
                writer.write_u8(0); // 0 for field name
                writer.write_str(name);
            }
            PathComponent::ArrayElement(index) => {
                writer.write_u8(1); // 1 for array element
                writer.write_varint_u32(*index as u32);
            }
        }
    }
}

impl From<&str> for PathComponent {
    fn from(value: &str) -> Self {
        PathComponent::FieldName(value.to_string())
    }
}

impl From<usize> for PathComponent {
    fn from(index: usize) -> Self {
        PathComponent::ArrayElement(index)
    }
}

impl fmt::Display for PathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathComponent::FieldName(name) => write!(f, "{}", name),
            PathComponent::ArrayElement(index) => write!(f, "{}", index),
        }
    }
}

/// Extracts a BSON value from a document given a path.
pub fn get_path_value<'a>(doc: &'a Document, path: &[PathComponent]) -> Option<BsonValueRef<'a>> {
    if path.is_empty() {
        return None;
    }

    let mut current = match path.first()? {
        PathComponent::FieldName(name) => doc.get(name)?,
        _ => return None,
    };

    for component in path.iter().skip(1) {
        current = match (component, current) {
            (PathComponent::FieldName(name), Bson::Document(d)) => d.get(name)?,
            (PathComponent::ArrayElement(index), Bson::Array(a)) => a.get(*index)?,
            _ => return None,
        };
    }

    Some(BsonValueRef(current))
}

pub fn format_path(path: &[PathComponent]) -> String {
    path.iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Limit {
    pub skip: Option<usize>,  // Number of rows to skip
    pub limit: Option<usize>, // Maximum number of rows to return
}

impl Serializable for Limit {

    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let skip = Option::<usize>::read_from(reader)?;
        let limit = Option::<usize>::read_from(reader)?;
        Ok(Limit { skip, limit })
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.skip.write_to(writer);
        self.limit.write_to(writer);
    }
}

#[derive(Debug, Clone)]
pub struct BsonValue(pub Bson);

impl BsonValue {
    pub fn to_bson(&self) -> Bson {
        self.0.clone()
    }

    pub fn as_ref(&self) -> BsonValueRef {
        BsonValueRef(&self.0)
    }
}

impl PartialEq for BsonValue {
    fn eq(&self, other: &Self) -> bool {
        bson_utils::bson_eq(&self.0, &other.0)
    }
}

impl Eq for BsonValue {}

impl Hash for BsonValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        bson_utils::bson_hash(&self.0, state);
    }
}

impl PartialOrd for BsonValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(bson_utils::cmp_bson(&self.0, &other.0))
    }
}

impl Ord for BsonValue {
    fn cmp(&self, other: &Self) -> Ordering {
        bson_utils::cmp_bson(&self.0, &other.0)
    }
}

impl From<i32> for BsonValue {
    fn from(value: i32) -> Self {
        BsonValue(Bson::Int32(value))
    }
}

impl From<i64> for BsonValue {
    fn from(value: i64) -> Self {
        BsonValue(Bson::Int64(value))
    }
}

impl From<f64> for BsonValue {
    fn from(value: f64) -> Self {
        BsonValue(Bson::Double(value))
    }
}

impl From<&str> for BsonValue {
    fn from(value: &str) -> Self {
        BsonValue(Bson::String(value.to_string()))
    }
}

impl From<bool> for BsonValue {
    fn from(value: bool) -> Self {
        BsonValue(Bson::Boolean(value))
    }
}

impl<T> From<BTreeSet<T>> for BsonValue
where
    T: Into<Bson>// Ensure each element can be converted into `Bson`
{
    fn from(values: BTreeSet<T>) -> Self {
        BsonValue(Bson::Array(values.into_iter().map(|v| v.into()).collect()))
    }
}

impl<T> From<Vec<T>> for BsonValue
where
    T: Into<Bson>// Ensure each element can be converted into `Bson`
{
    fn from(values: Vec<T>) -> Self {
        BsonValue(Bson::Array(values.into_iter().map(|v| v.into()).collect()))
    }
}

impl BsonKey for BsonValue {

    fn try_into_key(&self) -> Result<Vec<u8>> {
        self.0.try_into_key()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BsonValueRef<'a>(pub &'a Bson);

impl<'a> BsonValueRef<'a> {
    pub fn to_owned(&self) -> BsonValue {
        BsonValue(self.0.clone())
    }
}

impl<'a> PartialEq for BsonValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        bson_utils::bson_eq(&self.0, &other.0)
    }
}

impl<'a> Eq for BsonValueRef<'a> {}

impl<'a> Hash for BsonValueRef<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        bson_utils::bson_hash(&self.0, state);
    }
}

impl<'a> PartialOrd for BsonValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(bson_utils::cmp_bson(&self.0, &other.0))
    }
}

impl<'a> Ord for BsonValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        bson_utils::cmp_bson(&self.0, &other.0)
    }
}

impl From<BsonValueRef<'_>> for Bson {
    fn from(value: BsonValueRef) -> Self { value.to_owned().to_bson() }
}

#[macro_export]
macro_rules! bson_value {
    ( $($tokens:tt)* ) => {
        BsonValue(bson::bson!($($tokens)*))
    };
}

pub struct Parameters {
    parameters: Vec<BsonValue>,
}

impl Parameters {
    pub fn new() -> Self {
        Self {
            parameters: Vec::new(),
        }
    }

    pub fn collect_parameter(&mut self, value: BsonValue) -> Arc<Expr> {
        let idx = self.parameters.len() as u32;
        self.parameters.push(value);
        Arc::new(Expr::Placeholder(idx))
    }

    pub fn get(&self, index: u32) -> &BsonValue {
        let param = self.parameters.get(index as usize);
        if param.is_none() {
            panic!("Parameter index {} out of bounds", index);
        }
        param.unwrap()
    }

    pub fn len(&self) -> usize {
        self.parameters.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::byte_writer::ByteWriter;
    use crate::io::serializable::check_serialization_round_trip;
    use bson::{doc, Bson, Regex};
    use crate::query::expr_fn::*;

    #[test]
    fn test_get_path_value() {
        let doc = doc! {
            "a": 1,
            "b": { "c": "hello" },
            "d": [10, 20, { "e": 30 }],
        };

        // Simple field
        assert_eq!(
            get_path_value(&doc, &[PathComponent::FieldName("a".to_string())]),
            Some(BsonValueRef(&Bson::Int32(1)))
        );

        // Nested field
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("b".to_string()),
                    PathComponent::FieldName("c".to_string())
                ]
            ),
            Some(BsonValueRef(&Bson::String("hello".to_string())))
        );

        // Array element
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("d".to_string()),
                    PathComponent::ArrayElement(1)
                ]
            ),
            Some(BsonValueRef(&Bson::Int32(20)))
        );

        // Nested in array
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("d".to_string()),
                    PathComponent::ArrayElement(2),
                    PathComponent::FieldName("e".to_string())
                ]
            ),
            Some(BsonValueRef(&Bson::Int32(30)))
        );

        // Non-existent field
        assert_eq!(
            get_path_value(&doc, &[PathComponent::FieldName("z".to_string())]),
            None
        );

        // Partially correct path
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("b".to_string()),
                    PathComponent::FieldName("z".to_string())
                ]
            ),
            None
        );

        // Path into non-document
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("a".to_string()),
                    PathComponent::FieldName("z".to_string())
                ]
            ),
            None
        );

        // Index into non-array
        assert_eq!(
            get_path_value(
                &doc,
                &[
                    PathComponent::FieldName("a".to_string()),
                    PathComponent::ArrayElement(0)
                ]
            ),
            None
        );

        // Empty path
        assert_eq!(get_path_value(&doc, &[]), None);
    }

    #[test]
    fn test_basic_numeric_equality() {
        assert_eq!(bson_value!(5), bson_value!(5));
        assert_eq!(bson_value!(5_i32), bson_value!(5_i32));
        assert_eq!(bson_value!(5_i64), bson_value!(5_i64));
        assert_eq!(bson_value!(5_i32), bson_value!(5_i64));
        assert_eq!(bson_value!(5.0_f64), bson_value!(5.0_f64));
        assert_eq!(bson_value!(5_i32), bson_value!(5.0_f64));
        assert_eq!(bson_value!(5_i64), bson_value!(5.0_f64));

        assert_ne!(bson_value!(5), bson_value!(6));
        assert_ne!(bson_value!(5.0_f64), bson_value!(6.0_f64));
    }

    #[test]
    fn test_floating_point_comparisons() {
        assert_eq!(bson_value!(5.0000001_f64), bson_value!(5.0000001_f64));
        assert_eq!(bson_value!(5.0_f64), bson_value!(5.0_f64));

        // Ensure different numbers are NOT equal
        assert_ne!(bson_value!(5.1_f64), bson_value!(5.0_f64));
        assert_ne!(bson_value!(5.00001_f64), bson_value!(5.0_f64));
    }

    #[test]
    fn test_nan_comparisons() {
        assert_eq!(
            bson_value!(f64::NAN),
            bson_value!(f64::NAN),
            "NaN should be equal to NaN"
        );
        assert_ne!(bson_value!(5.1_f64), bson_value!(f64::NAN));
    }

    #[test]
    fn test_boolean_equality() {
        assert_eq!(bson_value!(true), bson_value!(true));
        assert_eq!(bson_value!(false), bson_value!(false));
        assert_ne!(bson_value!(true), bson_value!(false));
    }

    #[test]
    fn test_string_equality() {
        assert_eq!(bson_value!("hello"), bson_value!("hello"));
        assert_ne!(bson_value!("hello"), bson_value!("world"));
    }

    #[test]
    fn test_array_equality() {
        let array1 = bson_value!([1, 2, 3]);
        let array2 = bson_value!([1, 2, 3]);
        let array3 = bson_value!([3, 2, 1]);

        assert_eq!(&array1, &array2);
        assert_ne!(&array1, &array3);
    }

    #[test]
    fn test_document_equality() {
        let doc1 = bson_value!({ "a": 1, "b": 2 });
        let doc2 = bson_value!({ "b": 2, "a": 1 }); // Different order, should be equal
        let doc3 = bson_value!({ "a": 1, "b": 3 });

        assert_eq!(
            &doc1, &doc2,
            "Document equality should be order-independent"
        );
        assert_ne!(&doc1, &doc3);
    }

    #[test]
    fn test_nested_document_equality() {
        let doc1 = bson_value!({ "a": { "b": 1, "c": 2 }, "d": 3 });
        let doc2 = bson_value!({ "d": 3, "a": { "c": 2, "b": 1 } });

        assert_eq!(&doc1, &doc2);
    }

    #[test]
    fn test_mixed_document_array_equality() {
        let complex1 = bson_value!({
            "users": [
                { "name": "Alice", "age": 25 },
                { "name": "Bob", "age": 30 }
            ]
        });

        let complex2 = bson_value!({
            "users": [
                { "name": "Alice", "age": 25 },
                { "name": "Bob", "age": 30 }
            ]
        });

        let complex3 = bson_value!({
            "users": [
                { "name": "Bob", "age": 30 },
                { "name": "Alice", "age": 25 }
            ]
        });

        assert_eq!(&complex1, &complex2);
        assert_ne!(&complex1, &complex3, "Array order should matter");
    }

    #[test]
    fn test_regex_equality() {
        let regex1 = BsonValue(Bson::RegularExpression(Regex {
            pattern: "abc.*".to_string(),
            options: "i".to_string(),
        }));

        let regex2 = BsonValue(Bson::RegularExpression(Regex {
            pattern: "abc.*".to_string(),
            options: "i".to_string(),
        }));

        let regex3 = BsonValue(Bson::RegularExpression(Regex {
            pattern: "abc.*".to_string(),
            options: "".to_string(),
        }));

        assert_eq!(&regex1, &regex2);
        assert_ne!(&regex1, &regex3, "Regex options should matter");
    }

    #[test]
    fn test_null_equality() {
        assert_eq!(BsonValue(Bson::Null), BsonValue(Bson::Null));
    }

    #[test]
    fn test_different_types_not_equal() {
        assert_ne!(bson_value!(5), bson_value!("5"));
        assert_ne!(bson_value!(true), bson_value!(1));
        assert_ne!(bson_value!(Bson::Null), bson_value!(false));
    }

    #[test]
    #[should_panic(expected = "LogicalPlans should never be serialized before parametrization.")]
    fn test_serialize_literal_panics() {
        let expr = Expr::Literal(bson_value!(10));
        let mut writer = ByteWriter::new();
        expr.write_to(&mut writer);
    }

    #[test]
    fn test_expr_serialization_round_trip() {
        // Simple expressions
        check_serialization_round_trip(Expr::AlwaysTrue);
        check_serialization_round_trip(Expr::AlwaysFalse);
        check_serialization_round_trip(Expr::Exists(true));
        check_serialization_round_trip(Expr::Placeholder(42));

        // Field expressions
        check_serialization_round_trip(Expr::Field(vec!["a".into(), "b".into()]));
        check_serialization_round_trip(Expr::PositionalField(vec!["a".into(), 0.into()]));

        // Comparison
        let comparison = Expr::Comparison {
            operator: ComparisonOperator::Eq,
            value: Arc::new(Expr::Placeholder(0)),
        };
        check_serialization_round_trip(comparison);

        // Logical expressions
        let not_expr = Expr::Not(Arc::new(Expr::Exists(true)));
        check_serialization_round_trip(not_expr);

        let and_expr = Expr::And(vec![
            Arc::new(Expr::Exists(true)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(1)),
            }),
        ]);
        check_serialization_round_trip(and_expr);

        let or_expr = Expr::Or(vec![
            Arc::new(Expr::Exists(false)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(2)),
            }),
        ]);
        check_serialization_round_trip(or_expr);

        let nor_expr = Expr::Nor(vec![
            Arc::new(Expr::Exists(true)),
            Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(3)),
            }),
        ]);
        check_serialization_round_trip(nor_expr);

        // FieldFilters
        let field_filters = Expr::FieldFilters {
            field: Arc::new(Expr::Field(vec!["price".into()])),
            filters: vec![
                Arc::new(Expr::Comparison {
                    operator: ComparisonOperator::Gt,
                    value: Arc::new(Expr::Placeholder(4)),
                }),
                Arc::new(Expr::Exists(true)),
            ],
        };
        check_serialization_round_trip(field_filters);

        // Array expressions
        let all_expr = Expr::All(Arc::new(Expr::Placeholder(5)));
        check_serialization_round_trip(all_expr);

        let elem_match_expr = Expr::ElemMatch(vec![Arc::new(Expr::Comparison {
            operator: ComparisonOperator::Gte,
            value: Arc::new(Expr::Placeholder(6)),
        })]);
        check_serialization_round_trip(elem_match_expr);

        // Type and Size
        let type_expr = Expr::Type {
            bson_type: Arc::new(Expr::Placeholder(7)),
            negated: true,
        };
        check_serialization_round_trip(type_expr);

        let size_expr = Expr::Size {
            size: Arc::new(Expr::Placeholder(8)),
            negated: false,
        };
        check_serialization_round_trip(size_expr);
    }

    #[test]
    fn test_projection_serialization_round_trip() {
        // Simple ProjectionExpr
        check_serialization_round_trip(proj_field());
        check_serialization_round_trip(proj_positional_field());
        check_serialization_round_trip(proj_slice(Some(5), 10));
        check_serialization_round_trip(proj_slice(None, 20));
        let elem_match_expr = proj_elem_match(eq(placeholder(0)));
        check_serialization_round_trip((*elem_match_expr).clone());

        // Complex ProjectionExpr
        let complex_projection_expr = proj_fields([
            ("field_a", proj_field()),
            (
                "field_b",
                proj_fields([("nested_field", proj_field())]),
            ),
            (
                "array_field",
                proj_array_elements([(0, proj_slice(Some(5), 10))]),
            ),
        ]);
        check_serialization_round_trip(complex_projection_expr.clone());

        // Projection
        check_serialization_round_trip(Projection::Include(complex_projection_expr.clone()));
        check_serialization_round_trip(Projection::Exclude(complex_projection_expr.clone()));

        let simple_include = Projection::Include(proj_fields([("a", proj_field())]));
        check_serialization_round_trip(simple_include);

        let simple_exclude = Projection::Exclude(proj_fields([("b", proj_field())]));
        check_serialization_round_trip(simple_exclude);
    }

    #[test]
    fn test_projection_expr_add_remove() {
        // Initial empty projection
        let mut proj = proj_fields::<_, &str>([]);
        let proj = Arc::get_mut(&mut proj).unwrap();
        assert!(proj.is_empty());

        // Add a simple field "a"
        let path_a: Vec<PathComponent> = vec!["a".into()];
        proj.add_expr(&path_a, 0, proj_field()).unwrap();
        assert_eq!(*proj, *proj_fields([("a", proj_field())]));

        // Add a nested field "b.c"
        let path_b_c: Vec<PathComponent> = vec!["b".into(), "c".into()];
        proj.add_expr(&path_b_c, 0, proj_field()).unwrap();

        let expected = proj_fields([
            ("a", proj_field()),
            ("b", proj_fields([("c", proj_field())])),
        ]);
        assert_eq!(*proj, *expected);

        // Add an array projection "d" with element "0"
        let path_d: Vec<PathComponent> = vec!["d".into()];
        let proj_array_d = proj_array_elements([(0, proj_field())]);
        proj.add_expr(&path_d, 0, proj_array_d).unwrap();

        let expected_d = proj_array_elements([(0, proj_field())]);
        let children_of_root = match &proj {
            ProjectionExpr::Fields { children } => children,
            _ => panic!("Expected Fields projection"),
        };
        assert_eq!(
            children_of_root.get(&"d".into()).unwrap().as_ref(),
            expected_d.as_ref()
        );

        // --- Test removals ---

        // Remove "a"
        proj.remove_expr(&path_a, 0).unwrap();
        let children_of_root = match &proj {
            ProjectionExpr::Fields { children } => children,
            _ => panic!("Expected Fields projection"),
        };
        assert!(!children_of_root.contains_key(&"a".into()));

        // Remove "b.c" - this should also remove "b" as it becomes empty
        proj.remove_expr(&path_b_c, 0).unwrap();
        let children_of_root = match &proj {
            ProjectionExpr::Fields { children } => children,
            _ => panic!("Expected Fields projection"),
        };
        assert!(children_of_root.get(&"b".into()).is_none());
        assert!(!proj.is_empty()); // "d" should still be there

        // Remove "d"
        proj.remove_expr(&path_d, 0).unwrap();
        assert!(proj.is_empty());
    }

    #[test]
    fn test_projection_expr_errors() {
        let mut proj = (*proj_fields::<_, &str>([])).clone();

        // Add "a"
        let path_a: Vec<PathComponent> = vec!["a".into()];
        proj.add_expr(&path_a, 0, proj_field()).unwrap();

        // Error: Try to add "a.b" when "a" is already a terminal field
        let path_a_b: Vec<PathComponent> = vec!["a".into(), "b".into()];
        let result = proj.add_expr(&path_a_b, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid projection specification for path a.b"
        );

        // Error: Try to add "a" again, which is an invalid replacement
        let result = proj.add_expr(&path_a, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid projection specification for path a"
        );

        // Error: Invalid path component type (array index for Fields)
        let path_invalid_component: Vec<PathComponent> = vec![0.into()];
        let result = proj.add_expr(&path_invalid_component, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot use the array index 0 in path: 0. Expecting a field name."
        );

        let mut proj_array = proj_array_elements::<_, usize>([]);
        let proj_array = Arc::get_mut(&mut proj_array).unwrap();

        // Error: Invalid path component type (field name for ArrayElements)
        let path_invalid_component_for_array: Vec<PathComponent> = vec!["field".into()];
        let result =
            proj_array.add_expr(&path_invalid_component_for_array, 0, proj_field());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Cannot use the field name field in path: field. Expecting an array element."
        );

        // Test removing non-existent path is a no-op
        let path_z: Vec<PathComponent> = vec!["z".into()];
        let original_proj = proj.clone();
        proj.remove_expr(&path_z, 0).unwrap();
        assert_eq!(proj, original_proj);
    }
}
