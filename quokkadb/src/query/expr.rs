use crate::error;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::io::size_estimate::SizeEstimate;
use crate::query::tree_node::TreeNode;
use crate::query::{BsonValue, ComparisonOperator, ParameterKind, PathComponent, format_path};
use crate::util::interval::Interval;
use bson::Bson;
use std::collections::BTreeMap;
use std::io::Result;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

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
    Placeholder {
        index: u32,
        kind: ParameterKind,
    },
    /// Multiple filters on the same field (e.g., `{ "price": { "$ne": 1.99, "$exists": true } }`)
    FieldFilters {
        field: Arc<Expr>,
        filters: Vec<Arc<Expr>>, // List of filters on the same field
    },
    /// A single comparison (e.g., `$gt: 5`, `$eq: "Alice"`)
    Comparison {
        operator: ComparisonOperator, // `$eq`, `$ne`, `$gt`, `$lt`, etc.
        value: Arc<Expr>,             // The literal value
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

impl SizeEstimate for Expr {
    fn estimated_heap_size(&self) -> usize {
        match self {
            Expr::Field(path) | Expr::PositionalField(path) => path.estimated_heap_size(),
            Expr::Literal(value) => value.estimated_heap_size(),
            Expr::Placeholder { .. } | Expr::Exists(_) | Expr::AlwaysTrue | Expr::AlwaysFalse => 0,
            Expr::FieldFilters { field, filters } => {
                field.estimated_heap_size() + filters.estimated_heap_size()
            }
            Expr::Comparison { value, .. } => value.estimated_heap_size(),
            Expr::Interval(interval) => interval.estimated_heap_size(),
            Expr::And(children)
            | Expr::Or(children)
            | Expr::Nor(children)
            | Expr::ElemMatch(children) => children.estimated_heap_size(),
            Expr::Not(child) | Expr::All(child) => child.estimated_heap_size(),
            Expr::Type { bson_type, .. } => bson_type.estimated_heap_size(),
            Expr::Size { size, .. } => size.estimated_heap_size(),
        }
    }
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
                    children.push(child.clone());
                }
                if let Some(child) = interval.end_bound_value() {
                    children.push(child.clone());
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
                let old_children = [interval.start_bound(), interval.end_bound()];
                let old_children = old_children.iter().filter_map(|bound| match bound {
                    Bound::Included(endpoint) | Bound::Excluded(endpoint) => {
                        Some((*endpoint).clone())
                    }
                    Bound::Unbounded => None,
                });
                assert_eq!(
                    old_children.clone().count(),
                    children.len(),
                    "Interval child count mismatch"
                );
                if old_children
                    .zip(&children)
                    .all(|(old, new)| Arc::ptr_eq(&old, new))
                {
                    return self;
                }
                let mut iter = children.into_iter();
                let mut replace = |bound: Bound<&Arc<Expr>>| match bound {
                    Bound::Included(_) => Bound::Included(iter.next().unwrap()),
                    Bound::Excluded(_) => Bound::Excluded(iter.next().unwrap()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let start = replace(interval.start_bound());
                let end = replace(interval.end_bound());
                Arc::new(Expr::Interval(Interval::new(start, end)))
            }
            Expr::Type { negated, .. } => Arc::new(Expr::Type {
                bson_type: Self::get_first(children),
                negated: *negated,
            }),
            Expr::Size { negated, .. } => Arc::new(Expr::Size {
                size: Self::get_first(children),
                negated: *negated,
            }),
            Expr::All(..) => {
                assert_eq!(
                    children.len(),
                    1,
                    "All operator should have exactly one child"
                );
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

                Arc::new(Expr::Or(field_predicates))
            }
            // Opposite range comparisons are not complements for arrays or missing fields.
            Expr::Comparison {
                operator:
                    ComparisonOperator::Gt
                    | ComparisonOperator::Gte
                    | ComparisonOperator::Lt
                    | ComparisonOperator::Lte,
                ..
            } => Arc::new(Expr::Not(Arc::new(self.clone()))),
            Expr::Comparison { operator, value } => Arc::new(Expr::Comparison {
                operator: operator.negate(),
                value: value.clone(),
            }),
            Expr::Exists(bool) => Arc::new(Expr::Exists(!*bool)),
            Expr::All(_) => Arc::new(Expr::Not(Arc::new(self.clone()))),
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
            _ => Arc::new(Expr::Not(Arc::new(self.clone()))),
        }
    }
}

mod flags {
    pub const NOT: u8 = 1;
    pub const NOR: u8 = 2;
    pub const AND: u8 = 3;
    pub const OR: u8 = 4;
    pub const FIELD_FILTERS: u8 = 5;
    pub const COMPARISON: u8 = 6;
    pub const EXISTS: u8 = 7;
    pub const ALL: u8 = 8;
    pub const TYPE: u8 = 9;
    pub const SIZE: u8 = 10;
    pub const ALWAYS_TRUE: u8 = 11;
    pub const ALWAYS_FALSE: u8 = 12;
    pub const FIELD: u8 = 13;
    pub const POSITIONAL_FIELD: u8 = 14;
    pub const PLACEHOLDER: u8 = 15;
    pub const ELEM_MATCH: u8 = 16;
    pub const INTERVAL: u8 = 17;
}

impl Serializable for Expr {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            flags::NOT => {
                let expr = Arc::new(Self::read_from(reader, version)?);
                Ok(Expr::Not(expr))
            }
            flags::NOR => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader, version)?;
                Ok(Expr::Nor(exprs))
            }
            flags::AND => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader, version)?;
                Ok(Expr::And(exprs))
            }
            flags::OR => {
                let exprs = Vec::<Arc<Expr>>::read_from(reader, version)?;
                Ok(Expr::Or(exprs))
            }
            flags::FIELD_FILTERS => {
                let field = Arc::new(Self::read_from(reader, version)?);
                let filters = Vec::<Arc<Expr>>::read_from(reader, version)?;
                Ok(Expr::FieldFilters { field, filters })
            }
            flags::COMPARISON => {
                let operator = ComparisonOperator::read_from(reader, version)?;
                let value = Arc::new(Self::read_from(reader, version)?);
                Ok(Expr::Comparison { operator, value })
            }
            flags::EXISTS => {
                let exists = reader.read_u8()? == 1;
                Ok(Expr::Exists(exists))
            }
            flags::ALL => {
                let values = Arc::new(Self::read_from(reader, version)?);
                Ok(Expr::All(values))
            }
            flags::TYPE => {
                let bson_type = Arc::new(Self::read_from(reader, version)?);
                let negated = reader.read_u8()? == 1;
                Ok(Expr::Type { bson_type, negated })
            }
            flags::SIZE => {
                let size = Arc::new(Self::read_from(reader, version)?);
                let negated = reader.read_u8()? == 1;
                Ok(Expr::Size { size, negated })
            }
            flags::ALWAYS_TRUE => Ok(Expr::AlwaysTrue),
            flags::ALWAYS_FALSE => Ok(Expr::AlwaysFalse),
            flags::FIELD => {
                let path = Vec::<PathComponent>::read_from(reader, version)?;
                Ok(Expr::Field(path))
            }
            flags::POSITIONAL_FIELD => {
                let path = Vec::<PathComponent>::read_from(reader, version)?;
                Ok(Expr::PositionalField(path))
            }
            flags::PLACEHOLDER => {
                let idx = reader.read_varint_u32()?;
                let kind = match reader.read_u8()? {
                    0 => ParameterKind::NonArray,
                    1 => ParameterKind::Array,
                    tag => unreachable!("Invalid parameter kind tag: {tag}"),
                };
                Ok(Expr::Placeholder { index: idx, kind })
            }
            flags::ELEM_MATCH => {
                let predicates = Vec::<Arc<Expr>>::read_from(reader, version)?;
                Ok(Expr::ElemMatch(predicates))
            }
            flags::INTERVAL => {
                let interval = Interval::<Arc<Expr>>::read_from(reader, version)?;
                Ok(Expr::Interval(interval))
            }
            _ => panic!("Unknown Expr tag: {}", tag),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        match self {
            Expr::Not(expr) => {
                writer.write_u8(flags::NOT);
                expr.write_to(writer, version);
            }
            Expr::Nor(exprs) => {
                writer.write_u8(flags::NOR);
                exprs.write_to(writer, version);
            }
            Expr::And(exprs) => {
                writer.write_u8(flags::AND);
                exprs.write_to(writer, version);
            }
            Expr::Or(exprs) => {
                writer.write_u8(flags::OR);
                exprs.write_to(writer, version);
            }
            Expr::FieldFilters { field, filters } => {
                writer.write_u8(flags::FIELD_FILTERS);
                field.write_to(writer, version);
                filters.write_to(writer, version);
            }
            Expr::Comparison { operator, value } => {
                writer.write_u8(flags::COMPARISON);
                operator.write_to(writer, version);
                value.write_to(writer, version);
            }
            Expr::Exists(bool) => {
                writer.write_u8(flags::EXISTS);
                writer.write_u8(if *bool { 1 } else { 0 });
            }
            Expr::All(values) => {
                writer.write_u8(flags::ALL);
                values.write_to(writer, version);
            }
            Expr::Type { bson_type, negated } => {
                writer.write_u8(flags::TYPE);
                bson_type.write_to(writer, version);
                writer.write_u8(if *negated { 1 } else { 0 });
            }
            Expr::Size { size, negated } => {
                writer.write_u8(flags::SIZE);
                size.write_to(writer, version);
                writer.write_u8(if *negated { 1 } else { 0 });
            }
            Expr::AlwaysTrue => {
                writer.write_u8(flags::ALWAYS_TRUE);
            }
            Expr::AlwaysFalse => {
                writer.write_u8(flags::ALWAYS_FALSE);
            }
            Expr::Field(path) => {
                writer.write_u8(flags::FIELD);
                path.write_to(writer, version);
            }
            Expr::PositionalField(path) => {
                writer.write_u8(flags::POSITIONAL_FIELD);
                path.write_to(writer, version);
            }
            Expr::Placeholder { index: idx, kind } => {
                writer.write_u8(flags::PLACEHOLDER);
                writer.write_varint_u32(*idx); // Write the placeholder index for extra safety
                writer.write_u8(match kind {
                    ParameterKind::NonArray => 0,
                    ParameterKind::Array => 1,
                });
            }
            Expr::ElemMatch(predicates) => {
                writer.write_u8(flags::ELEM_MATCH);
                predicates.write_to(writer, version);
            }
            Expr::Literal(_) => {
                panic!("LogicalPlans should never be serialized before parametrization.");
            }
            Expr::Interval(interval) => {
                writer.write_u8(flags::INTERVAL);
                interval.write_to(writer, version);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionExpr {
    Fields {
        children: BTreeMap<PathComponent, Arc<ProjectionExpr>>,
    },
    ArrayElements {
        children: BTreeMap<PathComponent, Arc<ProjectionExpr>>,
    },
    Field,
    Slice {
        skip: Option<i32>,
        limit: i32,
    },
    ElemMatch {
        filter: Arc<Expr>,
    },
}

impl ProjectionExpr {
    fn get_children_for_component_mut<'a, 'b: 'a>(
        &'a mut self,
        path: &'b [PathComponent],
        component: usize,
    ) -> error::Result<&'a mut BTreeMap<PathComponent, Arc<ProjectionExpr>>> {
        match self {
            ProjectionExpr::Fields { children } => {
                if component < path.len()
                    && matches!(&path[component], &PathComponent::ArrayElement(_))
                {
                    return Err(error::Error::InvalidRequest(format!(
                        "Cannot use the array index {} in path: {}. Expecting a field name.",
                        &path[component],
                        format_path(path)
                    )));
                }
                Ok(children)
            }
            ProjectionExpr::ArrayElements { children } => {
                if component < path.len()
                    && matches!(&path[component], &PathComponent::FieldName(_))
                {
                    return Err(error::Error::InvalidRequest(format!(
                        "Cannot use the field name {} in path: {}. Expecting an array element.",
                        &path[component],
                        format_path(path)
                    )));
                }
                Ok(children)
            }
            _ => Err(error::Error::InvalidRequest(format!(
                "Invalid projection specification for path {}",
                format_path(path)
            ))),
        }
    }

    pub fn add_expr(
        &mut self,
        path: &[PathComponent],
        component: usize,
        expr: Arc<ProjectionExpr>,
    ) -> error::Result<()> {
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
        children: &mut BTreeMap<PathComponent, Arc<ProjectionExpr>>,
    ) -> error::Result<()> {
        let current_component = &path[component];
        let existing = children.get_mut(&current_component);
        match existing {
            None => {
                if path.len() == component + 1 {
                    children.insert(current_component.clone(), expr);
                } else {
                    let mut sub_node = if matches!(path[1], PathComponent::FieldName(_)) {
                        ProjectionExpr::Fields {
                            children: BTreeMap::new(),
                        }
                    } else {
                        ProjectionExpr::ArrayElements {
                            children: BTreeMap::new(),
                        }
                    };
                    sub_node.add_expr(path, component + 1, expr)?;
                    children.insert(current_component.clone(), Arc::new(sub_node));
                }
            }
            Some(existing) => match existing.as_ref() {
                ProjectionExpr::Fields { children: _ }
                | ProjectionExpr::ArrayElements { children: _ } => {
                    if path.len() == component + 1 {
                        return Err(error::Error::InvalidRequest(format!(
                            "Invalid projection specification for path {}",
                            format_path(path)
                        )));
                    } else if let Some(existing) = Arc::get_mut(existing) {
                        existing.add_expr(path, component + 1, expr)?;
                    } else {
                        panic!("Arc strong count is not 1 but should be ")
                    }
                }
                _ => {
                    return Err(error::Error::InvalidRequest(format!(
                        "Invalid projection specification for path {}",
                        format_path(path)
                    )));
                }
            },
        }
        Ok(())
    }

    fn remove_from_children(
        path: &[PathComponent],
        component: usize,
        children: &mut BTreeMap<PathComponent, Arc<ProjectionExpr>>,
    ) -> error::Result<()> {
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
            ProjectionExpr::Fields { children } | ProjectionExpr::ArrayElements { children } => {
                children.values().cloned().collect()
            }
            _ => vec![],
        }
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self>>) -> Arc<Self> {
        match self.as_ref() {
            ProjectionExpr::Fields { children: old } => {
                if old.is_empty() {
                    return self;
                }
                assert_eq!(old.len(), children.len(), "child count mismatch");
                let new_map = old.keys().cloned().zip(children.into_iter()).collect();
                Arc::new(ProjectionExpr::Fields { children: new_map })
            }
            ProjectionExpr::ArrayElements { children: old } => {
                if old.is_empty() {
                    return self;
                }
                assert_eq!(old.len(), children.len(), "child count mismatch");
                let new_map = old.keys().cloned().zip(children.into_iter()).collect();
                Arc::new(ProjectionExpr::ArrayElements { children: new_map })
            }
            _ => self, // leaves
        }
    }
}

impl Serializable for ProjectionExpr {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(ProjectionExpr::Fields {
                children: BTreeMap::<PathComponent, Arc<ProjectionExpr>>::read_from(
                    reader, version,
                )?,
            }),
            1 => Ok(ProjectionExpr::ArrayElements {
                children: BTreeMap::<PathComponent, Arc<ProjectionExpr>>::read_from(
                    reader, version,
                )?,
            }),
            2 => Ok(ProjectionExpr::Field),
            3 => Ok(ProjectionExpr::Slice {
                skip: Option::<i32>::read_from(reader, version)?,
                limit: reader.read_varint_i32()?,
            }),
            4 => Ok(ProjectionExpr::ElemMatch {
                filter: Arc::<Expr>::read_from(reader, version)?,
            }),
            _ => unreachable!("Invalid tag for ProjectionExpr: {}", tag),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        match self {
            ProjectionExpr::Fields { children } => {
                writer.write_u8(0);
                children.write_to(writer, version);
            }
            ProjectionExpr::ArrayElements { children } => {
                writer.write_u8(1);
                children.write_to(writer, version);
            }
            ProjectionExpr::Field => {
                writer.write_u8(2);
            }
            ProjectionExpr::Slice { skip, limit } => {
                writer.write_u8(3);
                skip.write_to(writer, version);
                writer.write_varint_i32(*limit);
            }
            ProjectionExpr::ElemMatch { filter } => {
                writer.write_u8(4);
                filter.write_to(writer, version);
            }
        }
    }
}

impl SizeEstimate for ProjectionExpr {
    fn estimated_heap_size(&self) -> usize {
        match self {
            ProjectionExpr::Fields { children } | ProjectionExpr::ArrayElements { children } => {
                children.estimated_heap_size()
            }
            ProjectionExpr::Field => 0,
            ProjectionExpr::Slice { skip, limit } => {
                skip.estimated_heap_size() + limit.estimated_heap_size()
            }
            ProjectionExpr::ElemMatch { filter } => filter.estimated_heap_size(),
        }
    }
}
