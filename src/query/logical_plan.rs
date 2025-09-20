use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::query::{Expr, Limit, Projection, ProjectionExpr, SortField};
use crate::query::tree_node::TreeNode;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use crate::query::update::UpdateExpr;
use crate::util::murmur_hash64::murmur_hash64a;

/// Represents the LogicalPlan for MongoDB-like operations
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Represents a no-operation plan, used for empty or trivial plans. This is a terminal operator.
    NoOp,
    /// Represents an insert operation for a single document. This is a terminal operator.
    InsertOne {
        collection: u32, // Collection identifier
        document: Vec<u8>, // The document to be inserted.
    },
    /// Represents an insert operation for set of documents into a collection. This is a terminal operator.
    InsertMany {
        collection: u32, // Collection identifier
        documents: Vec<Vec<u8>>, // The documents to be inserted.
    },
    /// Represents an update operation for a single document. This is a terminal operator.
    UpdateOne {
        collection: u32, // Collection identifier
        query: Arc<LogicalPlan>, // Filter to match the document to update
        update: UpdateExpr, // Update operations
    },
    /// Represents an update operation for multiple documents. This is a terminal operator.
    UpdateMany {
        collection: u32, // Collection identifier
        query: Arc<LogicalPlan>, // Filter to match documents to update
        update: UpdateExpr, // Update operations
    },
    /// Represents a collection scan with optional projection, filtering, and sorting. This is a terminal operator.
    CollectionScan {
        collection: u32, // Collection identifier
        projection: Option<Arc<Projection>>, // Fields to include
        filter: Option<Arc<Expr>>,            // Optional filtering condition
        sort: Option<Arc<Vec<SortField>>>,    // Optional sorting fields
    },

    /// Represents a filter operation.
    Filter {
        input: Arc<LogicalPlan>,
        condition: Arc<Expr>, // The filter expression
    },

    /// Represents a projection operation.
    Projection {
        input: Arc<LogicalPlan>,
        projection: Arc<Projection>, // The projection
    },

    /// Represents a sort operation.
    Sort {
        input: Arc<LogicalPlan>,
        sort_fields: Arc<Vec<SortField>>, // Fields and sort directions
    },

    /// Represents limit and skip combined into a single node.
    Limit {
        input: Arc<LogicalPlan>,
        limit: Limit,
    },
}

impl TreeNode for LogicalPlan {
    type Child = LogicalPlan;

    /// Return references to the children of the current node
    fn children(&self) -> Vec<Arc<Self::Child>> {
        match self {
            LogicalPlan::UpdateOne { query, .. } => vec![query.clone()],
            LogicalPlan::UpdateMany { query, .. } => vec![query.clone()],
            LogicalPlan::Filter { input, .. } => vec![input.clone()],
            LogicalPlan::Projection { input, .. } => vec![input.clone()],
            LogicalPlan::Sort { input, .. } => vec![input.clone()],
            LogicalPlan::Limit { input, .. } => vec![input.clone()],
            _ => vec![], // terminal nodes
        }
    }

    /// Create a new node with updated children
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self::Child>>) -> Arc<Self> {
        match self.as_ref() {
            LogicalPlan::UpdateOne { collection, update, .. } => Arc::new(LogicalPlan::UpdateOne {
                collection: *collection,
                query: Self::get_first(children),
                update: update.clone(),
            }),
            LogicalPlan::UpdateMany { collection, update, .. } => Arc::new(LogicalPlan::UpdateMany {
                collection: *collection,
                query: Self::get_first(children),
                update: update.clone(),
            }),
            LogicalPlan::Filter { condition, .. } => Arc::new(LogicalPlan::Filter {
                input: Self::get_first(children),
                condition: condition.clone(),
            }),
            LogicalPlan::Projection { projection, .. } => Arc::new(LogicalPlan::Projection {
                input: Self::get_first(children),
                projection: projection.clone(),
            }),
            LogicalPlan::Sort { sort_fields, .. } => Arc::new(LogicalPlan::Sort {
                input: Self::get_first(children),
                sort_fields: sort_fields.clone(),
            }),
            LogicalPlan::Limit { limit, .. } => Arc::new(LogicalPlan::Limit {
                input: Self::get_first(children),
                limit: limit.clone(),
            }),
            _ => self, // Terminal nodes remain unchanged
        }
    }
}

// Seed for MurmurHash64
const HASH_SEED: u64 = 20250309;

impl LogicalPlan {
    fn get_first(children: Vec<Arc<LogicalPlan>>) -> Arc<LogicalPlan> {
        children.into_iter().next().unwrap()
    }

    pub fn compute_hash(&self) -> u64 {
        const HASH_SEED: u64 = 20250309;

        let mut writer = ByteWriter::new();
        self.write_to(&mut writer);
        let bytes = writer.take_buffer();
        murmur_hash64a(&bytes, HASH_SEED)
    }
}

impl Serializable for LogicalPlan {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => {
                // NoOp
                Ok(LogicalPlan::NoOp)
            }
            1 => {
                // CollectionScan
                let collection = reader.read_varint_u32()?;
                let projection = Option::<Arc<Projection>>::read_from(reader)?;
                let filter = Option::<Arc<Expr>>::read_from(reader)?;
                let sort = Option::<Arc<Vec<SortField>>>::read_from(reader)?;
                Ok(LogicalPlan::CollectionScan {
                    collection,
                    projection,
                    filter,
                    sort,
                })
            }
            2 => {
                // Filter
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let condition = Arc::<Expr>::read_from(reader)?;
                Ok(LogicalPlan::Filter { input, condition })
            }
            3 => {
                // Projection
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let projection = Arc::<Projection>::read_from(reader)?;
                Ok(LogicalPlan::Projection {
                    input,
                    projection,
                })
            }
            4 => {
                // Sort
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let sort_fields = Arc::<Vec<SortField>>::read_from(reader)?;
                Ok(LogicalPlan::Sort {
                    input,
                    sort_fields,
                })
            }
            5 => {
                // Limit
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let limit = Limit {
                    skip: Option::<usize>::read_from(reader)?,
                    limit: Option::<usize>::read_from(reader)?,
                };
                Ok(LogicalPlan::Limit {
                    input,
                    limit,
                })
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid tag for LogicalPlan",
            )),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            LogicalPlan::NoOp => {
                writer.write_u8(0);
            }
            LogicalPlan::CollectionScan {
                collection,
                projection,
                filter,
                sort,
            } => {
                writer.write_u8(1);
                writer.write_varint_u32(*collection);
                projection.write_to(writer);
                filter.write_to(writer);
                sort.write_to(writer);
            }
            LogicalPlan::Filter { input, condition } => {
                writer.write_u8(2);
                input.write_to(writer);
                condition.write_to(writer);
            }
            LogicalPlan::Projection { input, projection } => {
                writer.write_u8(3);
                input.write_to(writer);
                projection.write_to(writer);
            }
            LogicalPlan::Sort {
                input,
                sort_fields,
            } => {
                writer.write_u8(4);
                input.write_to(writer);
                sort_fields.write_to(writer);
            }
            LogicalPlan::Limit {
                input,
                limit,
            } => {
                writer.write_u8(5);
                input.write_to(writer);
                limit.write_to(writer);
            }
            _ => {
                // For the other variants, serialization is not implemented as serialization is
                // only required for caching query statements.
                unimplemented!("Serialization for this LogicalPlan variant is not implemented");
            }
        }
    }
}

/// A builder for constructing `LogicalPlan` instances.
pub struct LogicalPlanBuilder {
    plan: Arc<LogicalPlan>,
}

impl LogicalPlanBuilder {
    /// Starts with a `TableScan` plan.
    pub fn scan(collection: u32) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::CollectionScan {
                collection,
                projection: None,
                filter: None,
                sort: None,
            }),
        }
    }

    #[cfg(test)]
    pub fn scan_with_filters_and_projections(
        collection: u32,
        filter: Option<Arc<Expr>>,
        projection: Option<Arc<Projection>>,
        sort: Option<Arc<Vec<SortField>>>) -> Self {
        Self {
            plan: Arc::new(LogicalPlan::CollectionScan {
                collection,
                projection,
                filter,
                sort,
            }),
        }
    }

    /// Adds a filter condition.
    pub fn filter(mut self, condition: Arc<Expr>) -> Self {
        self.plan = Arc::new(LogicalPlan::Filter {
            input: self.plan,
            condition,
        });
        self
    }

    /// Specifies fields for projection.
    pub fn project(mut self, projection: Arc<Projection>) -> Self {
        self.plan = Arc::new(LogicalPlan::Projection {
            input: self.plan,
            projection,
        });
        self
    }

    /// Specifies sorting order.
    pub fn sort(mut self, sort_fields: Arc<Vec<SortField>>) -> Self {
        self.plan = Arc::new(LogicalPlan::Sort {
            input: self.plan,
            sort_fields,
        });
        self
    }

    /// Adds a limit and/or skip operation.
    pub fn limit(mut self, skip: Option<usize>, limit: Option<usize>) -> Self {
        self.plan = Arc::new(LogicalPlan::Limit {
            input: self.plan,
            limit: Limit { skip, limit }
        });
        self
    }

    /// Finalizes the build process and returns the `LogicalPlan`.
    pub fn build(self) -> Arc<LogicalPlan> {
        self.plan.clone()
    }
}

/// Transforms the logical plan in a bottom-up way by applying a function to all filter expressions,
/// including those nested within projection expressions.
pub fn transform_up_filter<F>(plan: Arc<LogicalPlan>, function: F) -> Arc<LogicalPlan>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone,
{
    plan.transform_up(&|node: Arc<LogicalPlan>| match node.as_ref() {
        LogicalPlan::Filter { input, condition } => {
            let expr = condition.clone().transform_up(&|c| function(c));
            Arc::new(LogicalPlan::Filter {
                input: input.clone(),
                condition: expr,
            })
        }
        LogicalPlan::Projection { input, projection } => {
            let projection = transform_up_projection(&projection, &function);
            Arc::new(LogicalPlan::Projection {
                input: input.clone(),
                projection,
            })
        }
        LogicalPlan::CollectionScan { collection, projection, filter, sort } => {
            if filter.is_none() && projection.is_none() {
                return node;
            }

            let filter = if let Some(filter) = filter {
                Some(filter.clone().transform_up(&|c| function(c)))
            } else {
                None
            };

            let projection = if let Some(projection) = projection {
                Some(transform_up_projection(&projection, &function))
            } else {
                None
            };

            Arc::new(LogicalPlan::CollectionScan {
                collection: *collection,
                projection,
                filter,
                sort: sort.clone(),
            })
        },
        _ => node,
    })
}

fn transform_up_projection<F>(projection: &Arc<Projection>, function: &F) -> Arc<Projection>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone
{
    let projection = Arc::new(match projection.as_ref() {
        Projection::Include(proj_exprs) => {
            let new_projection = transform_up_proj_expr_filters(&function, &proj_exprs);
            Projection::Include(new_projection)
        }
        Projection::Exclude(proj_exprs) => {
            let new_projection = transform_up_proj_expr_filters(&function, &proj_exprs);
            Projection::Exclude(new_projection)
        }
    });
    projection
}

fn transform_up_proj_expr_filters<F>(function: F, proj_exprs: &Arc<ProjectionExpr>) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone,
{
    proj_exprs.clone().transform_up(&|c| transform_up_proj_elem_match_filter(c, function.clone()))
}

fn transform_up_proj_elem_match_filter<F>(
    proj_expr: Arc<ProjectionExpr>,
    function: F,
) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone,
{
    match proj_expr.as_ref() {
        ProjectionExpr::ElemMatch { filter } => {
            let new_expr = filter.clone().transform_up(&|c| function(c));
            Arc::new(ProjectionExpr::ElemMatch{ filter: new_expr })
        }
        _ => proj_expr.clone(),
    }
}

/// Transforms the logical plan in a top-down way by applying a function to all filter expressions,
/// including those nested within projection expressions.
pub fn transform_down_filter<F>(plan: Arc<LogicalPlan>, function: F) -> Arc<LogicalPlan>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone,
{
    plan.transform_down(&|node: Arc<LogicalPlan>| {
        match node.as_ref() {
            LogicalPlan::Filter { input, condition } => {
                let expr = condition.clone().transform_down(&function);
                Arc::new(LogicalPlan::Filter {
                    input: input.clone(),
                    condition: expr,
                })
            }
            LogicalPlan::Projection { input, projection } => {
                let projection = transform_down_projection(&projection, &function);
                Arc::new(LogicalPlan::Projection {
                    input: input.clone(),
                    projection,
                })
            }
            LogicalPlan::CollectionScan { collection, projection, filter, sort } => {
                if filter.is_none() && projection.is_none() {
                    return node;
                }

                let filter = if let Some(filter) = filter {
                    Some(filter.clone().transform_down(&|c| function(c)))
                } else {
                    None
                };

                let projection = if let Some(projection) = projection {
                    Some(transform_down_projection(&projection, &function))
                } else {
                    None
                };

                Arc::new(LogicalPlan::CollectionScan {
                    collection: *collection,
                    projection,
                    filter,
                    sort: sort.clone(),
                })
            },
            _ => node.clone(),
        }
    })
}

fn transform_down_projection<F>(projection: &Arc<Projection>, function: &F) -> Arc<Projection>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone
{
    let projection = Arc::new(match projection.as_ref() {
        Projection::Include(proj_exprs) => {
            let new_projection = transform_down_proj_expr_filters(&function, proj_exprs);
            Projection::Include(new_projection)
        }
        Projection::Exclude(proj_exprs) => {
            let new_projection = transform_down_proj_expr_filters(&function, proj_exprs);
            Projection::Exclude(new_projection)
        }
    });
    projection
}

fn transform_down_proj_expr_filters<F>(
    function: &F,
    proj_exprs: &Arc<ProjectionExpr>,
) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone,
{
    proj_exprs
        .clone()
        .transform_down(&|c| transform_down_proj_elem_match_filter(c, function))
}

fn transform_down_proj_elem_match_filter<F>(
    proj_expr: Arc<ProjectionExpr>,
    function: &F,
) -> Arc<ProjectionExpr>
where
    F: Fn(Arc<Expr>) -> Arc<Expr> + Clone,
{
    match proj_expr.as_ref() {
        ProjectionExpr::ElemMatch { filter } => {
            let new_expr = filter.clone().transform_down(function);
            Arc::new(ProjectionExpr::ElemMatch { filter: new_expr })
        }
        _ => proj_expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::serializable::check_serialization_round_trip;
    use crate::query::expr_fn::{elem_match, eq, field, field_filters, include, lit, placeholder, proj_elem_match, proj_field, proj_fields, sort_asc};
    use crate::query::{ComparisonOperator, SortOrder};

    #[test]
    fn test_logical_plan_serialization_round_trip() {
        check_serialization_round_trip(LogicalPlan::NoOp);

        let plan = LogicalPlan::CollectionScan {
            collection: 32,
            projection: Some(include(proj_fields([
                ("field1", proj_field()),
                ("field2", proj_field()),
            ]))),
            filter: Some(field_filters(field(["a"]), [eq(placeholder(0))])),
            sort: Some(Arc::new(vec![SortField {
                field: field(["a"]),
                order: SortOrder::Ascending,
            }])),
        };
        check_serialization_round_trip(plan);

        let plan_arc = LogicalPlanBuilder::scan(32)
            .filter(Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(0)),
            }))
            .project(include(proj_fields([
                ("a", proj_field()),
            ])))
            .sort(Arc::new(vec!(sort_asc(field(["b"])))))
            .limit(Some(10), Some(20))
            .build();

        check_serialization_round_trip(plan_arc.as_ref().clone());
    }

    #[test]
    fn test_transform_up_and_transform_down_filter() {
        // The expression to find and replace.
        let original = lit(10);
        // The expression to replace with.
        let transformed = lit("replaced");

        // Transformation closure for bottom-up traversal.
        let transformation = |expr: Arc<Expr>| -> Arc<Expr> {
            if *expr == *original {
                transformed.clone()
            } else {
                expr
            }
        };

        // Test with LogicalPlan::Filter
        let plan = LogicalPlanBuilder::scan_with_filters_and_projections(123,
                                                                         Some(eq(original.clone())),
                                                                         None,
                                                                         None)
            .filter(elem_match([eq(original.clone())]))
            .project(include(proj_elem_match(eq(original.clone()))))
            .build();

        let transformed_up = transform_up_filter(plan.clone(), transformation.clone());
        let transformed_down = transform_down_filter(plan.clone(), transformation.clone());

        let expected = LogicalPlanBuilder::scan_with_filters_and_projections(123,
                                                                                  Some(eq(transformed.clone())),
                                                                                  None,
                                                                                  None)
            .filter(elem_match([eq(transformed.clone())]))
            .project(include(proj_elem_match(eq(transformed.clone()))))
            .build();

        assert_eq!(transformed_up, expected);
        assert_eq!(transformed_down, expected);
    }
}
