use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::query::logical::Expr;
use crate::query::tree_node::TreeNode;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

/// Represents the LogicalPlan for MongoDB-like operations
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Represents a collection scan with optional projection, filtering, and sorting.
    CollectionScan {
        collection: u32, // Collection identifier
        projection: Option<Projection>, // Fields to include
        filter: Option<Expr>,            // Optional filtering condition
        sort: Option<Vec<SortField>>,    // Optional sorting fields
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
        skip: Option<usize>,  // Number of rows to skip
        limit: Option<usize>, // Maximum number of rows to return
    },
}

impl TreeNode for LogicalPlan {
    type Child = LogicalPlan;

    /// Return references to the children of the current node
    fn children(&self) -> Vec<Arc<Self::Child>> {
        match self {
            LogicalPlan::Filter { input, .. } => vec![input.clone()],
            LogicalPlan::Projection { input, .. } => vec![input.clone()],
            LogicalPlan::Sort { input, .. } => vec![input.clone()],
            LogicalPlan::Limit { input, .. } => vec![input.clone()],
            LogicalPlan::CollectionScan { .. } => vec![], // Leaf node
        }
    }

    /// Create a new node with updated children
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<Self::Child>>) -> Arc<Self> {
        match self.as_ref() {
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
            LogicalPlan::Limit { skip, limit, .. } => Arc::new(LogicalPlan::Limit {
                input: Self::get_first(children),
                skip: *skip,
                limit: *limit,
            }),
            LogicalPlan::CollectionScan { .. } => self, // Leaf nodes remain unchanged
        }
    }
}

impl LogicalPlan {
    fn get_first(children: Vec<Arc<LogicalPlan>>) -> Arc<LogicalPlan> {
        children.into_iter().next().unwrap()
    }
}

impl Serializable for LogicalPlan {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => {
                // CollectionScan
                let collection = reader.read_varint_u32()?;
                let projection = Option::<Projection>::read_from(reader)?;
                let filter = Option::<Expr>::read_from(reader)?;
                let sort = Option::<Vec<SortField>>::read_from(reader)?;
                Ok(LogicalPlan::CollectionScan {
                    collection,
                    projection,
                    filter,
                    sort,
                })
            }
            1 => {
                // Filter
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let condition = Arc::<Expr>::read_from(reader)?;
                Ok(LogicalPlan::Filter { input, condition })
            }
            2 => {
                // Projection
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let projection = Arc::<Projection>::read_from(reader)?;
                Ok(LogicalPlan::Projection {
                    input,
                    projection,
                })
            }
            3 => {
                // Sort
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let sort_fields = Arc::<Vec<SortField>>::read_from(reader)?;
                Ok(LogicalPlan::Sort {
                    input,
                    sort_fields,
                })
            }
            4 => {
                // Limit
                let input = Arc::<LogicalPlan>::read_from(reader)?;
                let skip = Option::<usize>::read_from(reader)?;
                let limit = Option::<usize>::read_from(reader)?;
                Ok(LogicalPlan::Limit {
                    input,
                    skip,
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
            LogicalPlan::CollectionScan {
                collection,
                projection,
                filter,
                sort,
            } => {
                writer.write_u8(0);
                writer.write_varint_u32(*collection);
                projection.write_to(writer);
                filter.write_to(writer);
                sort.write_to(writer);
            }
            LogicalPlan::Filter { input, condition } => {
                writer.write_u8(1);
                input.write_to(writer);
                condition.write_to(writer);
            }
            LogicalPlan::Projection { input, projection } => {
                writer.write_u8(2);
                input.write_to(writer);
                projection.write_to(writer);
            }
            LogicalPlan::Sort {
                input,
                sort_fields,
            } => {
                writer.write_u8(3);
                input.write_to(writer);
                sort_fields.write_to(writer);
            }
            LogicalPlan::Limit {
                input,
                skip,
                limit,
            } => {
                writer.write_u8(4);
                input.write_to(writer);
                skip.write_to(writer);
                limit.write_to(writer);
            }
        }
    }
}

/// Projection for included or excluded fields
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Include(Vec<Arc<Expr>>), // Fields to include
    Exclude(Vec<Arc<Expr>>), // Fields to exclude
}

impl Serializable for Projection {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let tag = reader.read_u8()?;
        match tag {
            0 => Ok(Projection::Include(Vec::<Arc<Expr>>::read_from(reader)?)),
            1 => Ok(Projection::Exclude(Vec::<Arc<Expr>>::read_from(reader)?)),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid tag for Projection",
            )),
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct SortField {
    pub field: Arc<Expr>,
    pub order: SortOrder,
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

/// A builder for constructing `LogicalPlan` instances.
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Starts with a `TableScan` plan.
    pub fn scan(collection: u32) -> Self {
        Self {
            plan: LogicalPlan::CollectionScan {
                collection,
                projection: None,
                filter: None,
                sort: None,
            },
        }
    }

    /// Adds a filter condition.
    pub fn filter(mut self, condition: Arc<Expr>) -> Self {
        self.plan = LogicalPlan::Filter {
            input: Arc::new(self.plan),
            condition,
        };
        self
    }

    /// Specifies fields for projection.
    pub fn project(mut self, projection: Projection) -> Self {
        self.plan = LogicalPlan::Projection {
            input: Arc::new(self.plan),
            projection: Arc::new(projection),
        };
        self
    }

    /// Specifies sorting order.
    pub fn sort(mut self, sort_fields: Vec<SortField>) -> Self {
        self.plan = LogicalPlan::Sort {
            input: Arc::new(self.plan),
            sort_fields: Arc::new(sort_fields),
        };
        self
    }

    /// Adds a limit and/or skip operation.
    pub fn limit(mut self, skip: Option<usize>, limit: Option<usize>) -> Self {
        self.plan = LogicalPlan::Limit {
            input: Arc::new(self.plan),
            skip,
            limit,
        };
        self
    }

    /// Finalizes the build process and returns the `LogicalPlan`.
    pub fn build(self) -> Arc<LogicalPlan> {
        Arc::new(self.plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::serializable::check_serialization_round_trip;
    use crate::query::logical::expr_fn::field;
    use crate::query::logical::ComparisonOperator;

    #[test]
    fn test_logical_plan_serialization_round_trip() {
        let plan = LogicalPlan::CollectionScan {
            collection: 32,
            projection: Some(Projection::Include(vec![field(["field1"]), field(["field2"])])),
            filter: Some(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(0)),
            }),
            sort: Some(vec![SortField {
                field: field(["a"]),
                order: SortOrder::Ascending,
            }]),
        };
        check_serialization_round_trip(plan);

        let plan_arc = LogicalPlanBuilder::scan(32)
            .filter(Arc::new(Expr::Comparison {
                operator: ComparisonOperator::Eq,
                value: Arc::new(Expr::Placeholder(0)),
            }))
            .project(Projection::Include(vec![field(["a"])]))
            .sort(vec![SortField {
                field: field(["b"]),
                order: SortOrder::Descending,
            }])
            .limit(Some(10), Some(20))
            .build();

        check_serialization_round_trip(plan_arc.as_ref().clone());
    }
}
