use crate::query::logical::Expr;
use crate::query::tree_node::TreeNode;
use std::sync::Arc;

/// Represents the LogicalPlan for MongoDB-like operations
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// Represents a collection scan with optional projection, filtering, and sorting.
    TableScan {
        collection: String,
        projection: Option<Vec<String>>, // Fields to include
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
            LogicalPlan::TableScan { .. } => vec![], // Leaf node
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
            LogicalPlan::TableScan { .. } => self, // Leaf nodes remain unchanged
        }
    }
}

impl LogicalPlan {
    fn get_first(children: Vec<Arc<LogicalPlan>>) -> Arc<LogicalPlan> {
        children.into_iter().next().unwrap()
    }
}

/// Projection for included or excluded fields
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    Include(Vec<Arc<Expr>>), // Fields to include
    Exclude(Vec<Arc<Expr>>), // Fields to exclude
}

/// Represents the sort order for a field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Represents a sorting instruction for a query.
#[derive(Debug, Clone, PartialEq)]
pub struct SortField {
    pub field: Arc<Expr>,
    pub order: SortOrder,
}

/// A builder for constructing `LogicalPlan` instances.
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Starts with a `TableScan` plan.
    pub fn scan(_database: &str, collection: &str) -> Self {
        Self {
            plan: LogicalPlan::TableScan {
                collection: collection.to_string(),
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
