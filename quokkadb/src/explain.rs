//! Public, read-only descriptions of query plans.
//!
//! These types support diagnostics and checks of broad planner choices, such as
//! whether a query uses a secondary index, without exposing executor plan nodes.

use crate::query::physical_plan::PhysicalPlan;
use crate::storage::Direction;
use crate::storage::catalog::Catalog;

/// A structured, read-only description of a planned query.
///
/// The plan reflects the optimizer's current choices. Operator details are
/// useful for diagnostics and tests, but those choices can change as the
/// optimizer evolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainPlan {
    /// The root operator of the plan tree.
    pub root: ExplainNode,
}

impl ExplainPlan {
    /// Creates an explanation from its root operator.
    pub fn new(root: ExplainNode) -> Self {
        Self { root }
    }

    /// Converts an internal read plan into its public diagnostic description.
    ///
    /// Keeping this conversion in the explanation layer prevents query
    /// execution internals from depending on the public API. Index metadata is
    /// resolved here so the result exposes names rather than catalog IDs.
    pub(crate) fn from_physical_plan(plan: &PhysicalPlan, catalog: &Catalog) -> Self {
        Self::new(explain_node(plan, catalog))
    }
}

fn explain_node(plan: &PhysicalPlan, catalog: &Catalog) -> ExplainNode {
    let (operator, children) = match plan {
        PhysicalPlan::NoOp => (ExplainOperator::NoOp, vec![]),
        PhysicalPlan::CollectionScan { direction, .. } => (
            ExplainOperator::CollectionScan {
                direction: explain_direction(direction),
            },
            vec![],
        ),
        PhysicalPlan::PointSearch { .. } => (ExplainOperator::PointSearch, vec![]),
        PhysicalPlan::IndexScan {
            collection,
            index,
            range,
            direction,
            ..
        } => {
            let metadata = catalog
                .get_collection_by_id(collection)
                .and_then(|collection| collection.get_index_by_id(*index))
                .unwrap_or_else(|| {
                    panic!(
                        "physical plan references missing index {index} in collection {collection}"
                    )
                });
            (
                ExplainOperator::IndexScan {
                    index_name: metadata.name(),
                    direction: explain_direction(direction),
                    equality_prefix_len: range.equal_prefix.len(),
                    has_range: range.tail.is_some(),
                },
                vec![],
            )
        }
        PhysicalPlan::MultiPointSearch { direction, .. } => (
            ExplainOperator::MultiPointSearch {
                direction: explain_direction(direction),
            },
            vec![],
        ),
        PhysicalPlan::Filter { input, .. } => {
            (ExplainOperator::Filter, vec![explain_node(input, catalog)])
        }
        PhysicalPlan::Projection { input, .. } => (
            ExplainOperator::Projection,
            vec![explain_node(input, catalog)],
        ),
        PhysicalPlan::InMemorySort { input, .. } => (
            ExplainOperator::InMemorySort,
            vec![explain_node(input, catalog)],
        ),
        PhysicalPlan::ExternalMergeSort {
            input,
            max_in_memory_rows,
            ..
        } => (
            ExplainOperator::ExternalMergeSort {
                max_in_memory_rows: *max_in_memory_rows,
            },
            vec![explain_node(input, catalog)],
        ),
        PhysicalPlan::TopKHeapSort { input, k, .. } => (
            ExplainOperator::TopKHeapSort { k: *k },
            vec![explain_node(input, catalog)],
        ),
        PhysicalPlan::Limit { input, skip, limit } => (
            ExplainOperator::Limit {
                skip: *skip,
                limit: *limit,
            },
            vec![explain_node(input, catalog)],
        ),
        PhysicalPlan::InsertOne { .. }
        | PhysicalPlan::InsertMany { .. }
        | PhysicalPlan::UpdateOne { .. }
        | PhysicalPlan::UpdateMany { .. }
        | PhysicalPlan::FindOneAndUpdate { .. }
        | PhysicalPlan::ReplaceOne { .. }
        | PhysicalPlan::FindOneAndReplace { .. }
        | PhysicalPlan::FindOneAndDelete { .. }
        | PhysicalPlan::DeleteOne { .. }
        | PhysicalPlan::DeleteMany { .. } => {
            unreachable!("explain is only supported for read query plans")
        }
    };

    ExplainNode::new(operator, children)
}

fn explain_direction(direction: &Direction) -> ExplainDirection {
    match direction {
        Direction::Forward => ExplainDirection::Forward,
        Direction::Reverse => ExplainDirection::Reverse,
    }
}

/// One operator in an explained query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainNode {
    /// The operation performed at this node.
    pub operator: ExplainOperator,
    /// Input plans consumed by this operator, in execution order.
    pub children: Vec<ExplainNode>,
}

impl ExplainNode {
    /// Creates an operator node with its input plans.
    pub fn new(operator: ExplainOperator, children: Vec<ExplainNode>) -> Self {
        Self { operator, children }
    }

    /// Returns whether this node or any of its descendants has `operator`.
    pub fn contains_operator(&self, operator: ExplainOperatorKind) -> bool {
        self.operator.kind() == operator
            || self
                .children
                .iter()
                .any(|child| child.contains_operator(operator))
    }
}

/// The operation represented by a node in an explained plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplainOperator {
    /// The query is known to produce no results.
    NoOp,
    /// Reads documents by scanning the collection's primary-key range.
    CollectionScan { direction: ExplainDirection },
    /// Reads one document by its primary key.
    PointSearch,
    /// Reads documents through a secondary index.
    IndexScan {
        /// Name of the selected index.
        index_name: String,
        /// Direction in which the index is scanned.
        direction: ExplainDirection,
        /// Number of leading index fields constrained by equality.
        equality_prefix_len: usize,
        /// Whether the next index field has a range constraint.
        has_range: bool,
    },
    /// Reads multiple documents by their primary keys.
    MultiPointSearch { direction: ExplainDirection },
    /// Applies a predicate to the input documents.
    Filter,
    /// Selects or reshapes fields from the input documents.
    Projection,
    /// Sorts all input documents in memory.
    InMemorySort,
    /// Sorts input documents using an external merge sort.
    ExternalMergeSort {
        /// Maximum number of rows kept in memory before spilling.
        max_in_memory_rows: usize,
    },
    /// Selects the first `k` sorted input documents using a heap.
    TopKHeapSort {
        /// Number of top documents retained by the sort.
        k: usize,
    },
    /// Skips and/or limits documents from the input.
    Limit {
        /// Number of input documents to skip.
        skip: Option<usize>,
        /// Maximum number of documents to return.
        limit: Option<usize>,
    },
}

impl ExplainOperator {
    /// Returns the broad kind of this operator, without its plan-specific details.
    pub fn kind(&self) -> ExplainOperatorKind {
        match self {
            Self::NoOp => ExplainOperatorKind::NoOp,
            Self::CollectionScan { .. } => ExplainOperatorKind::CollectionScan,
            Self::PointSearch => ExplainOperatorKind::PointSearch,
            Self::IndexScan { .. } => ExplainOperatorKind::IndexScan,
            Self::MultiPointSearch { .. } => ExplainOperatorKind::MultiPointSearch,
            Self::Filter => ExplainOperatorKind::Filter,
            Self::Projection => ExplainOperatorKind::Projection,
            Self::InMemorySort => ExplainOperatorKind::InMemorySort,
            Self::ExternalMergeSort { .. } => ExplainOperatorKind::ExternalMergeSort,
            Self::TopKHeapSort { .. } => ExplainOperatorKind::TopKHeapSort,
            Self::Limit { .. } => ExplainOperatorKind::Limit,
        }
    }
}

/// Broad operator categories, useful for assertions that ignore plan details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainOperatorKind {
    /// The query is known to produce no results.
    NoOp,
    /// Reads documents by scanning a collection's primary-key range.
    CollectionScan,
    /// Reads one document by its primary key.
    PointSearch,
    /// Reads documents through a secondary index.
    IndexScan,
    /// Reads multiple documents by their primary keys.
    MultiPointSearch,
    /// Filters documents from its input.
    Filter,
    /// Selects or reshapes fields.
    Projection,
    /// Sorts all input documents in memory.
    InMemorySort,
    /// Sorts input documents using an external merge sort.
    ExternalMergeSort,
    /// Selects the first sorted documents using a heap.
    TopKHeapSort,
    /// Skips and/or limits input documents.
    Limit,
}

/// Traversal direction used when scanning a collection or index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainDirection {
    /// Traverses the scan in its forward direction.
    Forward,
    /// Traverses the scan in its reverse direction.
    Reverse,
}
