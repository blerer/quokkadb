use crate::query::Expr;
use std::ops::Bound;
use std::sync::Arc;
use crate::query::logical_plan::Projection;
use crate::query::logical_plan::SortField;
use crate::storage::Direction;

/// Represents a physical plan that can be executed by the query engine.
///
/// A physical plan is a tree of operators that defines the concrete steps
/// to execute a query. It is generated from a `LogicalPlan` by the optimizer.
/// Unlike the logical plan, which describes *what* data is needed, the physical
/// plan describes *how* to get it.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// Scans a collection, optionally over a specific range of primary keys.
    CollectionScan {
        /// The identifier for the collection.
        collection: u32,
        /// The start bound of the range for the scan on the user primary key.
        start: Bound<Expr>,
        /// The end bound of the range for the scan on the user primary key.
        end: Bound<Expr>,
        /// The direction in which the scan must be performed
        direction: Direction,
        /// An optional list of fields to project. If `None`, all fields are returned.
        projection: Option<Arc<Projection>>,
    },

    /// Performs a lookup on an index for a single key.
    PointSearch {
        /// The identifier for the collection.
        collection: u32,
        /// The identifier for the index.
        index: u32,
        /// The user key to search for.
        key: Expr,
        /// An optional list of fields to project. If `None`, all fields are returned.
        projection: Option<Arc<Projection>>,
    },

    /// Scans an index over a given range.
    IndexScan {
        /// The identifier for the collection.
        collection: u32,
        /// The identifier for the index.
        index: u32,
        /// The start bound of the range for the scan on the index.
        start: Bound<Expr>,
        /// The end bound of the range for the scan on the index.
        end: Bound<Expr>,
        /// An optional list of fields to project. If `None`, all fields are returned.
        projection: Option<Arc<Projection>>,
    },

    /// Inserts a document into a collection. This is a terminal operator.
    InsertOne {
        /// The identifier for the collection.
        collection: u32,
        /// The document to be inserted.
        document: Vec<u8>,
    },

    /// Inserts a set of documents into a collection. This is a terminal operator.
    InsertMany {
        /// The identifier for the collection.
        collection: u32,
        /// The documents to be inserted.
        documents: Vec<Vec<u8>>,
    },

    /// Filters rows from its input based on a predicate.
    /// This is typically executed in memory on the data returned by the child operator.
    Filter {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The expression to evaluate for each row.
        predicate: Arc<Expr>,
    },

    /// Projects a new set of fields from the input.
    Projection {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// A list of (new_name, existing_field) tuples for projection.
        projection: Arc<Projection>,
    },

    /// Sorts the input in memory. This is for datasets that are expected to fit in memory.
    InMemorySort {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The fields and direction to sort by.
        sort_fields: Arc<Vec<SortField>>,
    },

    /// Sorts a large input using external merge sort, which can spill to disk.
    ExternalMergeSort {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The fields and direction to sort by.
        sort_fields: Arc<Vec<SortField>>,
        /// The maximum number of rows to keep in memory before spilling to disk.
        /// This is used to control memory usage.
        max_in_memory_rows: usize,
    },

    /// Finds the top K elements using a heap. This is an optimization for `ORDER BY ... LIMIT`.
    TopKHeapSort {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The fields and direction to sort by.
        sort_fields: Arc<Vec<SortField>>,
        /// The number of top elements to retrieve.
        k: usize,
    },

    /// Skips and/or limits the number of rows from its input.
    Limit {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The number of rows to skip.
        skip: Option<usize>,
        /// The maximum number of rows to return.
        limit: Option<usize>,
    },
}
