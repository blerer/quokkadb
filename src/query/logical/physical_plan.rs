use crate::query::logical::{logical_plan::SortField, Expr};
use bson::Document;
use std::ops::Range;
use std::sync::Arc;
use crate::storage::Direction;

/// Represents a physical plan that can be executed by the query engine.
///
/// A physical plan is a tree of operators that defines the concrete steps
/// to execute a query. It is generated from a `LogicalPlan` by the optimizer.
/// Unlike the logical plan, which describes *what* data is needed, the physical
/// plan describes *how* to get it.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    /// Scans a collection, optionally over a specific range of primary keys.
    TableScan {
        /// The identifier for the collection.
        collection: u32,
        /// The range for the scan on the user primary key.
        range: Range<Vec<u8>>,
        /// The direction in which the scan must be performed
        direction: Direction,
        /// An optional list of fields to project. If `None`, all fields are returned.
        projection: Option<Vec<String>>,
    },

    /// Performs a lookup on an index for a single key.
    PointSearch {
        /// The identifier for the collection.
        collection: u32,
        /// The identifier for the index.
        index: u32,
        /// The user key to search for.
        key: Vec<u8>,
        /// An optional list of fields to project. If `None`, all fields are returned.
        projection: Option<Vec<String>>,
    },

    /// Scans an index over a given range.
    IndexScan {
        /// The identifier for the collection.
        collection: u32,
        /// The identifier for the index.
        index: u32,
        /// The range to scan on the index.
        range: Range<Vec<u8>>,
        /// An optional list of fields to project. If `None`, all fields are returned.
        projection: Option<Vec<String>>,
    },

    /// Inserts a set of documents into a collection. This is a terminal operator.
    Insert {
        /// The identifier for the collection.
        collection: u32,
        /// The documents to be inserted.
        documents: Vec<Document>,
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
        fields: Vec<(String, String)>,
    },

    /// Sorts the input in memory. This is for datasets that are expected to fit in memory.
    InMemorySort {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The fields and direction to sort by.
        sort_fields: Vec<SortField>,
    },

    /// Sorts a large input using external merge sort, which can spill to disk.
    ExternalMergeSort {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The fields and direction to sort by.
        sort_fields: Vec<SortField>,
    },

    /// Finds the top K elements using a heap. This is an optimization for `ORDER BY ... LIMIT`.
    TopKHeapSort {
        /// The input plan that provides the data.
        input: Arc<PhysicalPlan>,
        /// The fields and direction to sort by.
        sort_fields: Vec<SortField>,
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
