mod parametrize_test {
    use super::super::*;
    use crate::query::BsonValue;
    use crate::query::expr_fn::{
        and, eq, exists, field, field_filters, interval, lit, placeholder,
    };
    use crate::query::logical_plan::LogicalPlanBuilder;

    #[test]
    fn test_parametrize_simple_filter() {
        let optimizer = Optimizer::new(&Options::default());
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection, None)
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build_arc();

        // Let's first check when the filter stay at the filter level
        let (parametrized_plan, params) = optimizer.parametrize(plan.clone());

        // Check params
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0), &BsonValue::from(10));

        // Check plan
        let expected_filter = field_filters(field(["a"]), vec![eq(placeholder(0))]);
        let expected_plan = LogicalPlanBuilder::scan(collection, None)
            .filter(expected_filter)
            .build_arc();

        assert_eq!(parametrized_plan, expected_plan);

        // Let's check when the filter is pushed down at the collection scan level
        let plan = optimizer.normalize(plan.clone());

        // Let's first check when the filter stay at the filter level
        let (parametrized_plan, params) = optimizer.parametrize(plan.clone());

        // Check params
        assert_eq!(params.len(), 1);
        assert_eq!(params.get(0), &BsonValue::from(10));

        // Check plan
        let expected_filter = field_filters(
            field(["a"]),
            vec![interval(Interval::closed(
                QueryEndpoint::Value(placeholder(0)),
                QueryEndpoint::Value(placeholder(0)),
            ))],
        );
        let expected_plan = LogicalPlanBuilder::scan_with_filters_and_projections(
            collection,
            Some(expected_filter),
            None,
            None,
        )
        .build_arc();
        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_complex_filter() {
        let optimizer = Optimizer::new(&Options::default());
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection, None)
            .filter(and(vec![
                field_filters(field(["a"]), vec![eq(lit(10))]),
                field_filters(field(["b"]), vec![eq(lit("hello"))]),
            ]))
            .build_arc();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 2);
        assert_eq!(params.get(0), &BsonValue::from(10));
        assert_eq!(params.get(1), &BsonValue::from("hello"));

        // Check plan
        let expected_condition = and(vec![
            field_filters(field(["a"]), vec![eq(placeholder(0))]),
            field_filters(field(["b"]), vec![eq(placeholder(1))]),
        ]);

        let expected_plan = LogicalPlanBuilder::scan(collection, None)
            .filter(expected_condition)
            .build_arc();

        assert_eq!(parametrized_plan, expected_plan);
    }

    #[test]
    fn test_parametrize_no_literals() {
        let optimizer = Optimizer::new(&Options::default());
        let collection = 14;
        let plan = LogicalPlanBuilder::scan(collection, None)
            .filter(field_filters(field(["a"]), vec![exists(true)]))
            .build_arc();
        let plan_clone = plan.clone();

        let (parametrized_plan, params) = optimizer.parametrize(plan);

        // Check params
        assert_eq!(params.len(), 0);

        // Check plan
        assert_eq!(parametrized_plan, plan_clone);
    }
}

mod optimizer_tests {
    use super::super::*;
    use crate::options::storage_quantity::StorageQuantity;
    use crate::query::expr_fn::{
        and, array_placeholder, eq, exists, field, field_filters, gt, gte, include, interval, lit,
        lte, ne, placeholder, proj_field, proj_fields, within,
    };
    use crate::query::logical_plan::LogicalPlanBuilder;
    use crate::storage::catalog::{CollectionBuilder, IndexBuilder};
    use crate::storage::count_stats::{CountStats, CountStatsKey};
    use bson::Bson;
    use std::collections::BTreeMap;

    const COLLECTION: u32 = 10;

    #[test]
    fn test_compute_residual_filter_removes_multiple_predicates() {
        let field_a = field(["a"]);
        let field_b = field(["b"]);
        let original = and([
            field_filters(field_a.clone(), [eq(placeholder(0)), gt(placeholder(1))]),
            field_filters(field_b.clone(), [gte(placeholder(2)), lte(placeholder(3))]),
        ]);
        let used_predicates = [(field_a, 0), (field_b, 1)];

        let residual = compute_residual_filter(original, &used_predicates);

        assert_eq!(
            residual,
            Some(and([
                field_filters(field(["a"]), [gt(placeholder(1))]),
                field_filters(field(["b"]), [gte(placeholder(2))]),
            ]))
        );
    }

    #[test]
    fn test_compute_residual_filter_removes_all_predicates_for_a_field() {
        let field_a = field(["a"]);
        let original = field_filters(field_a.clone(), [eq(placeholder(0)), gt(placeholder(1))]);
        let used_predicates = [(field_a.clone(), 0), (field_a, 1)];

        assert_eq!(compute_residual_filter(original, &used_predicates), None);
    }

    #[test]
    fn test_compound_index_selects_points_and_preserves_other_predicates() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and([
                field_filters(field(["a"]), vec![gt(lit(15)), eq(lit(10))]),
                field_filters(field(["b"]), vec![gte(lit(0)), lte(lit(20))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 2,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: Some(Interval::at_most(QueryEndpoint::Value(placeholder(2)))),
            },
            direction: Direction::Forward,
            filter: Some(and([
                field_filters(
                    field(["a"]),
                    [interval(Interval::greater_than(QueryEndpoint::Value(
                        placeholder(1),
                    )))],
                ),
                field_filters(
                    field(["b"]),
                    [interval(Interval::at_least(QueryEndpoint::Value(
                        placeholder(3),
                    )))],
                ),
            ])),
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_primary_key_selects_point_and_preserves_same_field_residual() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(
                field(["_id"]),
                vec![gt(lit(15)), eq(lit(10))],
            ))
            .build_arc();

        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: Some(field_filters(
                field(["_id"]),
                [interval(Interval::greater_than(QueryEndpoint::Value(
                    placeholder(1),
                )))],
            )),
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_primary_key_multipoint_preserves_same_field_residual() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(
                field(["_id"]),
                vec![within(lit(values)), ne(lit(10))],
            ))
            .build_arc();

        let output = PhysicalPlan::Filter {
            input: Arc::new(PhysicalPlan::MultiPointSearch {
                collection: COLLECTION,
                keys: array_placeholder(1),
                direction: Direction::Forward,
                filter: None,
                projection: None,
            }),
            predicate: field_filters(field(["_id"]), [ne(placeholder(0))]),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_index_scan_skips_non_interval_predicate_and_preserves_residual() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["a"]), vec![ne(lit(1)), eq(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::Filter {
            input: Arc::new(PhysicalPlan::IndexScan {
                collection: COLLECTION,
                index: 1,
                range: IndexScanRangeExpr {
                    equal_prefix: vec![placeholder(1)],
                    tail: None,
                },
                direction: Direction::Forward,
                filter: None,
                projection: None,
            }),
            predicate: field_filters(field(["a"]), [ne(placeholder(0))]),
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_primary_key_without_usable_predicate_keeps_full_scan_and_filter() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["_id"]), [ne(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::Filter {
            input: Arc::new(full_scan_plan()),
            predicate: field_filters(field(["_id"]), [ne(placeholder(0))]),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_noop() {
        check_optimization(Arc::new(LogicalPlan::NoOp), PhysicalPlan::NoOp);
    }

    #[test]
    fn test_optimize_collection_scan_no_filter() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None).build_arc();
        let output = full_scan_plan();

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_scan_with_non_pk_filter() {
        let filters = field_filters(field(["a"]), vec![gt(lit(10))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters.clone())
            .build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: Some(field_filters(
                field(["a"]),
                [interval(Interval::greater_than(QueryEndpoint::Value(
                    placeholder(0),
                )))],
            )),
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_projection() {
        let projection = include(proj_fields([("name", proj_field())]));
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .project(Some(projection.clone()))
            .build_arc();

        let output = PhysicalPlan::Projection {
            input: Arc::new(full_scan_plan()),
            projection,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_limit() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .limit(Some(10), Some(20))
            .build_arc();
        let output = PhysicalPlan::Limit {
            input: Arc::new(full_scan_plan()),
            skip: Some(10),
            limit: Some(20),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_point_search() {
        let filters = field_filters(field(["_id"]), vec![eq(lit(123))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();

        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_range_scan() {
        let filters = field_filters(field(["_id"]), vec![gt(lit(123))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(QueryEndpoint::Value(placeholder(0))),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_range_scan_with_residual() {
        let filters = and(vec![
            field_filters(field(["_id"]), vec![gt(lit(123))]),
            field_filters(field(["a"]), vec![gt(lit(10))]),
        ]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();

        // The residual filter will also be normalized and parametrized.
        let residual_filter = field_filters(
            field(["a"]),
            [interval(Interval::greater_than(QueryEndpoint::Value(
                placeholder(1),
            )))],
        );

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(QueryEndpoint::Value(placeholder(0))),
            direction: Direction::Forward,
            filter: Some(residual_filter),
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_pk_exists_true_is_full_scan() {
        let filters = field_filters(field(["_id"]), vec![exists(true)]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();
        let output = full_scan_plan();
        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_sort_elimination_pk_asc() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["_id"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields))
            .build_arc();

        // Sort is eliminated because collection scan provides data sorted by _id asc.
        let output = full_scan_plan();

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_sort_elimination_pk_desc() {
        let sort_fields = Arc::new(vec![SortField::desc(field(["_id"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields))
            .build_arc();

        // Sort is eliminated, and scan direction is reversed.
        let mut output = full_scan_plan();
        if let PhysicalPlan::CollectionScan { direction, .. } = &mut output {
            *direction = Direction::Reverse;
        }

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_sort_elimination_point_search() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["_id"]), vec![eq(lit(123))]))
            .sort(Some(sort_fields))
            .build_arc();

        // Sort is eliminated because point search returns at most one row.
        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_topk_heap_sort() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields.clone()))
            .limit(Some(5), Some(10))
            .build_arc();

        let topk_sort = PhysicalPlan::TopKHeapSort {
            input: Arc::new(full_scan_plan()),
            sort_fields,
            k: 15,
        };

        let output = PhysicalPlan::Limit {
            input: Arc::new(topk_sort),
            skip: Some(5),
            limit: Some(10),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_sort_memory_limit_uses_document_size_statistics() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields.clone()))
            .build_arc();

        let options = Options::default().with_sort_memory_limit(StorageQuantity::from_bytes(4096));
        let optimizer = Optimizer::new(&options);
        let stats = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(COLLECTION), 1),
            (CountStatsKey::CollectionDocumentBytes(COLLECTION), 1024),
        ]));
        let physical = optimizer
            .optimize(
                optimizer.parametrize(optimizer.normalize(input)).0,
                &test_metadata(),
                &stats,
            )
            .unwrap();

        assert!(matches!(
            physical.as_ref(),
            PhysicalPlan::ExternalMergeSort {
                max_in_memory_rows: 1,
                ..
            }
        ));
    }

    #[test]
    fn test_estimate_sort_row_bytes_falls_back_without_valid_statistics() {
        let optimizer = Optimizer::new(&Options::default());
        let metadata = test_metadata();

        assert_eq!(
            optimizer.estimate_sort_row_bytes(&metadata, &CountStats::default()),
            1024
        );

        let zero_stats = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(COLLECTION), -1),
            (CountStatsKey::CollectionDocumentBytes(COLLECTION), 1000),
        ]));
        assert_eq!(
            optimizer.estimate_sort_row_bytes(&metadata, &zero_stats),
            1024
        );

        let negative_stats = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(COLLECTION), 1),
            (CountStatsKey::CollectionDocumentBytes(COLLECTION), -1),
        ]));
        assert_eq!(
            optimizer.estimate_sort_row_bytes(&metadata, &negative_stats),
            1024
        );
    }

    #[test]
    fn test_estimate_sort_row_bytes_applies_minimum_and_saturates() {
        let optimizer = Optimizer::new(&Options::default());
        let metadata = test_metadata();

        let small_stats = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(COLLECTION), 100),
            (CountStatsKey::CollectionDocumentBytes(COLLECTION), 100),
        ]));
        assert_eq!(
            optimizer.estimate_sort_row_bytes(&metadata, &small_stats),
            256
        );

        let huge_stats = CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(COLLECTION), 1),
            (CountStatsKey::CollectionDocumentBytes(COLLECTION), i64::MAX),
        ]));
        assert_eq!(
            optimizer.estimate_sort_row_bytes(&metadata, &huge_stats),
            usize::MAX
        );
    }

    #[test]
    fn test_optimize_external_merge_sort() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields.clone()))
            .build_arc();

        let output = PhysicalPlan::ExternalMergeSort {
            input: Arc::new(full_scan_plan()),
            sort_fields,
            max_in_memory_rows: 4096,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_limit_elimination() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["_id"]), vec![eq(lit(123))]))
            .limit(None, Some(10))
            .build_arc();

        // Limit is eliminated because point search returns at most one row, which
        // satisfies the limit of 10.
        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_range_scan_with_sort_and_limit() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["_id"]), vec![gt(lit(123))]))
            .sort(Some(sort_fields.clone()))
            .limit(Some(5), Some(10))
            .build_arc();

        let scan = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(QueryEndpoint::Value(placeholder(0))),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        let topk_sort = PhysicalPlan::TopKHeapSort {
            input: Arc::new(scan),
            sort_fields,
            k: 15,
        };

        let output = PhysicalPlan::Limit {
            input: Arc::new(topk_sort),
            skip: Some(5),
            limit: Some(10),
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_limit_elimination_with_heapsort() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields.clone()))
            .limit(None, Some(10))
            .build_arc();

        let output = PhysicalPlan::TopKHeapSort {
            input: Arc::new(full_scan_plan()),
            sort_fields,
            k: 10,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_multipoint_search() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let filters = field_filters(field(["_id"]), vec![within(lit(values))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: array_placeholder(0),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_multipoint_search_desc() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let filters = field_filters(field(["_id"]), vec![within(lit(values))]);
        let sort_fields = Arc::new(vec![SortField::desc(field(["_id"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .sort(Some(sort_fields))
            .build_arc();

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: array_placeholder(0),
            direction: Direction::Reverse,
            filter: None,
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_pk_multipoint_search_with_residual() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let filters = and(vec![
            field_filters(field(["_id"]), vec![within(lit(values))]),
            field_filters(field(["a"]), vec![gt(lit(10))]),
        ]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();

        let residual_filter = field_filters(
            field(["a"]),
            [interval(Interval::greater_than(QueryEndpoint::Value(
                placeholder(1),
            )))],
        );

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: array_placeholder(0),
            direction: Direction::Forward,
            filter: Some(residual_filter),
            projection: None,
        };

        check_optimization(input, output);
    }

    fn check_optimization(input: Arc<LogicalPlan>, output: PhysicalPlan) {
        check_optimization_with_metadata(input, &test_metadata(), output);
    }

    fn check_optimization_with_metadata(
        input: Arc<LogicalPlan>,
        metadata: &CollectionMetadata,
        output: PhysicalPlan,
    ) {
        check_optimization_with_metadata_and_stats(input, metadata, CountStats::default(), &output);
    }

    fn check_optimization_with_metadata_and_stats(
        input: Arc<LogicalPlan>,
        metadata: &CollectionMetadata,
        count_stats: CountStats,
        output: &PhysicalPlan,
    ) {
        let physical_plan = optimize_with_catalog_and_stats(input, metadata, count_stats)
            .expect("test logical plan should be optimizable");
        assert_eq!(physical_plan.as_ref(), output)
    }

    fn check_optimization_fail(
        input: Arc<LogicalPlan>,
        metadata: &CollectionMetadata,
        count_stats: CountStats,
        error: &str,
    ) {
        let result = optimize_with_catalog_and_stats(input, metadata, count_stats);
        if let Err(ref e) = result {
            assert_eq!(e.to_string(), error);
        } else {
            panic!("Expected optimization to fail, but it succeeded");
        }
    }

    fn optimize_with_catalog_and_stats(
        input: Arc<LogicalPlan>,
        metadata: &CollectionMetadata,
        count_stats: CountStats,
    ) -> Result<Arc<PhysicalPlan>> {
        let optimizer = Optimizer::new(&Options::default());
        // First, normalize the logical plan
        let normalized_plan = optimizer.normalize(input);
        // Then, parametrize the plan to collect parameters
        let (logical_plan, _parameters) = optimizer.parametrize(normalized_plan);
        optimizer.optimize(logical_plan, metadata, &count_stats)
    }

    fn index_stats(collection_count: i64, index_count: i64) -> CountStats {
        CountStats::new(BTreeMap::from([
            (CountStatsKey::Collection(COLLECTION), collection_count),
            (
                CountStatsKey::Index {
                    collection: COLLECTION,
                    index: 1,
                },
                index_count,
            ),
        ]))
    }

    fn full_scan_plan() -> PhysicalPlan {
        PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        }
    }

    fn test_metadata() -> Arc<CollectionMetadata> {
        CollectionBuilder::new(COLLECTION, "test", 2).build()
    }

    fn test_metadata_with_indexes() -> Arc<CollectionMetadata> {
        CollectionBuilder::new(COLLECTION, "test", 2)
            .index(IndexBuilder::new(3).asc("a").queryable_at(5))
            .index(IndexBuilder::new(4).asc("a").desc("b").queryable_at(6))
            .build()
    }

    fn test_metadata_with_multikey_index(multikey_fields: BitSet) -> Arc<CollectionMetadata> {
        CollectionBuilder::new(COLLECTION, "test", 2)
            .index(
                IndexBuilder::new(3)
                    .asc("a")
                    .asc("b")
                    .asc("c")
                    .asc("d")
                    .queryable_at(5)
                    .multikey_fields(multikey_fields),
            )
            .build()
    }

    fn multikey_fields(positions: &[u8]) -> BitSet {
        let mut fields = BitSet::new();
        for &position in positions {
            fields.insert(position);
        }
        fields
    }

    #[test]
    fn hinted_index_unbounded_scan_condition_is_explicit() {
        let index_fields = vec![OrderedIndexField::asc("a")];
        let field = field(["a"]);
        let array_filter = field_filters(
            field.clone(),
            [interval(Interval::closed(
                QueryEndpoint::Value(array_placeholder(0)),
                QueryEndpoint::Value(array_placeholder(0)),
            ))],
        );
        let scalar_filter = field_filters(
            field.clone(),
            [interval(Interval::closed(
                QueryEndpoint::Value(placeholder(0)),
                QueryEndpoint::Value(placeholder(0)),
            ))],
        );
        let array_filter_option = Some(array_filter.clone());
        let scalar_filter_option = Some(scalar_filter.clone());
        let array_filters = HashMap::from([(field.as_ref(), array_filter.as_ref())]);
        let scalar_filters = HashMap::from([(field.as_ref(), scalar_filter.as_ref())]);
        let req = ReqProps::default();

        assert!(hinted_index_can_use_unbounded_scan(
            true,
            &array_filter_option,
            &req,
            &index_fields,
            Some(&array_filters),
        ));
        assert!(!hinted_index_can_use_unbounded_scan(
            false,
            &array_filter_option,
            &req,
            &index_fields,
            Some(&array_filters),
        ));
        assert!(!hinted_index_can_use_unbounded_scan(
            true,
            &scalar_filter_option,
            &req,
            &index_fields,
            Some(&scalar_filters),
        ));
        assert!(hinted_index_can_use_unbounded_scan(
            true,
            &None,
            &req,
            &index_fields,
            None,
        ));
    }

    #[test]
    fn hinted_unbounded_scan_rejects_sort_alone() {
        let index_fields = vec![OrderedIndexField::asc("a")];
        for sort_field in [SortField::asc(field(["a"])), SortField::desc(field(["a"]))] {
            let req = ReqProps {
                order: Some(vec![sort_field]),
                ..ReqProps::default()
            };
            assert!(!hinted_index_can_use_unbounded_scan(
                false,
                &None,
                &req,
                &index_fields,
                None,
            ));
            assert!(!hinted_index_can_use_unbounded_scan(
                true,
                &None,
                &req,
                &index_fields,
                None,
            ));
        }

        let req = ReqProps {
            order: Some(vec![SortField::asc(field(["b"]))]),
            ..ReqProps::default()
        };
        assert!(!hinted_index_can_use_unbounded_scan(
            true,
            &None,
            &req,
            &index_fields,
            None,
        ));
    }

    #[test]
    fn select_index_interval_prefers_points_and_skips_array_bounds() {
        let array_interval = interval(Interval::closed(
            QueryEndpoint::Value(array_placeholder(0)),
            QueryEndpoint::Value(array_placeholder(0)),
        ));
        let range_interval = interval(Interval::greater_than(QueryEndpoint::Value(placeholder(1))));
        let point_interval = interval(Interval::closed(
            QueryEndpoint::Value(placeholder(2)),
            QueryEndpoint::Value(placeholder(2)),
        ));

        let all_intervals = [
            array_interval.clone(),
            range_interval.clone(),
            point_interval.clone(),
        ];
        let selected = select_index_interval(&all_intervals).unwrap();
        assert_eq!(selected.0, 2);
        assert!(selected.1.is_point());

        let intervals_without_point = [array_interval, range_interval];
        let selected_without_point = select_index_interval(&intervals_without_point).unwrap();
        assert_eq!(selected_without_point.0, 1);
        assert!(!selected_without_point.1.is_point());

        let array_only = interval(Interval::closed(
            QueryEndpoint::Value(array_placeholder(3)),
            QueryEndpoint::Value(array_placeholder(3)),
        ));
        assert!(select_index_interval(&[array_only]).is_none());
    }

    #[test]
    fn test_array_bound_retains_safe_prefix_and_residual() {
        for range in [false, true] {
            let array = lit(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]));
            let input = LogicalPlanBuilder::scan(COLLECTION, None)
                .filter(and([
                    field_filters(field(["a"]), vec![eq(lit(5))]),
                    field_filters(
                        field(["b"]),
                        vec![if range { gt(array) } else { eq(array) }],
                    ),
                ]))
                .build_arc();
            let output = PhysicalPlan::IndexScan {
                collection: COLLECTION,
                index: 1,
                range: IndexScanRangeExpr {
                    equal_prefix: vec![placeholder(0)],
                    tail: None,
                },
                direction: Direction::Forward,
                filter: Some(field_filters(
                    field(["b"]),
                    [interval(if range {
                        Interval::greater_than(QueryEndpoint::Value(array_placeholder(1)))
                    } else {
                        Interval::closed(
                            QueryEndpoint::Value(array_placeholder(1)),
                            QueryEndpoint::Value(array_placeholder(1)),
                        )
                    })],
                )),
                projection: None,
            };
            check_optimization_with_metadata(
                input,
                &test_metadata_with_multikey_index(multikey_fields(&[1])),
                output,
            );
        }
    }

    #[test]
    fn test_array_bound_falls_back_to_document_filter() {
        for hinted in [false, true] {
            let input = LogicalPlanBuilder::scan(COLLECTION, hinted.then_some(1))
                .filter(field_filters(
                    field(["a"]),
                    vec![eq(lit(Bson::Array(vec![])))],
                ))
                .build_arc();
            let filter = Some(field_filters(
                field(["a"]),
                [interval(Interval::closed(
                    QueryEndpoint::Value(array_placeholder(0)),
                    QueryEndpoint::Value(array_placeholder(0)),
                ))],
            ));
            let output = if hinted {
                PhysicalPlan::IndexScan {
                    collection: COLLECTION,
                    index: 1,
                    range: IndexScanRangeExpr {
                        equal_prefix: vec![],
                        tail: None,
                    },
                    direction: Direction::Forward,
                    filter,
                    projection: None,
                }
            } else {
                PhysicalPlan::CollectionScan {
                    collection: COLLECTION,
                    range: Interval::all(),
                    direction: Direction::Forward,
                    filter,
                    projection: None,
                }
            };
            check_optimization_with_metadata(
                input,
                &test_metadata_with_multikey_index(multikey_fields(&[0])),
                output,
            );
        }
    }

    #[test]
    fn test_array_bound_uses_alternate_scalar_predicate() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(
                field(["a"]),
                vec![eq(lit(Bson::Array(vec![Bson::Int32(1)]))), gt(lit(0))],
            ))
            .build_arc();
        // Normalization orders the scalar bound before the array equality.
        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: Some(Interval::greater_than(QueryEndpoint::Value(placeholder(0)))),
            },
            direction: Direction::Forward,
            filter: Some(field_filters(
                field(["a"]),
                [
                    interval(Interval::greater_than(QueryEndpoint::Value(placeholder(0)))),
                    interval(Interval::closed(
                        QueryEndpoint::Value(array_placeholder(1)),
                        QueryEndpoint::Value(array_placeholder(1)),
                    )),
                ],
            )),
            projection: None,
        };
        check_optimization_with_metadata(
            input,
            &test_metadata_with_multikey_index(multikey_fields(&[0])),
            output,
        );
    }

    #[test]
    fn test_parameter_kind_separates_plan_hashes() {
        let optimizer = Optimizer::new(&Options::default());
        let hash = |value: Bson| {
            let input = LogicalPlanBuilder::scan(COLLECTION, None)
                .filter(field_filters(field(["a"]), vec![eq(lit(value))]))
                .build_arc();
            let (plan, _) = optimizer.parametrize(optimizer.normalize(input));
            plan.compute_hash()
        };
        assert_eq!(hash(Bson::Int32(1)), hash(Bson::Int32(2)));
        assert_eq!(
            hash(Bson::Array(vec![])),
            hash(Bson::Array(vec![Bson::Int32(1)]))
        );
        assert_ne!(
            hash(Bson::Int32(1)),
            hash(Bson::Array(vec![Bson::Int32(1)]))
        );
    }

    #[test]
    fn test_multikey_sort_requires_document_sort() {
        for reverse in [false, true] {
            for bounded in [false, true] {
                for hinted in [false, true] {
                    for limited in [false, true] {
                        let sort_fields = Arc::new(vec![if reverse {
                            SortField::desc(field(["a"]))
                        } else {
                            SortField::asc(field(["a"]))
                        }]);
                        let mut input = LogicalPlanBuilder::scan(COLLECTION, hinted.then_some(1));
                        if bounded {
                            input = input.filter(field_filters(field(["a"]), vec![gt(lit(1))]));
                        }
                        input = input.sort(Some(sort_fields.clone()));
                        if limited {
                            input = input.limit(None, Some(2));
                        }
                        if hinted && !bounded {
                            check_optimization_fail(
                                input.build_arc(),
                                &test_metadata_with_multikey_index(multikey_fields(&[0])),
                                CountStats::default(),
                                "Cannot use hinted index 'a_1_b_1_c_1_d_1' on collection 'test': no candidate scan can use it for this query",
                            );
                            continue;
                        }
                        let scan = if bounded || hinted {
                            PhysicalPlan::IndexScan {
                                collection: COLLECTION,
                                index: 1,
                                range: IndexScanRangeExpr {
                                    equal_prefix: vec![],
                                    tail: bounded.then(|| {
                                        Interval::greater_than(QueryEndpoint::Value(placeholder(0)))
                                    }),
                                },
                                direction: Direction::Forward,
                                filter: bounded.then(|| {
                                    field_filters(
                                        field(["a"]),
                                        [interval(Interval::greater_than(QueryEndpoint::Value(
                                            placeholder(0),
                                        )))],
                                    )
                                }),
                                projection: None,
                            }
                        } else {
                            full_scan_plan()
                        };
                        let output = if limited {
                            PhysicalPlan::TopKHeapSort {
                                input: Arc::new(scan),
                                sort_fields,
                                k: 2,
                            }
                        } else {
                            PhysicalPlan::ExternalMergeSort {
                                input: Arc::new(scan),
                                sort_fields,
                                max_in_memory_rows: 4096,
                            }
                        };
                        check_optimization_with_metadata(
                            input.build_arc(),
                            &test_metadata_with_multikey_index(multikey_fields(&[0])),
                            output,
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_multikey_sort_preserves_only_scalar_prefix_order() {
        for reverse in [false, true] {
            for bounded in [false, true] {
                for include_multikey in [false, true] {
                    let fields = if include_multikey {
                        vec!["a", "b"]
                    } else {
                        vec!["a"]
                    };
                    let sort_fields = Arc::new(
                        fields
                            .into_iter()
                            .map(|name| {
                                if reverse {
                                    SortField::desc(field([name]))
                                } else {
                                    SortField::asc(field([name]))
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                    let mut input = LogicalPlanBuilder::scan(COLLECTION, Some(1));
                    if bounded {
                        input = input.filter(field_filters(field(["a"]), vec![gt(lit(1))]));
                    }
                    let input = input.sort(Some(sort_fields.clone())).build_arc();
                    if include_multikey && !bounded {
                        check_optimization_fail(
                            input,
                            &test_metadata_with_multikey_index(multikey_fields(&[1])),
                            CountStats::default(),
                            "Cannot use hinted index 'a_1_b_1_c_1_d_1' on collection 'test': no candidate scan can use it for this query",
                        );
                        continue;
                    }
                    let scan = PhysicalPlan::IndexScan {
                        collection: COLLECTION,
                        index: 1,
                        range: IndexScanRangeExpr {
                            equal_prefix: vec![],
                            tail: bounded.then(|| {
                                Interval::greater_than(QueryEndpoint::Value(placeholder(0)))
                            }),
                        },
                        direction: if reverse && !include_multikey {
                            Direction::Reverse
                        } else {
                            Direction::Forward
                        },
                        filter: None,
                        projection: None,
                    };
                    let output = if include_multikey {
                        PhysicalPlan::ExternalMergeSort {
                            input: Arc::new(scan),
                            sort_fields,
                            max_in_memory_rows: 4096,
                        }
                    } else {
                        scan
                    };
                    check_optimization_with_metadata(
                        input,
                        &test_metadata_with_multikey_index(multikey_fields(&[1])),
                        output,
                    );
                }
            }
        }
    }

    #[test]
    fn test_optimize_compound_index_without_multikey_fields_uses_all_bounds() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and([
                field_filters(field(["a"]), vec![eq(lit(1))]),
                field_filters(field(["b"]), vec![eq(lit(2))]),
                field_filters(field(["c"]), vec![eq(lit(3))]),
                field_filters(field(["d"]), vec![eq(lit(4))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![
                    placeholder(0),
                    placeholder(1),
                    placeholder(2),
                    placeholder(3),
                ],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(
            input,
            &test_metadata_with_multikey_index(BitSet::new()),
            output,
        );
    }

    #[test]
    fn test_optimize_compound_index_stops_before_second_multikey_field() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and([
                field_filters(field(["a"]), vec![eq(lit(1))]),
                field_filters(field(["b"]), vec![eq(lit(2))]),
                field_filters(field(["c"]), vec![eq(lit(3))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: None,
            },
            direction: Direction::Forward,
            filter: Some(and([
                field_filters(
                    field(["b"]),
                    [interval(Interval::closed(
                        QueryEndpoint::Value(placeholder(1)),
                        QueryEndpoint::Value(placeholder(1)),
                    ))],
                ),
                field_filters(
                    field(["c"]),
                    [interval(Interval::closed(
                        QueryEndpoint::Value(placeholder(2)),
                        QueryEndpoint::Value(placeholder(2)),
                    ))],
                ),
            ])),
            projection: None,
        };

        check_optimization_with_metadata(
            input,
            &test_metadata_with_multikey_index(multikey_fields(&[0, 1])),
            output,
        );
    }

    #[test]
    fn test_optimize_compound_index_uses_scalar_field_before_second_multikey_field() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and([
                field_filters(field(["a"]), vec![eq(lit(1))]),
                field_filters(field(["b"]), vec![eq(lit(2))]),
                field_filters(field(["c"]), vec![eq(lit(3))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0), placeholder(1)],
                tail: None,
            },
            direction: Direction::Forward,
            filter: Some(field_filters(
                field(["c"]),
                [interval(Interval::closed(
                    QueryEndpoint::Value(placeholder(2)),
                    QueryEndpoint::Value(placeholder(2)),
                ))],
            )),
            projection: None,
        };

        check_optimization_with_metadata(
            input,
            &test_metadata_with_multikey_index(multikey_fields(&[0, 2])),
            output,
        );
    }

    #[test]
    fn test_optimize_compound_index_allows_scalar_fields_after_one_multikey_field() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and([
                field_filters(field(["a"]), vec![eq(lit(1))]),
                field_filters(field(["b"]), vec![eq(lit(2))]),
                field_filters(field(["c"]), vec![eq(lit(3))]),
                field_filters(field(["d"]), vec![eq(lit(4))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![
                    placeholder(0),
                    placeholder(1),
                    placeholder(2),
                    placeholder(3),
                ],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(
            input,
            &test_metadata_with_multikey_index(multikey_fields(&[1])),
            output,
        );
    }

    #[test]
    fn test_optimize_compound_index_keeps_fields_after_multikey_range_residual() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and([
                field_filters(field(["a"]), vec![eq(lit(1))]),
                field_filters(field(["b"]), vec![gt(lit(2))]),
                field_filters(field(["c"]), vec![eq(lit(3))]),
                field_filters(field(["d"]), vec![eq(lit(4))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: Some(Interval::greater_than(QueryEndpoint::Value(placeholder(1)))),
            },
            direction: Direction::Forward,
            filter: Some(and([
                field_filters(
                    field(["b"]),
                    [interval(Interval::greater_than(QueryEndpoint::Value(
                        placeholder(1),
                    )))],
                ),
                field_filters(
                    field(["c"]),
                    [interval(Interval::closed(
                        QueryEndpoint::Value(placeholder(2)),
                        QueryEndpoint::Value(placeholder(2)),
                    ))],
                ),
                field_filters(
                    field(["d"]),
                    [interval(Interval::closed(
                        QueryEndpoint::Value(placeholder(3)),
                        QueryEndpoint::Value(placeholder(3)),
                    ))],
                ),
            ])),
            projection: None,
        };

        check_optimization_with_metadata(
            input,
            &test_metadata_with_multikey_index(multikey_fields(&[1, 3])),
            output,
        );
    }

    #[test]
    fn test_optimize_sort_elimination_single_field_index() {
        let sort_fields = Arc::new(vec![SortField::asc(field(["a"]))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_sort_elimination_compound_index_prefix() {
        let sort_fields = Arc::new(vec![
            SortField::asc(field(["a"])),
            SortField::desc(field(["b"])),
        ]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 2,
            range: IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_sort_elimination_compound_index_prefix_reverse() {
        let sort_fields = Arc::new(vec![
            SortField::desc(field(["a"])),
            SortField::asc(field(["b"])),
        ]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .sort(Some(sort_fields))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 2,
            range: IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: None,
            },
            direction: Direction::Reverse,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_single_field_index_equality_uses_index_scan_range() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_compound_index_prefix_plus_tail_range_uses_index_scan() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and(vec![
                field_filters(field(["a"]), vec![eq(lit(10))]),
                field_filters(field(["b"]), vec![gt(lit(20))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 2,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: Some(Interval::greater_than(QueryEndpoint::Value(placeholder(1)))),
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_prefers_more_selective_compound_index_for_two_field_equality() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(and(vec![
                field_filters(field(["a"]), vec![eq(lit(10))]),
                field_filters(field(["b"]), vec![eq(lit(20))]),
            ]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 2,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0), placeholder(1)],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata(input, &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_non_indexed_field_still_uses_collection_scan() {
        let filters = field_filters(field(["z"]), vec![eq(lit(99))]);
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(filters)
            .build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: Some(field_filters(
                field(["z"]),
                [interval(Interval::closed(
                    QueryEndpoint::Value(placeholder(0)),
                    QueryEndpoint::Value(placeholder(0)),
                ))],
            )),
            projection: None,
        };

        check_optimization(input, output);
    }

    #[test]
    fn test_optimize_prefers_index_scan_when_count_stats_favor_index() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata_and_stats(
            input,
            &test_metadata_with_indexes(),
            index_stats(10_000, 100),
            &output,
        );
    }

    #[test]
    fn test_optimize_forces_hinted_index_scan() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(1))
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![placeholder(0)],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };

        check_optimization_with_metadata_and_stats(
            input.clone(),
            &test_metadata_with_indexes(),
            index_stats(10_000, 1),
            &output,
        );

        check_optimization_with_metadata_and_stats(
            input,
            &test_metadata_with_indexes(),
            index_stats(10, 1000),
            &output,
        );
    }

    #[test]
    fn test_optimize_allows_full_scan_of_hinted_index() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(1)).build_arc();

        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };
        check_optimization_with_metadata(input.clone(), &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_forces_collection_scan() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(0))
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: Some(field_filters(
                field(["a"]),
                [interval(Interval::closed(
                    QueryEndpoint::Value(placeholder(0)),
                    QueryEndpoint::Value(placeholder(0)),
                ))],
            )),
            projection: None,
        };

        check_optimization_with_metadata_and_stats(
            input.clone(),
            &test_metadata_with_indexes(),
            index_stats(10_000, 1),
            &output,
        );
    }

    #[test]
    fn test_collection_scan_hint_excludes_secondary_indexes_without_filter() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(0)).build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };
        check_optimization_with_metadata_and_stats(
            input.clone(),
            &test_metadata_with_indexes(),
            index_stats(10_000, 1),
            &output,
        );
    }

    #[test]
    fn test_collection_scan_hint_allows_primary_key_range_scan() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(0))
            .filter(field_filters(field(["_id"]), vec![gt(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::greater_than(QueryEndpoint::Value(placeholder(0))),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };
        check_optimization_with_metadata(input.clone(), &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_collection_scan_hint_allows_primary_key_multipoint_search() {
        let values = Bson::Array(vec![Bson::Int32(10), Bson::Int32(20)]);
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(0))
            .filter(field_filters(field(["_id"]), vec![within(lit(values))]))
            .build_arc();

        let output = PhysicalPlan::MultiPointSearch {
            collection: COLLECTION,
            keys: array_placeholder(0),
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };
        check_optimization_with_metadata(input.clone(), &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_collection_scan_hint_requires_sorting_after_scan() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(0))
            .sort(Some(Arc::new(vec![SortField::asc(field(["a"]))])))
            .build_arc();

        let output = PhysicalPlan::ExternalMergeSort {
            input: Arc::new(PhysicalPlan::CollectionScan {
                collection: COLLECTION,
                range: Interval::all(),
                direction: Direction::Forward,
                filter: None,
                projection: None,
            }),
            sort_fields: Arc::new(vec![SortField::asc(field(["a"]))]),
            max_in_memory_rows: 4096,
        };
        check_optimization_with_metadata(input.clone(), &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_hinted_index_can_provide_sort_without_filter() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(1))
            .sort(Some(Arc::new(vec![SortField::asc(field(["a"]))])))
            .build_arc();
        let output = PhysicalPlan::IndexScan {
            collection: COLLECTION,
            index: 1,
            range: IndexScanRangeExpr {
                equal_prefix: vec![],
                tail: None,
            },
            direction: Direction::Forward,
            filter: None,
            projection: None,
        };
        check_optimization_with_metadata(input.clone(), &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_collection_scan_hint_allows_primary_key_lookup() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(0))
            .filter(field_filters(field(["_id"]), vec![eq(lit(10))]))
            .build_arc();
        let output = PhysicalPlan::PointSearch {
            collection: COLLECTION,
            key: placeholder(0),
            filter: None,
            projection: None,
        };
        check_optimization_with_metadata(input.clone(), &test_metadata_with_indexes(), output);
    }

    #[test]
    fn test_optimize_rejects_hinted_index_that_cannot_serve_filter() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(1))
            .filter(field_filters(field(["z"]), vec![eq(lit(10))]))
            .build_arc();

        check_optimization_fail(
            input,
            &test_metadata_with_indexes(),
            CountStats::default(),
            "Cannot use hinted index 'a_1' on collection 'test': no candidate scan can use it for this query",
        )
    }

    #[test]
    fn test_optimize_rejects_unqueryable_hinted_index() {
        let metadata = CollectionBuilder::new(COLLECTION, "test", 2)
            .index(IndexBuilder::new(3).asc("a"))
            .build();
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(1)).build_arc();
        check_optimization_fail(
            input.clone(),
            &metadata,
            CountStats::default(),
            "Cannot use hinted index 'a_1' on collection 'test': the index is not queryable",
        );
    }

    #[test]
    fn test_optimize_rejects_missing_hinted_index() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(999)).build_arc();
        let result = optimize_with_catalog_and_stats(
            input,
            &test_metadata_with_indexes(),
            CountStats::default(),
        );

        assert!(matches!(
            result,
            Err(Error::IndexNotFound {
                collection_name,
                index_name,
                id: Some(999),
            }) if collection_name == "test" && index_name == "index id 999"
        ));
    }

    #[test]
    fn test_optimize_rejects_secondary_hint_for_primary_key_lookup() {
        let input = LogicalPlanBuilder::scan(COLLECTION, Some(1))
            .filter(field_filters(field(["_id"]), vec![eq(lit(10))]))
            .build_arc();

        check_optimization_fail(
            input,
            &test_metadata_with_indexes(),
            CountStats::default(),
            "Cannot use hinted index 'a_1' on collection 'test': no candidate scan can use it for this query",
        );
    }

    #[test]
    fn test_optimize_prefers_collection_scan_when_count_stats_favor_collection() {
        let input = LogicalPlanBuilder::scan(COLLECTION, None)
            .filter(field_filters(field(["a"]), vec![eq(lit(10))]))
            .build_arc();

        let output = PhysicalPlan::CollectionScan {
            collection: COLLECTION,
            range: Interval::all(),
            direction: Direction::Forward,
            filter: Some(field_filters(
                field(["a"]),
                [interval(Interval::closed(
                    QueryEndpoint::Value(placeholder(0)),
                    QueryEndpoint::Value(placeholder(0)),
                ))],
            )),
            projection: None,
        };

        check_optimization_with_metadata_and_stats(
            input,
            &test_metadata_with_indexes(),
            index_stats(10, 10_000),
            &output,
        );
    }
}
