use super::executor::{ExecutionOutput, ExecutionRow};
use crate::error::Result;
use crate::obs::metrics::Counter;
use crate::query::{BsonValueRef, Expr, SortField, SortOrder, get_path_value};
use bson::{Bson, Document};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use tempfile::{NamedTempFile, TempDir, tempdir};

#[derive(Clone)]
pub(crate) struct SpillMetrics {
    pub(crate) spills: Arc<Counter>,
    pub(crate) spill_bytes: Arc<Counter>,
}

impl Default for SpillMetrics {
    fn default() -> Self {
        Self {
            spills: Counter::new(),
            spill_bytes: Counter::new(),
        }
    }
}

/// Compares two BSON documents according to the provided sort fields.
/// Returns an `Ordering` based on multi-key, multi-order comparison.
pub fn compare_documents(a: &Document, b: &Document, sort_fields: &[SortField]) -> Ordering {
    for sf in sort_fields {
        if let Expr::Field(path) = sf.field.as_ref() {
            let val_a = get_path_value(a, path).unwrap_or(BsonValueRef(&Bson::Null));
            let val_b = get_path_value(b, path).unwrap_or(BsonValueRef(&Bson::Null));
            match val_a.cmp(&val_b) {
                Ordering::Equal => continue,
                ord => {
                    return if sf.order == SortOrder::Ascending {
                        ord
                    } else {
                        ord.reverse()
                    };
                }
            }
        }
    }
    Ordering::Equal
}

/// Performs an external merge sort for large datasets that exceed memory limits.
/// Sorts entirely in memory when the input fits within `max_in_memory_rows`.
/// Otherwise, uses temporary files to store sorted runs and merges them.
/// Returns an iterator over sorted rows, preserving their source BSON sizes.
pub fn external_merge_sort(
    input: ExecutionOutput,
    sort_fields: Arc<Vec<SortField>>,
    max_in_memory_rows: usize,
    metrics: SpillMetrics,
) -> Result<ExecutionOutput> {
    external_merge_sort_with_temp_dir(input, sort_fields, max_in_memory_rows, metrics, tempdir)
}

fn external_merge_sort_with_temp_dir(
    input: ExecutionOutput,
    sort_fields: Arc<Vec<SortField>>,
    max_in_memory_rows: usize,
    metrics: SpillMetrics,
    create_temp_dir: impl FnMut() -> std::io::Result<TempDir>,
) -> Result<ExecutionOutput> {
    let mut input = input;
    let mut create_temp_dir = create_temp_dir;
    assert!(
        max_in_memory_rows > 0,
        "max_in_memory_rows must be positive"
    );
    let mut runs = Vec::new();
    let mut spill_bytes = 0;
    let mut temp_dir = None;
    let mut input_drained = false;
    let mut pending = None;

    while !input_drained {
        let mut chunk: Vec<ExecutionRow> = Vec::with_capacity(max_in_memory_rows);
        if let Some(doc) = pending.take() {
            chunk.push(doc);
        }
        for _ in chunk.len()..max_in_memory_rows {
            match input.next() {
                Some(Ok(doc)) => chunk.push(doc),
                Some(Err(e)) => return Err(e),
                None => {
                    input_drained = true;
                    break;
                }
            }
        }

        if temp_dir.is_none() {
            // Look ahead so an input exactly at the threshold stays in memory.
            if !input_drained {
                pending = input.next().transpose()?;
            }
            if pending.is_none() {
                chunk.sort_by(|a, b| compare_documents(&a.document, &b.document, &sort_fields));
                return Ok(Box::new(chunk.into_iter().map(Ok)));
            }
            temp_dir = Some(create_temp_dir()?);
        }

        if !chunk.is_empty() {
            chunk.sort_by(|a, b| compare_documents(&a.document, &b.document, &sort_fields));

            let run_file = tempfile::Builder::new().tempfile_in(temp_dir.as_ref().unwrap())?;
            let mut writer = BufWriter::new(run_file);
            let document_count = chunk.len();
            for doc in chunk {
                write_spilled_row(&mut writer, &doc)?;
            }
            let mut file = writer.into_inner().map_err(|e| e.into_error())?;
            file.seek(SeekFrom::Start(0))?;
            spill_bytes += file.as_file().metadata()?.len();
            runs.push(SpillRun {
                file,
                document_count,
            });
        }
    }

    if runs.is_empty() {
        Ok(Box::new(std::iter::empty()))
    } else {
        let merge_iterator = MergeIterator::new(runs, sort_fields, temp_dir.unwrap())?;
        metrics.spills.inc();
        metrics.spill_bytes.inc_by(spill_bytes);
        Ok(Box::new(merge_iterator))
    }
}

/// Efficiently retrieves the top-k smallest documents according to sort fields using a heap.
/// Returns an iterator over the k sorted rows, preserving their source BSON sizes.
pub fn top_k_heap_sort(
    input: ExecutionOutput,
    sort_fields: Arc<Vec<SortField>>,
    k: usize,
) -> Result<ExecutionOutput> {
    if k == 0 {
        return Ok(Box::new(std::iter::empty()));
    }

    // Wrapper for ExecutionRow to implement Ord for the max-heap.
    struct HeapDoc {
        doc: ExecutionRow,
        sort_fields: Arc<Vec<SortField>>,
    }

    impl PartialEq for HeapDoc {
        fn eq(&self, other: &Self) -> bool {
            compare_documents(&self.doc.document, &other.doc.document, &self.sort_fields)
                == Ordering::Equal
        }
    }

    impl Eq for HeapDoc {}

    impl Ord for HeapDoc {
        fn cmp(&self, other: &Self) -> Ordering {
            compare_documents(&self.doc.document, &other.doc.document, &self.sort_fields)
        }
    }

    impl PartialOrd for HeapDoc {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut heap: BinaryHeap<HeapDoc> = BinaryHeap::with_capacity(k + 1);

    for item_res in input {
        let doc = item_res?;
        let heap_doc = HeapDoc {
            doc,
            sort_fields: sort_fields.clone(),
        };

        if heap.len() < k {
            heap.push(heap_doc);
        } else if &heap_doc < heap.peek().unwrap() {
            // The new document is smaller than the largest in the heap (max-heap).
            // Replace the largest with this new one. `peek_mut` is efficient for this.
            // unwrap() is safe because heap.len() >= k and k > 0.
            let mut max_in_heap = heap.peek_mut().unwrap();
            *max_in_heap = heap_doc;
        }
    }

    // The heap now contains the k smallest elements, but not in sorted order.
    // `into_sorted_vec` consumes the heap and returns a sorted vec (smallest to largest).
    let sorted_docs: Vec<ExecutionRow> = heap
        .into_sorted_vec()
        .into_iter()
        .map(|hd| hd.doc)
        .collect();
    Ok(Box::new(sorted_docs.into_iter().map(Ok)))
}

/// `MergeIterator` merges multiple sorted runs of BSON documents, typically produced by external merge sort.
/// It maintains a min-heap to efficiently select the next smallest document across all runs.
/// The iterator yields sorted documents in order, reading from temporary files as needed.
struct MergeIterator {
    /// Min-heap of the next document from each run.
    heap: BinaryHeap<HeapItem>,
    /// Buffered readers for each run's temporary file.
    readers: Vec<BufReader<NamedTempFile>>,
    /// Number of documents still expected from each run.
    remaining_documents: Vec<usize>,
    /// Temporary directory for cleanup on drop.
    _temp_dir: TempDir, // kept for its Drop side effect
}

struct SpillRun {
    file: NamedTempFile,
    document_count: usize,
}

struct HeapItem {
    doc: ExecutionRow,
    run_index: usize,
    sort_fields: Arc<Vec<SortField>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        compare_documents(&self.doc.document, &other.doc.document, &self.sort_fields)
            == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior in BinaryHeap (which is a max-heap)
        compare_documents(&other.doc.document, &self.doc.document, &self.sort_fields)
    }
}

impl MergeIterator {
    /// Creates a new `MergeIterator` from sorted runs, sort fields, and a temp directory.
    /// Loads the first document from each run into the heap.
    fn new(
        runs: Vec<SpillRun>,
        sort_fields: Arc<Vec<SortField>>,
        temp_dir: TempDir,
    ) -> Result<Self> {
        let mut remaining_documents: Vec<usize> =
            runs.iter().map(|run| run.document_count).collect();
        let mut readers = runs
            .into_iter()
            .map(|run| BufReader::new(run.file))
            .collect::<Vec<_>>();
        let mut heap = BinaryHeap::new();

        for (i, reader) in readers.iter_mut().enumerate() {
            if remaining_documents[i] == 0 {
                continue;
            }

            let doc = read_spilled_row(reader)?;
            remaining_documents[i] -= 1;
            heap.push(HeapItem {
                doc,
                run_index: i,
                sort_fields: sort_fields.clone(),
            });
        }

        Ok(MergeIterator {
            heap,
            readers,
            remaining_documents,
            _temp_dir: temp_dir,
        })
    }
}

impl Iterator for MergeIterator {
    type Item = Result<ExecutionRow>;

    /// Returns the next sorted document by popping the smallest item from the heap
    /// and loading the next document from the corresponding run.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(smallest) = self.heap.pop() {
            let run_index = smallest.run_index;
            if self.remaining_documents[run_index] > 0 {
                match read_spilled_row(&mut self.readers[run_index]) {
                    Ok(doc) => {
                        self.remaining_documents[run_index] -= 1;
                        self.heap.push(HeapItem {
                            doc,
                            run_index,
                            sort_fields: smallest.sort_fields.clone(),
                        });
                    }
                    Err(e) => return Some(Err(e.into())),
                }
            }
            Some(Ok(smallest.doc))
        } else {
            None // Heap is empty, we are done.
        }
    }
}

// Preserve the original size independently of the possibly projected BSON document.
fn write_spilled_row(writer: &mut impl Write, row: &ExecutionRow) -> Result<()> {
    let source_size =
        u64::try_from(row.source_bson_size).expect("Source BSON size must fit in u64");
    writer.write_all(&source_size.to_le_bytes())?;
    row.document.to_writer(writer)?;
    Ok(())
}

fn read_spilled_row(reader: &mut impl Read) -> Result<ExecutionRow> {
    let mut size = [0; 8];
    reader.read_exact(&mut size)?;
    let source_bson_size = usize::try_from(u64::from_le_bytes(size))
        .expect("Spilled source BSON size must fit in usize");
    Ok(ExecutionRow {
        document: Document::from_reader(reader)?,
        source_bson_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{SortField, SortOrder, make_sort_field};
    use bson::doc;
    use std::cmp::Ordering;
    use std::io::{Seek, Write};
    use std::sync::Arc;

    fn test_row(document: Document) -> ExecutionRow {
        let source_bson_size = document.to_vec().unwrap().len();
        ExecutionRow {
            document,
            source_bson_size,
        }
    }

    #[test]
    fn test_sort_documents() {
        let doc1 = doc! { "a": 1, "b": "xyz", "c": { "d": 10 } };
        let doc2 = doc! { "a": 2, "b": "abc", "c": { "d": 20 } };
        let doc3 = doc! { "a": 2, "b": "xyz", "c": { "d": 5 } };

        // Sort by 'a' ascending
        let sort_fields = vec![make_sort_field(vec!["a".into()], SortOrder::Ascending)];
        assert_eq!(
            compare_documents(&doc1, &doc2, &sort_fields),
            Ordering::Less
        );
        assert_eq!(
            compare_documents(&doc2, &doc1, &sort_fields),
            Ordering::Greater
        );
        assert_eq!(
            compare_documents(&doc2, &doc3, &sort_fields),
            Ordering::Equal
        );

        // Sort by 'a' descending
        let sort_fields = vec![make_sort_field(vec!["a".into()], SortOrder::Descending)];
        assert_eq!(
            compare_documents(&doc1, &doc2, &sort_fields),
            Ordering::Greater
        );
        assert_eq!(
            compare_documents(&doc2, &doc1, &sort_fields),
            Ordering::Less
        );

        // Sort by 'b' ascending
        let sort_fields = vec![make_sort_field(vec!["b".into()], SortOrder::Ascending)];
        assert_eq!(
            compare_documents(&doc1, &doc2, &sort_fields),
            Ordering::Greater
        ); // "xyz" > "abc"
        assert_eq!(
            compare_documents(&doc2, &doc1, &sort_fields),
            Ordering::Less
        );

        // Multi-key sort: 'a' asc, then 'b' asc
        let sort_fields = vec![
            make_sort_field(vec!["a".into()], SortOrder::Ascending),
            make_sort_field(vec!["b".into()], SortOrder::Ascending),
        ];
        assert_eq!(
            compare_documents(&doc2, &doc3, &sort_fields),
            Ordering::Less
        );

        // Multi-key sort: 'a' asc, then 'c.d' desc
        let sort_fields = vec![
            make_sort_field(vec!["a".into()], SortOrder::Ascending),
            make_sort_field(vec!["c".into(), "d".into()], SortOrder::Descending),
        ];
        assert_eq!(
            compare_documents(&doc2, &doc3, &sort_fields),
            Ordering::Less
        );

        // Sort on nested key
        let sort_fields = vec![make_sort_field(
            vec!["c".into(), "d".into()],
            SortOrder::Ascending,
        )];
        assert_eq!(
            compare_documents(&doc1, &doc2, &sort_fields),
            Ordering::Less
        ); // 10 < 20
        assert_eq!(
            compare_documents(&doc3, &doc1, &sort_fields),
            Ordering::Less
        ); // 5 < 10

        // Field missing in one doc
        let doc4 = doc! { "b": "only b" };
        let sort_fields = vec![make_sort_field(vec!["a".into()], SortOrder::Ascending)];
        assert_eq!(
            compare_documents(&doc1, &doc4, &sort_fields),
            Ordering::Greater
        );
        assert_eq!(
            compare_documents(&doc4, &doc1, &sort_fields),
            Ordering::Less
        );

        // No sort fields
        assert_eq!(compare_documents(&doc1, &doc2, &[]), Ordering::Equal);
    }

    fn run_sort_test<F>(mut sort_fn: F, k: Option<usize>)
    where
        F: FnMut(ExecutionOutput, Arc<Vec<SortField>>) -> Result<ExecutionOutput>,
    {
        let docs = vec![
            doc! { "_id": 1, "name": "c", "value": 10.0 },
            doc! { "_id": 2, "name": "a", "value": 30.0 },
            doc! { "_id": 3, "name": "b", "value": 20.0 },
            doc! { "_id": 4, "name": "a", "value": 10.0 },
            doc! { "_id": 5, "name": "c", "value": 5.0 },
        ];

        // Ascending sort
        let sort_fields_asc = Arc::new(vec![
            make_sort_field(vec!["name".into()], SortOrder::Ascending),
            make_sort_field(vec!["value".into()], SortOrder::Ascending),
        ]);
        let input_iter_asc = Box::new(docs.clone().into_iter().map(|doc| Ok(test_row(doc))));
        let sorted_iter_asc = sort_fn(input_iter_asc, sort_fields_asc.clone()).unwrap();
        let sorted_docs_asc: Vec<Document> =
            sorted_iter_asc.map(|row| row.unwrap().document).collect();
        let ids_asc: Vec<i32> = sorted_docs_asc
            .iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect();
        let mut expected_ids_asc = vec![4, 2, 3, 5, 1];
        if let Some(k_val) = k {
            expected_ids_asc.truncate(k_val);
        }
        assert_eq!(ids_asc, expected_ids_asc);

        // Descending sort
        let sort_fields_desc = Arc::new(vec![
            make_sort_field(vec!["name".into()], SortOrder::Descending),
            make_sort_field(vec!["value".into()], SortOrder::Descending),
        ]);
        let input_iter_desc = Box::new(docs.clone().into_iter().map(|doc| Ok(test_row(doc))));
        let sorted_iter_desc = sort_fn(input_iter_desc, sort_fields_desc).unwrap();
        let sorted_docs_desc: Vec<Document> =
            sorted_iter_desc.map(|row| row.unwrap().document).collect();
        let ids_desc: Vec<i32> = sorted_docs_desc
            .iter()
            .map(|d| d.get_i32("_id").unwrap())
            .collect();
        let mut expected_ids_desc = vec![1, 5, 3, 2, 4];
        if let Some(k_val) = k {
            expected_ids_desc.truncate(k_val);
        }
        assert_eq!(ids_desc, expected_ids_desc);

        // Edge case: Empty input
        let docs_empty: Vec<Document> = vec![];
        let input_iter_empty = Box::new(docs_empty.into_iter().map(|doc| Ok(test_row(doc))));
        let sorted_iter_empty = sort_fn(input_iter_empty, sort_fields_asc.clone()).unwrap();
        assert_eq!(sorted_iter_empty.count(), 0);

        // Edge case: Single document
        let docs_single = vec![doc! { "_id": 1 }];
        let input_iter_single =
            Box::new(docs_single.clone().into_iter().map(|doc| Ok(test_row(doc))));
        let sorted_iter_single = sort_fn(input_iter_single, sort_fields_asc).unwrap();
        let sorted_docs_single: Vec<Document> = sorted_iter_single
            .map(|row| row.unwrap().document)
            .collect();
        let expected_len = if let Some(k_val) = k {
            std::cmp::min(k_val, docs_single.len())
        } else {
            docs_single.len()
        };
        assert_eq!(sorted_docs_single.len(), expected_len);
        if !sorted_docs_single.is_empty() {
            assert_eq!(sorted_docs_single[0].get_i32("_id").unwrap(), 1);
        }
    }

    #[test]
    fn test_external_merge_sort() {
        for threshold in [1, 2, 4, 5, 6] {
            run_sort_test(
                |iter, fields| {
                    external_merge_sort(iter, fields, threshold, SpillMetrics::default())
                },
                None,
            );
        }
    }

    #[test]
    fn test_external_merge_sort_without_temp_files() {
        for threshold in [5, 6] {
            run_sort_test(
                |iter, fields| {
                    external_merge_sort_with_temp_dir(
                        iter,
                        fields,
                        threshold,
                        SpillMetrics::default(),
                        || panic!("fitting input must not create a temporary directory"),
                    )
                },
                None,
            );
        }
    }

    #[test]
    fn test_external_merge_sort_records_actual_spilling() {
        let docs = (0..5).map(|id| Ok(test_row(doc! { "_id": id })));
        let fields = Arc::new(vec![make_sort_field(
            vec!["_id".into()],
            SortOrder::Ascending,
        )]);
        let spills = Counter::new();
        let spill_bytes = Counter::new();

        let output = external_merge_sort(
            Box::new(docs),
            fields.clone(),
            5,
            SpillMetrics {
                spills: spills.clone(),
                spill_bytes: spill_bytes.clone(),
            },
        )
        .unwrap();
        assert_eq!(output.count(), 5);
        assert_eq!(spills.get(), 0);
        assert_eq!(spill_bytes.get(), 0);

        let docs = (0..5).map(|id| Ok(test_row(doc! { "_id": id })));
        let output = external_merge_sort(
            Box::new(docs),
            fields,
            2,
            SpillMetrics {
                spills: spills.clone(),
                spill_bytes: spill_bytes.clone(),
            },
        )
        .unwrap();
        assert_eq!(output.count(), 5);
        assert_eq!(spills.get(), 1);
        assert!(spill_bytes.get() > 0);
    }

    #[test]
    fn test_external_merge_sort_input_errors() {
        // Fail during buffering, lookahead, and after the first spill.
        for successful_rows in [0, 1, 2, 3] {
            let input = (0..successful_rows)
                .map(|id| Ok(test_row(doc! { "_id": id })))
                .chain(std::iter::once(Err(crate::error::Error::InvalidRequest(
                    "input failed".into(),
                ))));
            let mut created_temp_dir = false;
            let result = external_merge_sort_with_temp_dir(
                Box::new(input),
                Arc::new(vec![make_sort_field(
                    vec!["_id".into()],
                    SortOrder::Ascending,
                )]),
                2,
                SpillMetrics::default(),
                || {
                    created_temp_dir = true;
                    tempdir()
                },
            );
            assert!(
                matches!(result, Err(crate::error::Error::InvalidRequest(message)) if message == "input failed")
            );
            assert_eq!(created_temp_dir, successful_rows > 2);
        }
    }

    #[test]
    fn test_merge_iterator_rejects_truncated_source_size() {
        for prefix_len in 0..8 {
            for complete_rows in 0..=1 {
                let temp_dir = tempdir().unwrap();
                let mut file = tempfile::Builder::new().tempfile_in(&temp_dir).unwrap();
                if complete_rows == 1 {
                    write_spilled_row(&mut file, &test_row(doc! { "_id": 1 })).unwrap();
                }
                file.write_all(&42_u64.to_le_bytes()[..prefix_len]).unwrap();
                file.rewind().unwrap();
                let result = MergeIterator::new(
                    vec![SpillRun {
                        file,
                        document_count: complete_rows + 1,
                    }],
                    Arc::new(vec![]),
                    temp_dir,
                );
                if complete_rows == 0 {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().next().unwrap().is_err());
                }
            }
        }
    }

    #[test]
    fn test_merge_iterator_rejects_truncated_first_document() {
        let temp_dir = tempdir().unwrap();
        let mut file = tempfile::Builder::new().tempfile_in(&temp_dir).unwrap();
        file.write_all(&42_u64.to_le_bytes()).unwrap();
        file.write_all(&[5, 0, 0]).unwrap();

        let result = MergeIterator::new(
            vec![SpillRun {
                file,
                document_count: 1,
            }],
            Arc::new(vec![]),
            temp_dir,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_merge_iterator_rejects_truncated_later_document() {
        let temp_dir = tempdir().unwrap();
        let mut file = tempfile::Builder::new().tempfile_in(&temp_dir).unwrap();
        write_spilled_row(&mut file, &test_row(doc! { "_id": 1 })).unwrap();
        file.write_all(&42_u64.to_le_bytes()).unwrap();
        file.write_all(&[5, 0, 0]).unwrap();
        file.rewind().unwrap();

        let mut iterator = MergeIterator::new(
            vec![SpillRun {
                file,
                document_count: 2,
            }],
            Arc::new(vec![]),
            temp_dir,
        )
        .unwrap();

        assert!(iterator.next().unwrap().is_err());
    }

    #[test]
    fn test_top_k_heap_sort() {
        // k=3, standard case
        run_sort_test(
            |iter, fields| top_k_heap_sort(iter, fields.clone(), 3),
            Some(3),
        );

        // k=0 edge case
        run_sort_test(
            |iter, fields| top_k_heap_sort(iter, fields.clone(), 0),
            Some(0),
        );

        // k > number of docs edge case
        run_sort_test(
            |iter, fields| top_k_heap_sort(iter, fields.clone(), 10),
            Some(10),
        );
    }
}
