use crate::error::Result;
use crate::query::logical_plan::{SortField, SortOrder};
use crate::query::{get_path_value, BsonValueRef, Expr};
use bson::{Bson, Document};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufReader, BufWriter, ErrorKind, Seek, SeekFrom};
use std::sync::Arc;
use tempfile::{tempdir, NamedTempFile, TempDir};

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

/// Sorts all documents in memory using the specified sort fields.
/// Returns an iterator over sorted documents.
pub fn in_memory_sort(
    input_iter: Box<dyn Iterator<Item = Result<Document>>>,
    sort_fields: &Arc<Vec<SortField>>,
) -> Result<Box<dyn Iterator<Item = Result<Document>>>> {
    let mut rows: Vec<Document> = input_iter.collect::<Result<Vec<_>>>()?;
    rows.sort_by(|a, b| compare_documents(a, b, sort_fields));
    Ok(Box::new(rows.into_iter().map(Ok)))
}

/// Performs an external merge sort for large datasets that exceed memory limits.
/// Uses temporary files to store sorted runs and merges them.
/// Returns an iterator over sorted documents.
pub fn external_merge_sort(
    mut input: Box<dyn Iterator<Item = Result<Document>>>,
    sort_fields: Arc<Vec<SortField>>,
    max_in_memory_rows: usize,
) -> Result<Box<dyn Iterator<Item = Result<Document>>>> {
    let mut runs = Vec::new();
    let temp_dir = tempdir()?;
    let mut input_drained = false;

    while !input_drained {
        let mut chunk: Vec<Document> = Vec::with_capacity(max_in_memory_rows);
        for _ in 0..max_in_memory_rows {
            match input.next() {
                Some(Ok(doc)) => chunk.push(doc),
                Some(Err(e)) => return Err(e),
                None => {
                    input_drained = true;
                    break;
                }
            }
        }

        if !chunk.is_empty() {
            chunk.sort_by(|a, b| compare_documents(a, b, &sort_fields));

            let run_file = tempfile::Builder::new().tempfile_in(&temp_dir)?;
            let mut writer = BufWriter::new(run_file);
            for doc in chunk {
                doc.to_writer(&mut writer)?;
            }
            let mut file = writer.into_inner().map_err(|e| e.into_error())?;
            file.seek(SeekFrom::Start(0))?;
            runs.push(file);
        }
    }

    if runs.is_empty() {
        Ok(Box::new(std::iter::empty()))
    } else {
        Ok(Box::new(MergeIterator::new(runs, sort_fields, temp_dir)?))
    }
}

/// Efficiently retrieves the top-k smallest documents according to sort fields using a heap.
/// Returns an iterator over the k sorted documents.
pub fn top_k_heap_sort(
    input: Box<dyn Iterator<Item = Result<Document>>>,
    sort_fields: Arc<Vec<SortField>>,
    k: usize,
) -> Result<Box<dyn Iterator<Item = Result<Document>>>> {
    if k == 0 {
        return Ok(Box::new(std::iter::empty()));
    }

    // Wrapper for Document to implement Ord for the max-heap.
    struct HeapDoc {
        doc: Document,
        sort_fields: Arc<Vec<SortField>>,
    }

    impl PartialEq for HeapDoc {
        fn eq(&self, other: &Self) -> bool {
            compare_documents(&self.doc, &other.doc, &self.sort_fields) == Ordering::Equal
        }
    }

    impl Eq for HeapDoc {}

    impl Ord for HeapDoc {
        fn cmp(&self, other: &Self) -> Ordering {
            compare_documents(&self.doc, &other.doc, &self.sort_fields)
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
    let sorted_docs: Vec<Document> = heap.into_sorted_vec().into_iter().map(|hd| hd.doc).collect();
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
    /// Temporary directory for cleanup on drop.
    _temp_dir: TempDir, // kept for its Drop side effect
}

struct HeapItem {
    doc: Document,
    run_index: usize,
    sort_fields: Arc<Vec<SortField>>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        compare_documents(&self.doc, &other.doc, &self.sort_fields) == Ordering::Equal
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
        compare_documents(&other.doc, &self.doc, &self.sort_fields)
    }
}

impl MergeIterator {
    /// Creates a new `MergeIterator` from sorted runs, sort fields, and a temp directory.
    /// Loads the first document from each run into the heap.
    fn new(
        mut runs: Vec<NamedTempFile>,
        sort_fields: Arc<Vec<SortField>>,
        temp_dir: TempDir,
    ) -> Result<Self> {
        let mut readers = runs.drain(..).map(BufReader::new).collect::<Vec<_>>();
        let mut heap = BinaryHeap::new();

        for (i, reader) in readers.iter_mut().enumerate() {
            match Document::from_reader(reader) {
                Ok(doc) => {
                    heap.push(HeapItem {
                        doc,
                        run_index: i,
                        sort_fields: sort_fields.clone(),
                    });
                }
                Err(bson::de::Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                    // Empty run, which is fine.
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(MergeIterator {
            heap,
            readers,
            _temp_dir: temp_dir,
        })
    }
}

impl Iterator for MergeIterator {
    type Item = Result<Document>;

    /// Returns the next sorted document by popping the smallest item from the heap
    /// and loading the next document from the corresponding run.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(smallest) = self.heap.pop() {
            let next_item = match Document::from_reader(&mut self.readers[smallest.run_index]) {
                Ok(doc) => {
                    self.heap.push(HeapItem {
                        doc,
                        run_index: smallest.run_index,
                        sort_fields: smallest.sort_fields.clone(),
                    });
                    Some(Ok(smallest.doc))
                }
                Err(bson::de::Error::Io(e)) if e.kind() == ErrorKind::UnexpectedEof => {
                    // This run is exhausted.
                    Some(Ok(smallest.doc))
                }
                Err(e) => {
                    // An actual error occurred.
                    Some(Err(e.into()))
                }
            };
            next_item
        } else {
            None // Heap is empty, we are done.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::logical_plan::{make_sort_field, SortField, SortOrder};
    use crate::query::{Expr, PathComponent};
    use bson::doc;
    use std::cmp::Ordering;
    use std::sync::Arc;

    #[test]
 fn test_sort_documents() {
        let doc1 = doc! { "a": 1, "b": "xyz", "c": { "d": 10 } };
        let doc2 = doc! { "a": 2, "b": "abc", "c": { "d": 20 } };
        let doc3 = doc! { "a": 2, "b": "xyz", "c": { "d": 5 } };

        // Sort by 'a' ascending
        let sort_fields = vec![make_sort_field(vec!["a".into()], SortOrder::Ascending)];
        assert_eq!(compare_documents(&doc1, &doc2, &sort_fields), Ordering::Less);
        assert_eq!(compare_documents(&doc2, &doc1, &sort_fields), Ordering::Greater);
        assert_eq!(compare_documents(&doc2, &doc3, &sort_fields), Ordering::Equal);

        // Sort by 'a' descending
        let sort_fields = vec![make_sort_field(vec!["a".into()], SortOrder::Descending)];
        assert_eq!(compare_documents(&doc1, &doc2, &sort_fields), Ordering::Greater);
        assert_eq!(compare_documents(&doc2, &doc1, &sort_fields), Ordering::Less);

        // Sort by 'b' ascending
        let sort_fields = vec![make_sort_field(vec!["b".into()], SortOrder::Ascending)];
        assert_eq!(compare_documents(&doc1, &doc2, &sort_fields), Ordering::Greater); // "xyz" > "abc"
        assert_eq!(compare_documents(&doc2, &doc1, &sort_fields), Ordering::Less);

        // Multi-key sort: 'a' asc, then 'b' asc
        let sort_fields = vec![
            make_sort_field(vec!["a".into()], SortOrder::Ascending),
            make_sort_field(vec!["b".into()], SortOrder::Ascending),
        ];
        assert_eq!(compare_documents(&doc2, &doc3, &sort_fields), Ordering::Less);

        // Multi-key sort: 'a' asc, then 'c.d' desc
        let sort_fields = vec![
            make_sort_field(vec!["a".into()], SortOrder::Ascending),
            make_sort_field(vec!["c".into(), "d".into()], SortOrder::Descending),
        ];
        assert_eq!(compare_documents(&doc2, &doc3, &sort_fields), Ordering::Less);

        // Sort on nested key
        let sort_fields = vec![make_sort_field(vec!["c".into(), "d".into()], SortOrder::Ascending)];
        assert_eq!(compare_documents(&doc1, &doc2, &sort_fields), Ordering::Less); // 10 < 20
        assert_eq!(compare_documents(&doc3, &doc1, &sort_fields), Ordering::Less); // 5 < 10

        // Field missing in one doc
        let doc4 = doc! { "b": "only b" };
        let sort_fields = vec![make_sort_field(vec!["a".into()], SortOrder::Ascending)];
        assert_eq!(compare_documents(&doc1, &doc4, &sort_fields), Ordering::Greater);
        assert_eq!(compare_documents(&doc4, &doc1, &sort_fields), Ordering::Less);

        // No sort fields
        assert_eq!(compare_documents(&doc1, &doc2, &[]), Ordering::Equal);
    }

    fn run_sort_test<F>(mut sort_fn: F, k: Option<usize>)
    where
        F: FnMut(
            Box<dyn Iterator<Item = Result<Document>>>,
            Arc<Vec<SortField>>,
        ) -> Result<Box<dyn Iterator<Item = Result<Document>>>>,
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
        let input_iter_asc = Box::new(docs.clone().into_iter().map(Ok));
        let sorted_iter_asc = sort_fn(input_iter_asc, sort_fields_asc.clone()).unwrap();
        let sorted_docs_asc: Vec<Document> = sorted_iter_asc.map(Result::unwrap).collect();
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
        let input_iter_desc = Box::new(docs.clone().into_iter().map(Ok));
        let sorted_iter_desc = sort_fn(input_iter_desc, sort_fields_desc).unwrap();
        let sorted_docs_desc: Vec<Document> = sorted_iter_desc.map(Result::unwrap).collect();
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
        let input_iter_empty = Box::new(docs_empty.into_iter().map(Ok));
        let sorted_iter_empty = sort_fn(input_iter_empty, sort_fields_asc.clone()).unwrap();
        assert_eq!(sorted_iter_empty.count(), 0);

        // Edge case: Single document
        let docs_single = vec![doc! { "_id": 1 }];
        let input_iter_single = Box::new(docs_single.clone().into_iter().map(Ok));
        let sorted_iter_single = sort_fn(input_iter_single, sort_fields_asc).unwrap();
        let sorted_docs_single: Vec<Document> = sorted_iter_single.map(Result::unwrap).collect();
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
    fn test_in_memory_sort() {
        run_sort_test(|iter, fields| in_memory_sort(iter, &fields), None);
    }

    #[test]
    fn test_external_merge_sort() {
        run_sort_test(|iter, fields| external_merge_sort(iter, fields, 2), None);
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
