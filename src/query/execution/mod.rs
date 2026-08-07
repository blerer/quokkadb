use crate::query::PathComponent;
use bson::{Bson, Document};

mod executor;
mod filters;
mod indexes;
mod projections;
pub(crate) mod query_executor;
pub(crate) mod sorts;
mod updates;
pub use query_executor::QueryExecutor;

/// Inserts `value` at `path` inside `doc`, creating intermediate documents as needed.
/// Array-element path components are skipped since upsert base documents are plain documents.
pub fn set_path_value(doc: &mut Document, path: &[PathComponent], value: Bson) {
    match path {
        [] => {}
        [PathComponent::FieldName(name)] => {
            doc.insert(name.clone(), value);
        }
        [PathComponent::FieldName(name), rest @ ..] => {
            let nested = doc
                .entry(name.clone())
                .or_insert_with(|| Bson::Document(Document::new()));
            if let Bson::Document(nested_doc) = nested {
                set_path_value(nested_doc, rest, value);
            }
        }
        _ => {} // skip array-element components
    }
}

#[cfg(test)]
mod tests {
    use super::set_path_value;
    use crate::query::PathComponent;
    use bson::{doc, Bson, Document};

    #[test]
    fn set_path_value_creates_nested_documents() {
        let mut doc = Document::new();

        set_path_value(
            &mut doc,
            &[PathComponent::from("a"), PathComponent::from("b")],
            Bson::Int32(1),
        );

        assert_eq!(doc, doc! { "a": { "b": 1 } });
    }

    #[test]
    fn set_path_value_merges_nested_fields_with_shared_prefix() {
        let mut doc = Document::new();

        set_path_value(
            &mut doc,
            &[PathComponent::from("a"), PathComponent::from("b")],
            Bson::Int32(1),
        );
        set_path_value(
            &mut doc,
            &[PathComponent::from("a"), PathComponent::from("c")],
            Bson::Int32(2),
        );

        assert_eq!(doc, doc! { "a": { "b": 1, "c": 2 } });
    }

    #[test]
    fn set_path_value_skips_array_components() {
        let mut doc = Document::new();

        set_path_value(
            &mut doc,
            &[
                PathComponent::from("a"),
                PathComponent::from(0usize),
                PathComponent::from("b"),
            ],
            Bson::Int32(1),
        );

        assert_eq!(doc, doc! { "a": {} });
    }
}
