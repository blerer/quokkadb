use crate::error::Result;
use crate::query::execution::filters;
use crate::query::{Parameters, PathComponent, Projection, ProjectionExpr};
use std::fmt;
use std::sync::Arc;
use bson::{Bson, Document};
use crate::query::{BsonValueRef};

pub fn to_projector(
    projection: &Arc<Projection>,
    parameters: &Parameters,
) -> Result<Box<dyn Fn(Document) -> Result<Document> + Send + Sync>> {
    match projection.as_ref() {
        Projection::Include(projection_expr) => {
            let projector = build_projector(projection_expr, parameters)?;
            Ok(Box::new(move |doc| Ok(projector.include(&doc))))
        }
        Projection::Exclude(projection_expr) => {
            let projector = build_projector(projection_expr, parameters)?;
            Ok(Box::new(move |mut doc| {
                projector.exclude(&mut doc);
                Ok(doc)
            }))
        }
    }
}

fn build_projector(
    projection_expr: &Arc<ProjectionExpr>,
    parameters: &Parameters,
) -> Result<Projector> {
    match projection_expr.as_ref() {
        ProjectionExpr::Fields { children } => {
            let children_projectors = children
                .iter()
                .map(|(path_comp, proj_expr)| {
                    if let PathComponent::FieldName(name) = path_comp {
                        let projector = build_projector(proj_expr, parameters)?;
                        Ok((name.clone(), projector))
                    } else {
                        panic!("Expected field name in projection");
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Projector::Fields { children: children_projectors })
        }
        ProjectionExpr::ArrayElements { children } => {
            let children_projectors = children
                .iter()
                .map(|(path_comp, proj_expr)| {
                    if let PathComponent::ArrayElement(idx) = path_comp {
                        let projector = build_projector(proj_expr, parameters)?;
                        Ok((*idx, projector))
                    } else {
                        panic!("Expected array index in projection");
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Projector::ArrayElements {
                children: children_projectors,
            })
        }
        ProjectionExpr::Field => Ok(Projector::Field),
        ProjectionExpr::PositionalField { filter } => Ok(Projector::PositionalField {
            filter: to_element_filter(filter.clone(), parameters),
        }),
        ProjectionExpr::Slice { skip, limit } => Ok(Projector::Slice {
            skip: *skip,
            limit: *limit,
        }),
        ProjectionExpr::ElemMatch { filter } => Ok(Projector::ElemMatch {
            filter: to_element_filter(filter.clone(), parameters),
        }),
        ProjectionExpr::PositionalFieldRef => panic!("PositionalFieldRef should not appear in execution plan"),
    }
}

/// Creates a filter function for array elements inside a projection.
/// This helper function assumes that array elements are documents when filtering.
fn to_element_filter(
    predicate: Arc<crate::query::Expr>,
    parameters: &Parameters,
) -> Box<dyn Fn(BsonValueRef) -> bool + Send + Sync> {
    let doc_filter = filters::to_value_filter(predicate, parameters);
    Box::new(move |element| doc_filter(Some(element)))
}

enum Projector {
    Fields {
        children: Vec<(String, Projector)>,
    },
    ArrayElements {
        children: Vec<(usize, Projector)>,
    },
    Field,
    PositionalField {
        filter:  Box<dyn Fn(BsonValueRef) -> bool + Send + Sync>,
    },
    Slice {
        skip: Option<i32>,
        limit: i32,
    },
    ElemMatch {
        filter: Box<dyn Fn(BsonValueRef) -> bool + Send + Sync>,
    },
}

impl fmt::Display for Projector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Projector::Fields { .. } => "Fields",
            Projector::ArrayElements { .. } => "ArrayElements",
            Projector::Field => "Field",
            Projector::PositionalField { .. } => "PositionalField",
            Projector::Slice { .. } => "Slice",
            Projector::ElemMatch { .. } => "ElemMatch",
        };
        write!(f, "{}", name)
    }
}

impl Projector {

    pub fn include(&self, doc: &Document) -> Document {
        let mut projected = Document::new();
        match self {
            Projector::Fields { children } => {
                for (key, projector) in children {
                    if let Some(value) = doc.get(key) {
                        if let Some(included_value) = projector.include_value(value) {
                            projected.insert(key.clone(), included_value);
                        }
                    }
                }
                projected
            }
            _ => projected
        }
    }

    pub fn include_value(&self, value: &Bson) -> Option<Bson> {
        match self {
            Projector::Fields { children: _ } => {
                if let Bson::Document(doc) = value {
                    Some(Bson::Document(self.include(doc)))
                } else {
                    None
                }
            },
            Projector::ArrayElements { children } => {
                let mut doc = Document::new();
                if let Bson::Array(array) = value {
                    for (idx, projector) in children {
                        if let Some(value) = array.get(*idx) {
                            let element = projector.include_value(value);
                            if let Some(element) = element {
                                doc.insert(idx.to_string(), element);
                            }
                        }
                    }
                    Some(Bson::Document(doc))
                } else {
                    None
                }
            },
            Projector::Field => Some(value.clone()),
            Projector::PositionalField { filter } => {
                if let Bson::Array(array) = value {
                    let filtered = array.iter()
                        .filter(|b| filter(BsonValueRef(*b)))
                        .take(1)
                        .cloned()
                        .collect::<Vec<Bson>>();
                    if filtered.is_empty() { None } else { Some(Bson::Array(filtered)) }
                } else {
                    None
                }
            },
            Projector::Slice { skip, limit } => {
                let skip = skip.unwrap_or(0) as usize;
                let limit = *limit as usize;
                if let Bson::Array(array) = value {
                    let sliced = array.iter()
                        .skip(skip)
                        .take(limit)
                        .cloned()
                        .collect::<Vec<Bson>>();
                    Some(Bson::Array(sliced))
                } else {
                    None
                }
            },
            Projector::ElemMatch { filter } => {
                if let Bson::Array(array) = value {
                    let filtered = array.iter()
                        .filter(|b| filter(BsonValueRef(*b)))
                        .cloned()
                        .collect::<Vec<Bson>>();
                    Some(Bson::Array(filtered))
                } else {
                    None
                }
            },
        }
    }

    pub fn exclude<'a>(&self, doc: &'a mut Document) -> &'a mut Document {
        match self {
            Projector::Fields { children } => {
                for (key, projector) in children {
                    match projector {
                        Projector::Field => {
                            doc.remove(key);
                        },
                        Projector::Fields { .. } => {
                            if let Some(Bson::Document(sub_doc)) = doc.get_mut(key) {
                                projector.exclude(sub_doc);
                            }
                        },
                        _ => {
                            panic!("Unexpected projector: {}", self)
                        }
                    }
                }
                doc
            }
            _ => doc
        }
    }
}
