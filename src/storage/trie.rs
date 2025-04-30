use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned, Shared};
use core::sync::atomic::Ordering;
use std::sync::Arc;

/// Represents a node in the compressed trie.
///
/// - `Chain`: A compressed path of multiple bytes leading to a child.
/// - `Sparse`: A node with a small number of children (sorted transitions).
/// - `Split`: A node with many children (indexed directly by byte).
/// - `Leaf`: A terminal node holding a value.
/// - `Prefix`: A node that has both a value and continues with children.
enum Node {
    /// A compressed path fragment with a single child.
    Chain {
        path: Vec<u8>,
        child: Atomic<Node>,
    },
    /// A node with multiple children (up to a threshold).
    Sparse {
        transitions: Vec<(u8, Atomic<Node>)>,
    },
    /// A node optimized for many children (direct array index by byte value).
    Split {
        children: [Atomic<Node>; 256],
    },
    /// A leaf node holding a value.
    Leaf {
        value: Arc<[u8]>,
    },
    /// A node that stores a value and continues with children.
    Prefix {
        content: Arc<[u8]>,
        child: Atomic<Node>,
    },
}

#[derive(Debug)]
pub struct Trie {
    root: Atomic<Node>,
}

impl Trie {
    pub fn new() -> Self {
        Trie {
            root: Atomic::null(),
        }
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) {

        let guard = &epoch::pin();

        let old = self.root.load(Ordering::Relaxed, guard);

        if old.is_null() {
            // Try to install a new node
            let new = Owned::new(Self::new_leaf(key, value));
            self.root.store(new, Ordering::Relaxed);

        } else {
            // Node already exists, follow it
            if let Some(new_node) = Self::insert_internal(old, key, value, guard) {
                self.root.store(new_node, Ordering::Relaxed);
            }
        }
    }

    fn insert_internal(shared: Shared<Node>,  key: &[u8], value: &[u8], guard: &Guard) -> Option<Owned<Node>> {

        let node = unsafe { shared.deref() };

        if key.is_empty() {
            return match node {
                Node::Prefix { content: _, child } => {
                    Some(Owned::new(Node::Prefix {
                        content: Arc::from(value),
                        child: child.clone(),
                    }))
                },
                Node::Leaf { value: _ } => {
                    Some(Owned::new(Node::Leaf { value: Arc::from(value) }))
                },
                _ => {
                    Some(Owned::new(Node::Prefix { content: Arc::from(value), child: Atomic::from(shared) }))
                }
            }
        }

        match node {
            Node::Sparse { transitions } => {

                let idx = transitions.binary_search_by_key(&key[0], |(b, _)| *b);

                match idx {
                    Ok(i) => {
                        // Update existing child
                        let atomic = &transitions[i].1;
                        let shared = atomic.load(Ordering::Relaxed, guard);

                        if let Some(new_node) = Self::insert_internal(shared, &key[1..], value, guard) {
                            atomic.store(new_node, Ordering::Relaxed);
                        }
                        None
                    }
                    Err(i) => {
                        let new_leaf = Self::new_leaf(&key[1..], value);
                        // Insert new transition, converting the node into a split node if needed
                        if transitions.len() + 1 > 6 {
                            // We have reached the sparse limit and need to use a split node
                            let mut children : [Atomic<Node>; 256] = std::array::from_fn(|i| {
                                transitions.iter()
                                    .find(|(b, _)| *b as usize == i)
                                    .map(|(_, child)| child.clone())
                                    .unwrap_or_else(|| Atomic::null())
                            });
                            children[key[0] as usize] = Atomic::new(new_leaf);
                            Some(Owned::new(Node::Split { children }))
                        } else {
                            let new_transitions = Self::cow_insert(transitions, i, (key[0], Atomic::new(new_leaf)));
                            Some(Owned::new(Node::Sparse { transitions: new_transitions }))
                        }
                    }
                }
            }
            Node::Chain { path, child } => {

                let common = path.iter()
                    .zip(key.iter())
                    .take_while(|(a, b)| a == b)
                    .count();

                if common == path.len() {

                    let shared = child.load(Ordering::Relaxed, guard);
                    if let Some(new_node) = Self::insert_internal(shared, &key[common..], value, guard) {
                        child.store(new_node, Ordering::Relaxed);
                    }
                    None

                } else if common == key.len() {

                    // The key end within the chain. We need to break the chain to introduce
                    // a prefix

                    let new_child = Node::Chain {
                        path: path[common..].to_vec(),
                        child: child.clone(),
                    };

                    let new_child = Node::Prefix { content : Arc::from(value), child: Atomic::new(new_child) };

                    Some(Owned::new(Node::Chain {
                        path: path[..common].to_vec(),
                        child: Atomic::new(new_child),
                    }))

                } else {

                    // Need to split the chain
                    let mut transitions = vec![
                        (path[common], Atomic::new(Node::Chain { path: path[common + 1..].to_vec(), child: child.clone() })),
                        (key[common], Atomic::new(Self::new_leaf(&key[common + 1..], value))),
                    ];
                    transitions.sort_unstable_by_key(|(b, _)| *b);

                    let mut new_node = Node::Sparse { transitions };

                    if common != 0 {
                        new_node = Node::Chain {
                            path: path[..common].to_vec(),
                            child: Atomic::new(new_node),
                        };
                    }

                    Some(Owned::new(new_node))
                }
            }
            Node::Leaf { value: current_value } => {
                Some(Owned::new(Node::Prefix {
                    content: current_value.clone(),
                    child : Atomic::new(Self::new_leaf(key, value)),
                }))
            }
            Node::Prefix { content: _ , child } => {

                let shared = child.load(Ordering::Relaxed, guard);
                if let Some(new_node) = Self::insert_internal(shared, key, value, guard) {
                    child.store(new_node, Ordering::Relaxed);
                }
                None
             }
            Node::Split { children } => {
                let idx = key[0] as usize;
                let atomic = &children[idx];
                let shared = atomic.load(Ordering::Relaxed, guard);
                if shared.is_null() {
                    atomic.store(Owned::new(Self::new_leaf(&key[1..], value)), Ordering::Relaxed);
                } else {
                    if let Some(new_node) = Self::insert_internal(shared, &key[1..], value, guard) {
                        atomic.store(new_node, Ordering::Relaxed);
                    }
                }
                None
            }
        }
    }

    fn new_leaf(key: &[u8], value: &[u8]) -> Node {
        let leaf = Node::Leaf { value: Arc::from(value) };
        if key.is_empty() {
            leaf
        } else {
            Node::Chain { path: key.to_vec(), child: Atomic::new(leaf) }
        }
    }

    fn cow_insert<T: std::clone::Clone>(vec: &Vec<T>, idx: usize, value: T) -> Vec<T> {
        let mut new_vec = Vec::with_capacity(vec.len() + 1);
        new_vec.extend_from_slice(&vec[..idx]);
        new_vec.push(value);
        new_vec.extend_from_slice(&vec[idx..]);
        new_vec
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
    use core::sync::atomic::Ordering;
    use crate::storage::trie::{Node, Trie};

    impl PartialEq for Node {
        fn eq(&self, other: &Self) -> bool {
            use Node::*;

            match (self, other) {
                (Leaf { value: v1 }, Leaf { value: v2 }) => v1 == v2,
                (Chain { path: p1, child: c1 }, Chain { path: p2, child: c2 }) => {
                    p1 == p2 && atomic_eq(c1, c2)
                }
                (Sparse { transitions: t1 }, Sparse { transitions: t2 }) => {
                    if t1.len() != t2.len() {
                        return false;
                    }
                    t1.iter().zip(t2.iter()).all(|((b1, n1), (b2, n2))| {
                        b1 == b2 && atomic_eq(n1, n2)
                    })
                }
                (Split { children: c1 }, Split { children: c2 }) => {
                    (0..256).all(|i| atomic_eq(&c1[i], &c2[i]))
                }
                (Prefix { content: c1, child: n1 }, Prefix { content: c2, child: n2 }) => {
                    c1 == c2 && atomic_eq(n1, n2)
                }
                _ => false,
            }
        }
    }

    fn atomic_eq(a: &Atomic<Node>, b: &Atomic<Node>) -> bool {
        let guard = &epoch::pin();
        let ap = a.load(Ordering::Acquire, guard);
        let bp = b.load(Ordering::Acquire, guard);

        match (unsafe { ap.as_ref() }, unsafe { bp.as_ref() }) {
            (None, None) => true,
            (Some(x), Some(y)) => x == y,
            _ => false,
        }
    }

    impl std::fmt::Debug for Node {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            use Node::*;
            match self {
                Leaf { value } => f.debug_struct("Leaf")
                    .field("value", value)
                    .finish(),
                Chain { path, child } => {
                    let guard = &epoch::pin();
                    let child = child.load(Ordering::Acquire, guard);
                    f.debug_struct("Chain")
                        .field("path", path)
                        .field("child", &SharedDebug(child))
                        .finish()
                }
                Sparse { transitions } => {
                    let guard = &epoch::pin();
                    let transitions_debug: Vec<_> = transitions.iter()
                        .map(|(b, child)| (*b, SharedDebug(child.load(Ordering::Acquire, guard))))
                        .collect();
                    f.debug_struct("Sparse")
                        .field("transitions", &transitions_debug)
                        .finish()
                }
                Split { children } => {
                    let guard = &epoch::pin();
                    let children_debug: Vec<_> = children.iter().enumerate()
                        .filter_map(|(i, child)| {
                            let loaded = child.load(Ordering::Acquire, guard);
                            unsafe {
                                loaded.as_ref().map(|_| (i, SharedDebug(loaded)))
                            }
                        })
                        .collect();
                    f.debug_struct("Split")
                        .field("children", &children_debug)
                        .finish()
                }
                Prefix { content, child } => {
                    let guard = &epoch::pin();
                    let child = child.load(Ordering::Acquire, guard);
                    f.debug_struct("Prefix")
                        .field("content", content)
                        .field("child", &SharedDebug(child))
                        .finish()
                }
            }
        }
    }

    struct SharedDebug<'g>(Shared<'g, Node>);

    #[cfg(test)]
    impl<'g> std::fmt::Debug for SharedDebug<'g> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(node) = unsafe { self.0.as_ref() } {
                node.fmt(f)
            } else {
                write!(f, "null")
            }
        }
    }

    #[test]
    fn test_insert() {
        let trie = Trie::new();
        trie.insert(b"cat", b"1");

        let expected = chain(b"cat", leaf(b"1"));

        assert_tries_eq(&trie, expected);

        trie.insert(b"dog", b"2");

        let expected = sparse(vec![
            (b'c', chain(b"at", leaf(b"1"))),
            (b'd', chain(b"og", leaf(b"2")))
        ]);

        assert_tries_eq(&trie, expected);

        trie.insert(b"monkey", b"3");

        let expected = sparse(vec![
            (b'c', chain(b"at", leaf(b"1"))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'm', chain(b"onkey", leaf(b"3")))
        ]);

        assert_tries_eq(&trie, expected);

        trie.insert(b"mountain lion", b"4");

        let expected = sparse(vec![
            (b'c', chain(b"at", leaf(b"1"))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain lion", leaf(b"4"))),
            ]))),
        ]);

        assert_tries_eq(&trie, expected);

        trie.insert(b"caterpillar", b"5");

        let expected = sparse(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain lion", leaf(b"4"))),
            ]))),
        ]);

        assert_tries_eq(&trie, expected);

        trie.insert(b"kangaroo", b"6");

        let expected = sparse(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain lion", leaf(b"4"))),
            ]))),
        ]);

        assert_tries_eq(&trie, expected);

        trie.insert(b"mountain", b"7");

        let expected = sparse(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
            ]))),
        ]);

        assert_tries_eq(&trie, expected);

        trie.insert(b"", b"8");

        let expected = prefix( b"8", sparse(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
            ]))),
        ]));

        assert_tries_eq(&trie, expected);

        trie.insert(b"giraffe", b"9");
        trie.insert(b"zebra", b"10");

        let expected = prefix( b"8", sparse(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'g', chain(b"iraffe", leaf(b"9"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
            ]))),
            (b'z', chain(b"ebra", leaf(b"10"))),
        ]));

        assert_tries_eq(&trie, expected);

        trie.insert(b"snake", b"11");

        let expected = prefix( b"8", split(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'g', chain(b"iraffe", leaf(b"9"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
            ]))),
            (b's', chain(b"nake", leaf(b"11"))),
            (b'z', chain(b"ebra", leaf(b"10"))),
        ]));

        assert_tries_eq(&trie, expected);

        trie.insert(b"quokka", b"12");

        let expected = prefix( b"8", split(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'g', chain(b"iraffe", leaf(b"9"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
            ]))),
            (b'q', chain(b"uokka", leaf(b"12"))),
            (b's', chain(b"nake", leaf(b"11"))),
            (b'z', chain(b"ebra", leaf(b"10"))),
        ]));

        assert_tries_eq(&trie, expected);

        trie.insert(b"mountain goat", b"13");

        let expected = prefix( b"8", split(vec![
            (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
            (b'd', chain(b"og", leaf(b"2"))),
            (b'g', chain(b"iraffe", leaf(b"9"))),
            (b'k', chain(b"angaroo", leaf(b"6"))),
            (b'm', chain(b"o", sparse(vec![
                (b'n', chain(b"key", leaf(b"3"))),
                (b'u', chain(b"ntain", prefix(b"7", chain(b" ", sparse(vec![
                    (b'g', chain(b"oat", leaf(b"13"))),
                    (b'l', chain(b"ion", leaf(b"4"))),
                ]))))),
            ]))),
            (b'q', chain(b"uokka", leaf(b"12"))),
            (b's', chain(b"nake", leaf(b"11"))),
            (b'z', chain(b"ebra", leaf(b"10"))),
        ]));

        assert_tries_eq(&trie, expected);
    }

    #[test]
    fn test_insert_into_empty_trie_with_empty_path() {
        let trie = Trie::new();
        trie.insert(b"", b"1");

        assert_tries_eq(&trie, leaf(b"1"));
    }

    #[test]
    fn test_insert_into_empty_trie() {
        let trie = Trie::new();
        trie.insert(b"path", b"1");

        assert_tries_eq(&trie, chain(b"path", leaf(b"1")));
    }

    #[test]
    fn test_insert_into_leaf() {
        let trie = Trie::new();
        trie.insert(b"", b"1");
        trie.insert(b"", b"2");

        assert_tries_eq(&trie, leaf(b"2"));

        trie.insert(b"path", b"3");

        assert_tries_eq(&trie, prefix(b"2", chain(b"path", leaf(b"3"))));
    }

    #[test]
    fn test_insert_into_sparse() {
        let trie = Trie::new();
        trie.insert(b"cobra", b"1");
        trie.insert(b"python", b"2");

        assert_tries_eq(&trie, sparse(vec![
            (b'c', chain(b"obra", leaf(b"1"))),
            (b'p', chain(b"ython", leaf(b"2"))),
        ]));

        trie.insert(b"mamba", b"3");

        assert_tries_eq(&trie, sparse(vec![
            (b'c', chain(b"obra", leaf(b"1"))),
            (b'm', chain(b"amba", leaf(b"3"))),
            (b'p', chain(b"ython", leaf(b"2"))),
        ]));

        trie.insert(b"boa", b"4");

        assert_tries_eq(&trie, sparse(vec![
            (b'b', chain(b"oa", leaf(b"4"))),
            (b'c', chain(b"obra", leaf(b"1"))),
            (b'm', chain(b"amba", leaf(b"3"))),
            (b'p', chain(b"ython", leaf(b"2"))),
        ]));

        trie.insert(b"sidewinder", b"5");

        assert_tries_eq(&trie, sparse(vec![
            (b'b', chain(b"oa", leaf(b"4"))),
            (b'c', chain(b"obra", leaf(b"1"))),
            (b'm', chain(b"amba", leaf(b"3"))),
            (b'p', chain(b"ython", leaf(b"2"))),
            (b's', chain(b"idewinder", leaf(b"5"))),
        ]));

        trie.insert(b"copperhead", b"6");

        assert_tries_eq(&trie, sparse(vec![
            (b'b', chain(b"oa", leaf(b"4"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"1"))),
                (b'p', chain(b"perhead", leaf(b"6"))),
            ]))),
            (b'm', chain(b"amba", leaf(b"3"))),
            (b'p', chain(b"ython", leaf(b"2"))),
            (b's', chain(b"idewinder", leaf(b"5"))),
        ]));

        trie.insert(b"", b"7");

        assert_tries_eq(&trie, prefix(b"7", sparse(vec![
            (b'b', chain(b"oa", leaf(b"4"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"1"))),
                (b'p', chain(b"perhead", leaf(b"6"))),
            ]))),
            (b'm', chain(b"amba", leaf(b"3"))),
            (b'p', chain(b"ython", leaf(b"2"))),
            (b's', chain(b"idewinder", leaf(b"5"))),
        ])));
    }


    #[test]
    fn test_insert_into_split() {
        let trie = Trie::new();
        trie.insert(b"boa", b"1");
        trie.insert(b"cobra", b"2");
        trie.insert(b"fer-de-lance", b"3");
        trie.insert(b"mamba", b"4");
        trie.insert(b"python", b"5");
        trie.insert(b"sidewinder", b"6");
        trie.insert(b"taipan", b"7");

        assert_tries_eq(&trie, split(vec![
            (b'b', chain(b"oa", leaf(b"1"))),
            (b'c', chain(b"obra", leaf(b"2"))),
            (b'f', chain(b"er-de-lance", leaf(b"3"))),
            (b'm', chain(b"amba", leaf(b"4"))),
            (b'p', chain(b"ython", leaf(b"5"))),
            (b's', chain(b"idewinder", leaf(b"6"))),
            (b't', chain(b"aipan", leaf(b"7"))),
        ]));

        trie.insert(b"copperhead", b"8");

        assert_tries_eq(&trie, split(vec![
            (b'b', chain(b"oa", leaf(b"1"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"2"))),
                (b'p', chain(b"perhead", leaf(b"8"))),
            ]))),
            (b'f', chain(b"er-de-lance", leaf(b"3"))),
            (b'm', chain(b"amba", leaf(b"4"))),
            (b'p', chain(b"ython", leaf(b"5"))),
            (b's', chain(b"idewinder", leaf(b"6"))),
            (b't', chain(b"aipan", leaf(b"7"))),
        ]));

        trie.insert(b"asian cobra", b"9");

        assert_tries_eq(&trie, split(vec![
            (b'a', chain(b"sian cobra", leaf(b"9"))),
            (b'b', chain(b"oa", leaf(b"1"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"2"))),
                (b'p', chain(b"perhead", leaf(b"8"))),
            ]))),
            (b'f', chain(b"er-de-lance", leaf(b"3"))),
            (b'm', chain(b"amba", leaf(b"4"))),
            (b'p', chain(b"ython", leaf(b"5"))),
            (b's', chain(b"idewinder", leaf(b"6"))),
            (b't', chain(b"aipan", leaf(b"7"))),
        ]));

        trie.insert(b"water snake", b"10");

        assert_tries_eq(&trie, split(vec![
            (b'a', chain(b"sian cobra", leaf(b"9"))),
            (b'b', chain(b"oa", leaf(b"1"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"2"))),
                (b'p', chain(b"perhead", leaf(b"8"))),
            ]))),
            (b'f', chain(b"er-de-lance", leaf(b"3"))),
            (b'm', chain(b"amba", leaf(b"4"))),
            (b'p', chain(b"ython", leaf(b"5"))),
            (b's', chain(b"idewinder", leaf(b"6"))),
            (b't', chain(b"aipan", leaf(b"7"))),
            (b'w', chain(b"ater snake", leaf(b"10"))),
        ]));

        trie.insert(b"cottonmouth", b"11");

        assert_tries_eq(&trie, split(vec![
            (b'a', chain(b"sian cobra", leaf(b"9"))),
            (b'b', chain(b"oa", leaf(b"1"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"2"))),
                (b'p', chain(b"perhead", leaf(b"8"))),
                (b't', chain(b"tonmouth", leaf(b"11"))),
            ]))),
            (b'f', chain(b"er-de-lance", leaf(b"3"))),
            (b'm', chain(b"amba", leaf(b"4"))),
            (b'p', chain(b"ython", leaf(b"5"))),
            (b's', chain(b"idewinder", leaf(b"6"))),
            (b't', chain(b"aipan", leaf(b"7"))),
            (b'w', chain(b"ater snake", leaf(b"10"))),
        ]));

        trie.insert(b"", b"12");

        assert_tries_eq(&trie, prefix(b"12", split(vec![
            (b'a', chain(b"sian cobra", leaf(b"9"))),
            (b'b', chain(b"oa", leaf(b"1"))),
            (b'c', chain(b"o", sparse(vec![
                (b'b', chain(b"ra", leaf(b"2"))),
                (b'p', chain(b"perhead", leaf(b"8"))),
                (b't', chain(b"tonmouth", leaf(b"11"))),
            ]))),
            (b'f', chain(b"er-de-lance", leaf(b"3"))),
            (b'm', chain(b"amba", leaf(b"4"))),
            (b'p', chain(b"ython", leaf(b"5"))),
            (b's', chain(b"idewinder", leaf(b"6"))),
            (b't', chain(b"aipan", leaf(b"7"))),
            (b'w', chain(b"ater snake", leaf(b"10"))),
        ])));
    }

    #[test]
    fn test_insert_into_prefix() {
        let trie = Trie::new();
        trie.insert(b"cobra", b"1");
        trie.insert(b"", b"2");

        assert_tries_eq(&trie, prefix(b"2", chain(b"cobra", leaf(b"1"))));

        trie.insert(b"", b"3");

        assert_tries_eq(&trie, prefix(b"3", chain(b"cobra", leaf(b"1"))));
    }

    #[test]
    fn test_insert_into_chain() {
        let trie = Trie::new();
        trie.insert(b"cobra", b"1");

        assert_tries_eq(&trie, chain(b"cobra", leaf(b"1")));

        trie.insert(b"common european viper", b"2");

        assert_tries_eq(&trie, chain(b"co", sparse(vec![
            (b'b', chain(b"ra", leaf(b"1"))),
            (b'm', chain(b"mon european viper", leaf(b"2"))),
        ])));

        trie.insert(b"common water snake", b"3");

        assert_tries_eq(&trie, chain(b"co", sparse(vec![
            (b'b', chain(b"ra", leaf(b"1"))),
            (b'm', chain(b"mon ", sparse(vec![
                (b'e', chain(b"uropean viper", leaf(b"2"))),
                (b'w', chain(b"ater snake", leaf(b"3"))),
            ]))),
        ])));
   }

    fn assert_tries_eq(trie: &Trie, expected: Node) {
        let guard = &epoch::pin();
        let shared = trie.root.load(Ordering::Acquire, guard);
        let root = unsafe { shared.deref() };

        assert_eq!(root, &expected);
    }

    fn sparse(transitions: Vec<(u8, Node)>) -> Node {
        let transitions = transitions.into_iter().map(|(b, node)| (b, Atomic::new(node))).collect::<Vec<_>>();
        Node::Sparse { transitions }
    }

    fn chain(key: &[u8], value: Node) -> Node {
        Node::Chain { path: key.to_vec(), child: Atomic::new(value) }
    }

    fn leaf(value: &[u8]) -> Node {
        Node::Leaf { value: Arc::from(value) }
    }

    fn prefix(content: &[u8], child: Node) -> Node {
        Node::Prefix { content: Arc::from(content), child: Atomic::new(child) }
    }

    fn split(transitions: Vec<(u8, Node)>) -> Node {
        let mut array: [Atomic<Node>; 256] = std::array::from_fn(|_| Atomic::null());

        for (b, child) in transitions {
            array[b as usize].store(Owned::new(child), Ordering::Release);
        }

        Node::Split { children: array }
    }
}