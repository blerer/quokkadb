use std::sync::Arc;
use arc_swap::ArcSwap;

/// Represents a node in the compressed trie.
///
/// - `Chain`: A compressed path of multiple bytes leading to a child.
/// - `Sparse`: A node with a small number of children (sorted transitions).
/// - `Split`: A node with many children (indexed directly by byte).
/// - `Leaf`: A terminal node holding a value.
/// - `Prefix`: A node that has both a value and continues with children.
#[derive(Debug, PartialEq)]
enum Node {
    /// A compressed path fragment with a single child.
    Chain {
        path: u8,
        child: Arc<Node>,
    },
    /// A node with multiple children (up to a threshold).
    Sparse {
        transitions: Vec<(u8, Arc<Node>)>,
    },
    /// A node optimized for many children (direct array index by byte value).
    Split {
        children: ImmutableArray,
    },
    /// A leaf node holding a value.
    Leaf {
        value: Arc<[u8]>,
    },
    /// A node that stores a value and continues with children.
    Prefix {
        content: Arc<[u8]>,
        child: Arc<Node>,
    },
    Empty,
}

impl Node {

    fn new(key: &[u8], value: &[u8]) -> Arc<Node> {
        let leaf = Node::Leaf { value: Arc::from(value) };
        Arc::new(if key.is_empty() {
            leaf
        } else {
            Node::Chain { path: key.to_vec(), child: Arc::new(leaf) }
        })
    }

    fn insert(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Arc<Node> {

        if key.is_empty() {
            return match self.as_ref() {
                Node::Prefix { content: _, child } => {
                    Arc::new(Node::Prefix {
                        content: Arc::from(value),
                        child: child.clone(),
                    })
                },
                Node::Leaf { value: _ } | Node::Empty => {
                    Arc::new(Node::Leaf { value: Arc::from(value) })
                },
                _ => {
                    Arc::new(Node::Prefix { content: Arc::from(value), child: self.clone() })
                }
            }
        }

        match self.as_ref() {
            Node::Empty => {
                Node::new(key, value)
            },
            Node::Sparse { transitions } => {

                let mut new_transitions = Vec::with_capacity(transitions.len() + 1);
                let mut inserted = false;
                for (b, node) in transitions.iter() {
                    if b == &key[0] {
                        new_transitions.push((*b, node.insert(&key[1..], value)));
                        inserted = true;
                    } else {
                        if !inserted && b > &key[0] {
                            new_transitions.push((key[0], Node::new(&key[1..], value)));
                            inserted = true;
                        }
                        new_transitions.push((*b, node.clone()));
                    }
                }

                if !inserted {
                    new_transitions.push((key[0], Node::new(&key[1..], value)));
                }

                if new_transitions.len() <= 6 {
                    Arc::new(Node::Sparse { transitions: new_transitions })
                } else {
                    Arc::new(Node::Split { children: ImmutableArray::new(new_transitions) })
                }
            }
            Node::Chain { path, child } => {

                let common = path.iter()
                    .zip(key.iter())
                    .take_while(|(a, b)| a == b)
                    .count();

                if common == path.len() {

                    Arc::new(Node::Chain {
                        path: path.to_vec(),
                        child: child.insert(&key[common..], value)
                    })

                } else if common == key.len() {

                    // The key end within the chain. We need to break the chain to introduce
                    // a prefix

                    let new_child = Node::Chain {
                        path: path[common..].to_vec(),
                        child: child.clone(),
                    };

                    let new_child = Node::Prefix { content : Arc::from(value), child: Arc::new(new_child) };

                    Arc::new(Node::Chain {
                        path: path[..common].to_vec(),
                        child: Arc::new(new_child),
                    })

                } else {

                    // Need to split the chain
                    let mut transitions = vec![
                        (path[common], Arc::new(Node::Chain { path: path[common + 1..].to_vec(), child: child.clone() })),
                        (key[common], Node::new(&key[common + 1..], value)),
                    ];
                    transitions.sort_unstable_by_key(|(b, _)| *b);

                    let mut new_node = Node::Sparse { transitions };

                    if common != 0 {
                        new_node = Node::Chain {
                            path: path[..common].to_vec(),
                            child: Arc::new(new_node),
                        };
                    }

                    Arc::new(new_node)
                }
            }
            Node::Leaf { value: current_value } => {
                Arc::new(Node::Prefix {
                    content: current_value.clone(),
                    child : Node::new(key, value),
                })
            }
            Node::Prefix { content , child } => {
                Arc::new(Node::Prefix {
                    content: content.clone(),
                    child : child.insert(key, value),
                })
            }
            Node::Split { children } => {
                Arc::new(Node::Split {
                    children: children.insert(key[0], &key[1..], value),
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct Trie {
    root: ArcSwap<Node>,
}

impl Trie {
    pub fn new() -> Self {
        Trie {
            root: ArcSwap::new(Arc::new(Node::Empty)),
        }
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) {

        let old = self.root.load();

        self.root.store(old.insert(key, value));
    }
}

#[derive(Debug, PartialEq)]
struct Leafs {
    items: [Option<Arc<Node>>; 8],
}

impl Leafs {
    fn empty() -> Self {
        Leafs { items: std::array::from_fn(|_| None) }
    }

    fn new(byte: u8, key: &[u8], value: &[u8]) -> Arc<Self> {
        let mut items = std::array::from_fn(|_| None);
        items[Self::idx(&byte)] = Some(Node::new(key, value));
        Arc::new(Leafs { items })
    }
    fn insert(self: &Arc<Self>, byte: u8, key: &[u8], value: &[u8]) -> Arc<Self> {
        let mut new_items = self.items.clone();
        let idx = Self::idx(&byte);
        let old_item = &new_items[idx];
        let new_item = if let Some(node) = old_item {
            node.insert(key, value)
        } else {
            Node::new(key, value)
        };
        new_items[idx] = Some(new_item);
        Arc::new(Leafs { items: new_items })
    }

    fn get(&self, byte: u8) -> Option<Arc<Node>> {
        match &self.items[Self::idx(&byte)] {
            Some(node) => Some(node.clone()),
            None => None,
        }
    }

    fn insert_node(&mut self, idx: usize, node: Arc<Node>) {
        self.items[idx] = Some(node);
    }


    fn idx(byte: &u8) -> usize {
        (byte & 0x07) as usize
    }
}

#[derive(Debug, PartialEq)]
struct InternalNodes {
    items: [Option<Arc<Leafs>>; 8],
}

impl InternalNodes {

    fn empty() -> Self {
        InternalNodes { items: std::array::from_fn(|_| None) }
    }

    fn new(byte: u8, key: &[u8], value: &[u8]) -> Arc<Self> {
        let mut items = std::array::from_fn(|_| None);
        items[Self::idx(&byte)] = Some(Leafs::new(byte, key, value));
        Arc::new(Self { items })
    }

    fn insert(self: &Arc<Self>, byte: u8, key: &[u8], value: &[u8]) -> Arc<Self> {
        let mut new_items = self.items.clone();
        let idx = Self::idx(&byte);
        let old_item = &new_items[idx];
        let new_item = if let Some(node) = old_item {
            node.insert(byte, key, value)
        } else {
            Leafs::new(byte, key, value)
        };
        new_items[idx] = Some(new_item);
        Arc::new(Self { items: new_items })
    }

    fn insert_leafs(&mut self, idx: usize, leafs: Arc<Leafs>) {
        self.items[idx] = Some(leafs);
    }

    fn get(&self, byte: u8) -> Option<Arc<Node>> {
        match &self.items[Self::idx(&byte)] {
            Some(item) => item.get(byte),
            None => None,
        }
    }

    fn idx(byte: &u8) -> usize {
        ((*byte >> 3) & 0x07) as usize
    }
}

#[derive(Debug, PartialEq)]
struct ImmutableArray {
    items: [Option<Arc<InternalNodes>>; 4]
}

impl ImmutableArray {

    fn empty() -> Self {
        ImmutableArray { items: std::array::from_fn(|_| None) }
    }

    fn new(nodes: Vec<(u8, Arc<Node>)>) -> Self {

        if nodes.is_empty() {
            return Self::empty();
        }

        // We know that the nodes are ordered by u8
        let mut array = std::array::from_fn(|_| None);
        let mut internals = InternalNodes::empty();
        let mut leafs = Leafs::empty();

        let mut iter = nodes.iter();
        let (b, node) = iter.next().unwrap();

        leafs.insert_node(Leafs::idx(b), node.clone());
        let mut prev_l = InternalNodes::idx(b);
        let mut prev_i = ImmutableArray::idx(b);

        while let Some((b, node)) = iter.next() {

            let i = ImmutableArray::idx(b);
            let l = InternalNodes::idx(b);

            if i == prev_i {

                if l != prev_l {
                    internals.insert_leafs(prev_l, Arc::new(leafs));
                    leafs = Leafs::empty();
                }

            } else {
                internals.insert_leafs(prev_l, Arc::new(leafs));
                leafs = Leafs::empty();
                array[prev_i] = Some(Arc::new(internals));
                internals = InternalNodes::empty();
            }

            leafs.insert_node(Leafs::idx(b), node.clone());
            prev_l = l;
            prev_i = i;
        }

        internals.insert_leafs(prev_l, Arc::new(leafs));
        array[prev_i] = Some(Arc::new(internals));

        Self {
            items: array,
        }
    }

    fn insert(&self, byte: u8, key: &[u8], value: &[u8]) -> Self {

        let mut new_items = self.items.clone();
        let idx = Self::idx(&byte);
        let old_item = &new_items[idx];
        let new_item = if let Some(node) = old_item {
            node.insert(byte, key, value)
        } else {
            InternalNodes::new(byte, key, value)
        };
        new_items[idx] = Some(new_item);
        Self { items: new_items }
    }

    fn get(&self, byte: u8) -> Option<Arc<Node>> {
        match &self.items[Self::idx(&byte)] {
            Some(item) => item.get(byte),
            None => None,
        }
    }

    fn idx(byte: &u8) -> usize {
        (byte >> 6) as usize // top 2 bits (0..3)
    }

}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::ops::Deref;
    use crate::storage::trie::{ImmutableArray, Node, Trie};

    mod immutable_array {
        use super::*;                // ImmutableArray, Node, Leafs, Internals

        /// Simple reference implementation: each byte value indexes directly.
        #[derive(Clone)]
        struct Dense256([Option<Arc<Node>>; 256]);

        impl Dense256 {
            fn new(pairs: &[(u8, Arc<Node>)]) -> Self {
                let mut d = Self(std::array::from_fn(|_| None));
                for (b, n) in pairs {
                    d.0[*b as usize] = Some(n.clone());
                }
                d
            }
            fn insert(&self, b: u8, n: Arc<Node>) -> Self {
                let mut d = self.clone();
                d.0[b as usize] = Some(n);
                d
            }
            fn get(&self, b: u8) -> Option<Arc<Node>> {
                self.0[b as usize].clone()
            }
        }

        /// Helper that compares two optional `Arc<Node>`s by pointer, not by value.
        fn same(a: Option<Arc<Node>>, b: Option<Arc<Node>>) -> bool {
            match (a, b) {
                (Some(x), Some(y)) => &x == &y,
                (None, None) => true,
                _ => false,
            }
        }

        #[test]
        fn new_and_get() {
            // build 64 deterministic pairs so we exercise every bucket
            let pairs: Vec<_> = (0..=u8::MAX)
                .step_by(4)
                .map(|b| (b, Arc::new(Node::Leaf { value: Arc::from([b]) })))
                .collect();

            let array = ImmutableArray::new(pairs.clone());

            let mut iter = pairs.iter();
            let mut next = iter.next();

            for i in 0..=u8::MAX {
                if let Some((b, node)) = next {
                    if *b == i {
                        assert_eq!(Some(node.clone()), array.get(i));
                        next = iter.next();
                    } else {
                        assert_eq!(None, array.get(i));
                    }
                } else {
                    assert_eq!(None, array.get(i));
                }
            }
        }

        #[test]
        fn new() {

            let pairs: Vec<_> = (0..=u8::MAX)
                .map(|b| (b, Arc::new(Node::Leaf { value: Arc::from([b]) })))
                .collect();

            let mut array = ImmutableArray::new(pairs);
            for b in 0..=u8::MAX {
                array = array.insert(b, &[], &[b]);
            }

            for b in 0..=u8::MAX {
                assert_eq!(array.get(b), Some(Arc::new(Node::Leaf { value: Arc::from([b]) })));
            }
        }


        #[test]
        fn insert() {
            let mut array = ImmutableArray::empty();
            for b in 0..=u8::MAX {
                array = array.insert(b, &[], &[b]);
            }

            for b in 0..=u8::MAX {
                assert_eq!(array.get(b), Some(Arc::new(Node::Leaf { value: Arc::from([b]) })));
            }
        }

        #[test]
        fn insert_matches_dense() {

            let mut pairs: Vec<(u8, Arc<Node>)> = (0..128u8)
                .map(|b| (b, Arc::new(Node::Leaf { value: Arc::from([b]) })))
                .collect();

            let ia0 = ImmutableArray::new(pairs.clone());
            let dense0 = Dense256::new(&pairs);

            // insert 128 more keys, verifying after each step
            for b in 128u8..=255 {
                let n = Arc::new(Node::Leaf { value: Arc::from([b]) });
                pairs.push((b, n.clone()));

                let ia1 = ia0.insert(b, &[], &[b]);          // value doesn’t matter, compare pointers
                let dense1 = dense0.insert(b, n);

                for k in 0u8..=255 {
                    assert!(same(ia1.get(k), dense1.get(k)), "after inserting {:02X}", b);
                }

                // old array must be unchanged (COW)
                assert!(ia0.get(b).is_none(), "ia0 mutated by insert");
            }
        }

        #[test]
        fn updates_existing_slot() {
            let leaf1 = Arc::new(Node::Leaf { value: Arc::from(*b"X") });
            let leaf2 = Arc::new(Node::Leaf { value: Arc::from(*b"Y") });

            let array_original = ImmutableArray::new(vec![(42, leaf1.clone())]);
            let array_updated = array_original.insert(42, b"", b"Y");

            assert_ne!(array_original.get(42), array_updated.get(42));
            assert_eq!(array_original.get(42), Some(leaf1));
            assert_eq!(array_updated.get(42), Some(leaf2));
        }
    }

    mod trie {
        use super::*;
        #[test]
        fn insert() {
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

            let expected = prefix(b"8", sparse(vec![
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

            let expected = prefix(b"8", sparse(vec![
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

            let expected = prefix(b"8", split(vec![
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

            let expected = prefix(b"8", split(vec![
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

            let expected = prefix(b"8", split(vec![
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
        fn insert_into_empty_trie_with_empty_path() {
            let trie = Trie::new();
            trie.insert(b"", b"1");

            assert_tries_eq(&trie, leaf(b"1"));
        }

        #[test]
        fn insert_into_empty_trie() {
            let trie = Trie::new();
            trie.insert(b"path", b"1");

            assert_tries_eq(&trie, chain(b"path", leaf(b"1")));
        }

        #[test]
        fn insert_into_leaf() {
            let trie = Trie::new();
            trie.insert(b"", b"1");
            trie.insert(b"", b"2");

            assert_tries_eq(&trie, leaf(b"2"));

            trie.insert(b"path", b"3");

            assert_tries_eq(&trie, prefix(b"2", chain(b"path", leaf(b"3"))));
        }

        #[test]
        fn insert_into_sparse() {
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
        fn insert_into_split() {
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
        fn insert_into_prefix() {
            let trie = Trie::new();
            trie.insert(b"cobra", b"1");
            trie.insert(b"", b"2");

            assert_tries_eq(&trie, prefix(b"2", chain(b"cobra", leaf(b"1"))));

            trie.insert(b"", b"3");

            assert_tries_eq(&trie, prefix(b"3", chain(b"cobra", leaf(b"1"))));
        }

        #[test]
        fn insert_into_chain() {
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

        fn assert_tries_eq(trie: &Trie, expected: Arc<Node>) {
            assert_eq!(trie.root.load().deref(), &expected);
        }

        fn sparse(transitions: Vec<(u8, Arc<Node>)>) -> Arc<Node> {
            Arc::new(Node::Sparse { transitions })
        }

        fn chain(key: &[u8], value: Arc<Node>) -> Arc<Node> {
            Arc::new(Node::Chain { path: key.to_vec(), child: value })
        }

        fn leaf(value: &[u8]) -> Arc<Node> {
            Arc::new(Node::Leaf { value: Arc::from(value) })
        }

        fn prefix(content: &[u8], child: Arc<Node>) -> Arc<Node> {
            Arc::new(Node::Prefix { content: Arc::from(content), child })
        }

        fn split(transitions: Vec<(u8, Arc<Node>)>) -> Arc<Node> {
            Arc::new(Node::Split { children: ImmutableArray::new(transitions) })
        }
    }
}
