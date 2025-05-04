use std::sync::Arc;
use arc_swap::ArcSwap;
use smallvec::{smallvec, SmallVec};

/// Represents a node in the compressed trie.
///
/// - `Chain`: A compressed path of multiple bytes leading to a child.
/// - `Sparse`: A node with a small number of children (sorted transitions).
/// - `Split`: A node with many children (indexed directly by byte).
/// - `Leaf`: A terminal node holding a value.
/// - `Prefix`: A node that has both a value and continues with children.
#[derive(Clone, Debug, PartialEq)]
enum Node {
    /// A compressed path fragment with a single child.
    ChainMany {
        path: Arc<[u8]>,
        child: Box<Node>,
    },
    ChainOne {
        path: u8,
        child: Box<Node>,
    },
    /// A node with multiple children (up to a threshold).
    Sparse {
        transitions: SmallVec<[(u8, Arc<Node>); 7]>,
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
        child: Box<Node>,
    },
    Empty,
}

impl Node {

    fn new(key: &[u8], value: &[u8]) -> Node {
        let leaf = Node::Leaf { value: Arc::from(value) };
        if key.is_empty() {
            leaf
        } else {
            Self::new_chain(key, leaf)
        }
    }

    fn new_chain(path: &[u8], child: Node) -> Node {
        if path.len() == 1 {
            Node::ChainOne { path: path[0], child: Box::new(child) }
        } else {
            Node::ChainMany { path: Arc::from(path), child: Box::new(child) }
        }
    }

    #[inline(never)]
    fn insert(&self, key: &[u8], value: &[u8]) -> Node {

        if key.is_empty() {
            return match self {
                Node::Prefix { content: _, child } => {
                    Node::Prefix {
                        content: Arc::from(value),
                        child: child.clone(),
                    }
                },
                Node::Leaf { value: _ } | Node::Empty => {
                    Node::Leaf { value: Arc::from(value) }
                },
                _ => {
                    Node::Prefix { content: Arc::from(value), child: Box::new(self.clone()) }
                }
            }
        }

        match self {
            Node::Empty => {
                Node::new(key, value)
            },
            Node::Sparse { transitions } => {
                Self::insert_into_sparse(&key, value, transitions)
            }
            Node::ChainOne { path, child} => {
                Self::insert_into_chain_one(&key, value, path, child)
            },
            Node::ChainMany { path, child } => {
                Self::insert_into_chain_many(&key, value, &path, child)
            },
            Node::Leaf { value: current_value } => {
                Self::insert_into_leaf(key, value, current_value)
            },
            Node::Prefix { content , child } => {
                Self::insert_into_prefix(key, value, content, child)
            },
            Node::Split { children } => {
                Self::insert_into_split(&key, value, children)
            },
        }
    }

    fn insert_into_split(key: &&[u8], value: &[u8], children: &ImmutableArray) -> Node {
        Node::Split {
            children: children.insert(key[0], &key[1..], value),
        }
    }

    fn insert_into_prefix(key: &[u8], value: &[u8], content: &Arc<[u8]>, child: &Node) -> Node {
        Node::Prefix {
            content: content.clone(),
            child: Box::new(child.insert(key, value)),
        }
    }

    fn insert_into_leaf(key: &[u8], value: &[u8], current_value: &Arc<[u8]>) -> Node {
        Node::Prefix {
            content: current_value.clone(),
            child: Box::new(Node::new(key, value)),
        }
    }

    fn insert_into_chain_many(key: &&[u8], value: &[u8], path: &Arc<[u8]>, child: &Node) -> Node {
        let common = path.iter()
            .zip(key.iter())
            .take_while(|(a, b)| a == b)
            .count();

        if common == path.len() {
            Node::ChainMany {
                path: path.clone(),
                child: Box::new(child.insert(&key[common..], value))
            }
        } else if common == key.len() {

            // The key end within the chain. We need to break the chain to introduce
            // a prefix
            let new_child = Self::new_chain(&path[common..], child.clone());
            let new_child = Node::Prefix { content: Arc::from(value), child: Box::new(new_child) };
            Self::new_chain(&path[..common], new_child)
        } else {

            // Need to split the chain
            let mut transitions = smallvec![
                        (path[common], Arc::new(Self::new_chain(&path[common + 1..], child.clone()))),
                        (key[common], Arc::new(Self::new(&key[common + 1..], value)))
                    ];
            transitions.sort_unstable_by_key(|(b, _)| *b);

            let mut new_node = Node::Sparse { transitions };

            if common != 0 {
                new_node = Self::new_chain(&path[..common], new_node);
            }

            new_node
        }
    }

    fn insert_into_chain_one(key: &&[u8], value: &[u8], path: &u8, child: &Node) -> Node {
        if *path == key[0] {
            Node::ChainOne { path: *path, child: Box::new(child.insert(&key[1..], value)) }
        } else {
            // Need to split the chain
            let mut transitions = smallvec![
                        (*path, Arc::new(child.clone())),
                        (key[0], Arc::new(Self::new(&key[1..], value))),
                    ];
            transitions.sort_unstable_by_key(|(b, _)| *b);

            Node::Sparse { transitions }
        }
    }

    fn insert_into_sparse(key: &&[u8], value: &[u8], transitions: &SmallVec<[(u8, Arc<Node>); 7]>) -> Node {
        let mut new_transitions = SmallVec::with_capacity(transitions.len() + 1);
        let mut inserted = false;
        for (b, node) in transitions.iter() {
            if b == &key[0] {
                new_transitions.push((*b, Arc::new(node.insert(&key[1..], value))));
                inserted = true;
            } else {
                if !inserted && b > &key[0] {
                    new_transitions.push((key[0], Arc::new(Node::new(&key[1..], value))));
                    inserted = true;
                }
                new_transitions.push((*b, node.clone()));
            }
        }

        if !inserted {
            new_transitions.push((key[0], Arc::new(Node::new(&key[1..], value))));
        }

        if new_transitions.len() <= 6 {
            Node::Sparse { transitions: new_transitions }
        } else {
            Node::Split { children: ImmutableArray::new(new_transitions) }
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

        self.root.store(Arc::new(old.insert(key, value)));
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
        items[Self::idx(&byte)] = Some(Arc::new(Node::new(key, value)));
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
        new_items[idx] = Some(Arc::new(new_item));
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

#[derive(Clone, Debug, PartialEq)]
struct ImmutableArray {
    items: [Option<Arc<InternalNodes>>; 4]
}

impl ImmutableArray {

    fn empty() -> Self {
        ImmutableArray { items: std::array::from_fn(|_| None) }
    }

    fn new(nodes: SmallVec<[(u8, Arc<Node>); 7]>) -> Self {

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
        use smallvec::{smallvec, SmallVec};
        use super::*;                // ImmutableArray, Node, Leafs, Internals

        #[test]
        fn new_and_get() {
            // build 64 deterministic pairs so we exercise every bucket
            let pairs: SmallVec<_> = (0..=u8::MAX)
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

            let pairs: SmallVec<_> = (0..=u8::MAX)
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
        fn updates_existing_slot() {
            let leaf1 = Arc::new(Node::Leaf { value: Arc::from(*b"X") });
            let leaf2 = Arc::new(Node::Leaf { value: Arc::from(*b"Y") });

            let array_original = ImmutableArray::new(smallvec![(42, leaf1.clone())]);
            let array_updated = array_original.insert(42, b"", b"Y");

            assert_ne!(array_original.get(42), array_updated.get(42));
            assert_eq!(array_original.get(42), Some(leaf1));
            assert_eq!(array_updated.get(42), Some(leaf2));
        }
    }

    mod trie {
        use smallvec::{smallvec, SmallVec};
        use super::*;
        #[test]
        fn insert() {
            let trie = Trie::new();
            trie.insert(b"cat", b"1");

            let expected = chain(b"cat", leaf(b"1"));

            assert_tries_eq(&trie, expected);

            trie.insert(b"dog", b"2");

            let expected = sparse(smallvec![
                (b'c', chain(b"at", leaf(b"1"))),
                (b'd', chain(b"og", leaf(b"2")))
            ]);

            assert_tries_eq(&trie, expected);

            trie.insert(b"monkey", b"3");

            let expected = sparse(smallvec![
                (b'c', chain(b"at", leaf(b"1"))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'm', chain(b"onkey", leaf(b"3")))
            ]);

            assert_tries_eq(&trie, expected);

            trie.insert(b"mountain lion", b"4");

            let expected = sparse(smallvec![
                (b'c', chain(b"at", leaf(b"1"))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain lion", leaf(b"4"))),
                ]))),
            ]);

            assert_tries_eq(&trie, expected);

            trie.insert(b"caterpillar", b"5");

            let expected = sparse(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain lion", leaf(b"4"))),
                ]))),
            ]);

            assert_tries_eq(&trie, expected);

            trie.insert(b"kangaroo", b"6");

            let expected = sparse(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain lion", leaf(b"4"))),
                ]))),
            ]);

            assert_tries_eq(&trie, expected);

            trie.insert(b"mountain", b"7");

            let expected = sparse(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
                ]))),
            ]);

            assert_tries_eq(&trie, expected);

            trie.insert(b"", b"8");

            let expected = prefix(b"8", sparse(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
                ]))),
            ]));

            assert_tries_eq(&trie, expected);

            trie.insert(b"giraffe", b"9");
            trie.insert(b"zebra", b"10");

            let expected = prefix(b"8", sparse(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'g', chain(b"iraffe", leaf(b"9"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
                ]))),
                (b'z', chain(b"ebra", leaf(b"10"))),
            ]));

            assert_tries_eq(&trie, expected);

            trie.insert(b"snake", b"11");

            let expected = prefix(b"8", split(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'g', chain(b"iraffe", leaf(b"9"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
                ]))),
                (b's', chain(b"nake", leaf(b"11"))),
                (b'z', chain(b"ebra", leaf(b"10"))),
            ]));

            assert_tries_eq(&trie, expected);

            trie.insert(b"quokka", b"12");

            let expected = prefix(b"8", split(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'g', chain(b"iraffe", leaf(b"9"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain", prefix(b"7", chain(b" lion", leaf(b"4"))))),
                ]))),
                (b'q', chain(b"uokka", leaf(b"12"))),
                (b's', chain(b"nake", leaf(b"11"))),
                (b'z', chain(b"ebra", leaf(b"10"))),
            ]));

            assert_tries_eq(&trie, expected);

            trie.insert(b"mountain goat", b"13");

            let expected = prefix(b"8", split(smallvec![
                (b'c', chain(b"at", prefix(b"1", chain(b"erpillar", leaf(b"5"))))),
                (b'd', chain(b"og", leaf(b"2"))),
                (b'g', chain(b"iraffe", leaf(b"9"))),
                (b'k', chain(b"angaroo", leaf(b"6"))),
                (b'm', chain(b"o", sparse(smallvec![
                    (b'n', chain(b"key", leaf(b"3"))),
                    (b'u', chain(b"ntain", prefix(b"7", chain(b" ", sparse(smallvec![
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

            assert_tries_eq(&trie, sparse(smallvec![
                (b'c', chain(b"obra", leaf(b"1"))),
                (b'p', chain(b"ython", leaf(b"2"))),
            ]));

            trie.insert(b"mamba", b"3");

            assert_tries_eq(&trie, sparse(smallvec![
                (b'c', chain(b"obra", leaf(b"1"))),
                (b'm', chain(b"amba", leaf(b"3"))),
                (b'p', chain(b"ython", leaf(b"2"))),
            ]));

            trie.insert(b"boa", b"4");

            assert_tries_eq(&trie, sparse(smallvec![
                (b'b', chain(b"oa", leaf(b"4"))),
                (b'c', chain(b"obra", leaf(b"1"))),
                (b'm', chain(b"amba", leaf(b"3"))),
                (b'p', chain(b"ython", leaf(b"2"))),
            ]));

            trie.insert(b"sidewinder", b"5");

            assert_tries_eq(&trie, sparse(smallvec![
                (b'b', chain(b"oa", leaf(b"4"))),
                (b'c', chain(b"obra", leaf(b"1"))),
                (b'm', chain(b"amba", leaf(b"3"))),
                (b'p', chain(b"ython", leaf(b"2"))),
                (b's', chain(b"idewinder", leaf(b"5"))),
            ]));

            trie.insert(b"copperhead", b"6");

            assert_tries_eq(&trie, sparse(smallvec![
                (b'b', chain(b"oa", leaf(b"4"))),
                (b'c', chain(b"o", sparse(smallvec![
                    (b'b', chain(b"ra", leaf(b"1"))),
                    (b'p', chain(b"perhead", leaf(b"6"))),
                ]))),
                (b'm', chain(b"amba", leaf(b"3"))),
                (b'p', chain(b"ython", leaf(b"2"))),
                (b's', chain(b"idewinder", leaf(b"5"))),
            ]));

            trie.insert(b"", b"7");

            assert_tries_eq(&trie, prefix(b"7", sparse(smallvec![
                (b'b', chain(b"oa", leaf(b"4"))),
                (b'c', chain(b"o", sparse(smallvec![
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

            assert_tries_eq(&trie, split(smallvec![
                (b'b', chain(b"oa", leaf(b"1"))),
                (b'c', chain(b"obra", leaf(b"2"))),
                (b'f', chain(b"er-de-lance", leaf(b"3"))),
                (b'm', chain(b"amba", leaf(b"4"))),
                (b'p', chain(b"ython", leaf(b"5"))),
                (b's', chain(b"idewinder", leaf(b"6"))),
                (b't', chain(b"aipan", leaf(b"7"))),
            ]));

            trie.insert(b"copperhead", b"8");

            assert_tries_eq(&trie, split(smallvec![
                (b'b', chain(b"oa", leaf(b"1"))),
                (b'c', chain(b"o", sparse(smallvec![
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

            assert_tries_eq(&trie, split(smallvec![
                (b'a', chain(b"sian cobra", leaf(b"9"))),
                (b'b', chain(b"oa", leaf(b"1"))),
                (b'c', chain(b"o", sparse(smallvec![
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

            assert_tries_eq(&trie, split(smallvec![
                (b'a', chain(b"sian cobra", leaf(b"9"))),
                (b'b', chain(b"oa", leaf(b"1"))),
                (b'c', chain(b"o", sparse(smallvec![
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

            assert_tries_eq(&trie, split(smallvec![
                (b'a', chain(b"sian cobra", leaf(b"9"))),
                (b'b', chain(b"oa", leaf(b"1"))),
                (b'c', chain(b"o", sparse(smallvec![
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

            assert_tries_eq(&trie, prefix(b"12", split(smallvec![
                (b'a', chain(b"sian cobra", leaf(b"9"))),
                (b'b', chain(b"oa", leaf(b"1"))),
                (b'c', chain(b"o", sparse(smallvec![
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

            assert_tries_eq(&trie, chain(b"co", sparse(smallvec![
                (b'b', chain(b"ra", leaf(b"1"))),
                (b'm', chain(b"mon european viper", leaf(b"2"))),
            ])));

            trie.insert(b"common water snake", b"3");

            assert_tries_eq(&trie, chain(b"co", sparse(smallvec![
                (b'b', chain(b"ra", leaf(b"1"))),
                (b'm', chain(b"mon ", sparse(smallvec![
                    (b'e', chain(b"uropean viper", leaf(b"2"))),
                    (b'w', chain(b"ater snake", leaf(b"3"))),
                ]))),
            ])));
        }

        fn assert_tries_eq(trie: &Trie, expected: Node) {
            assert_eq!(trie.root.load().deref(), &Arc::new(expected));
        }

        fn sparse(transitions: SmallVec<[(u8, Node); 7]>) -> Node {
            let transitions = transitions.into_iter().map(|(k, v)| (k, Arc::new(v))).collect::<SmallVec<_>>();
            Node::Sparse { transitions }
        }

        fn chain(key: &[u8], value: Node) -> Node {
            Node::new_chain(key, value)
        }

        fn leaf(value: &[u8]) -> Node {
            Node::Leaf { value: Arc::from(value) }
        }

        fn prefix(content: &[u8], child: Node) -> Node {
            Node::Prefix { content: Arc::from(content), child: Box::new(child) }
        }

        fn split(transitions: SmallVec<[(u8, Node); 7]>) -> Node {
            let transitions = transitions.into_iter().map(|(k, v)| (k, Arc::new(v))).collect::<SmallVec<_>>();
            Node::Split { children: ImmutableArray::new(transitions) }
        }
    }
}
