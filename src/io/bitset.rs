use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;

/// A simple BitSet implementation using an u64 to store bits.
#[derive(Debug, PartialEq, Eq)]
pub struct BitSet(u64);

impl BitSet {
    pub fn new() -> Self {
        BitSet(0)
    }

    /// Insert a bit into the BitSet.
    /// # Arguments
    /// * `bit` - The bit position to insert (0-63).
    pub fn insert(&mut self, bit: u8) {
        self.0 |= 1 << bit;
    }

    /// Check if the BitSet contains a bit.
    pub fn contains(&self, bit: u8) -> bool {
        (self.0 & (1 << bit)) != 0
    }
}

impl Serializable for BitSet {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Self>
    where
        Self: Sized
    {
        reader.read_varint_u64().map(BitSet)
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.0);
    }
}

#[cfg(test)]
mod tests {
    use crate::io::serializable::check_serialization_round_trip;
    use super::*;

    #[test]
    fn new_bitset_is_empty() {
        let bs = BitSet::new();
        for i in 0..64 {
            assert!(!bs.contains(i), "bit {i} should not be set in new BitSet");
        }
    }

    #[test]
    fn insert_and_contains() {
        let mut bs = BitSet::new();
        bs.insert(0);
        bs.insert(5);
        bs.insert(63);

        assert!(bs.contains(0));
        assert!(bs.contains(5));
        assert!(bs.contains(63));

        assert!(!bs.contains(1));
        assert!(!bs.contains(4));
        assert!(!bs.contains(62));
    }

    #[test]
    fn insert_is_idempotent() {
        let mut bs = BitSet::new();
        bs.insert(10);
        bs.insert(10);
        assert!(bs.contains(10));
    }

    #[test]
    fn serialization_round_trip() {
        let mut bs = BitSet::new();
        bs.insert(0);
        bs.insert(31);
        bs.insert(63);

        check_serialization_round_trip(bs);
    }

    #[test]
    fn serialization_empty() {
        check_serialization_round_trip(BitSet::new());
    }
}