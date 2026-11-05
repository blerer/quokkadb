use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use crate::io::size_estimate::SizeEstimate;
use std::fmt;
use std::io::Result;

/// An endpoint of a query interval, distinct from the expression it contains.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum QueryEndpoint<T> {
    Value(T),
}

impl<T> QueryEndpoint<T> {
    pub(crate) fn value(&self) -> &T {
        match self {
            Self::Value(value) => value,
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for QueryEndpoint<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value().fmt(f)
    }
}

impl<T: SizeEstimate> SizeEstimate for QueryEndpoint<T> {
    fn estimated_heap_size(&self) -> usize {
        self.value().estimated_heap_size()
    }
}

impl<T: Serializable> Serializable for QueryEndpoint<T> {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>, version: u32) -> Result<Self> {
        match reader.read_u8()? {
            0 => Ok(Self::Value(T::read_from(reader, version)?)),
            tag => unreachable!("Invalid query endpoint tag: {tag}"),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter, version: u32) {
        match self {
            Self::Value(value) => {
                writer.write_u8(0);
                value.write_to(writer, version);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Expr;
    use crate::query::expr_fn::{lit, placeholder};
    use crate::query::tree_node::TreeNode;
    use crate::util::interval::Interval;
    use std::ops::{Bound, RangeBounds};
    use std::sync::Arc;

    #[test]
    fn endpoint_serialization_round_trip() {
        let endpoint = QueryEndpoint::Value(placeholder(3));
        let mut writer = ByteWriter::new();
        endpoint.write_to(&mut writer, 0);
        let bytes = writer.take_buffer();
        assert_eq!(bytes[0], 0);
        let decoded = QueryEndpoint::<Arc<Expr>>::read_from(&ByteReader::new(bytes), 0).unwrap();
        assert_eq!(decoded, endpoint);
    }
}
