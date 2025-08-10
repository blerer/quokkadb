use std::collections::BTreeMap;
use std::hash::Hash;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;

/// A component that can be serialized using the write_to method and deserialized using
/// the read_from method.
pub trait Serializable {
    /// Deserialized the component from the specified ByteReader
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self>
    where
        Self: Sized;

    /// Serialize the component into the specified ByteWriter
    fn write_to(&self, writer: &mut ByteWriter);
}

impl Serializable for i32 {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        reader.read_varint_i32()
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_i32(*self);
    }
}

impl Serializable for u64 {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        reader.read_varint_u64()
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(*self);
    }
}

impl Serializable for usize {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        Ok(reader.read_varint_u64()? as usize)
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(*self as u64);
    }
}

impl<T> Serializable for Option<T>
where
    T: Serializable,
{
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let presence = reader.read_u8()?;
        if presence == 1 {
            let value = T::read_from(reader)?;
            Ok(Some(value))
        } else if presence == 0 {
            Ok(None)
        } else {
            Err(Error::new(ErrorKind::InvalidData, "Invalid option presence byte"))
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            Some(value) => {
                writer.write_u8(1); // Indicate presence
                value.write_to(writer);
            }
            None => {
                writer.write_u8(0); // Indicate absence
            }
        }
    }
}

impl<T> Serializable for Vec<T>
where
    T: Serializable,
{
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let length = reader.read_varint_u64()? as usize;
        let mut vec = Vec::with_capacity(length);
        for _ in 0..length {
            vec.push(T::read_from(reader)?);
        }
        Ok(vec)
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.len() as u64);
        for item in self {
            item.write_to(writer);
        }
    }
}

impl<K, V> Serializable for BTreeMap<K, V>
where
    K: Eq + Hash + Ord + Serializable, V: Serializable,
{
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let length = reader.read_varint_u64()? as usize;
        let mut map = BTreeMap::new();
        for _ in 0..length {
            map.insert(K::read_from(reader)?, V::read_from(reader)?);
        }
        Ok(map)
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        writer.write_varint_u64(self.len() as u64);
        for (key, value) in self {
            key.write_to(writer);
            value.write_to(writer);
        }
    }
}

impl<T> Serializable for Arc<T>
where
    T: Serializable,
{
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        Ok(Arc::new(T::read_from(reader)?))
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        (**self).write_to(writer);
    }
}

#[cfg(test)]
pub fn check_serialization_round_trip<T>(element: T)
where
    T: std::fmt::Debug + PartialEq + Serializable,
{
    // Serialize the element.
    let mut writer = ByteWriter::new();
    element.write_to(&mut writer);
    let bytes = writer.take_buffer();

    // Deserialize the element from the serialized bytes.
    let reader = ByteReader::new(&bytes);
    let deserialized = Serializable::read_from(&reader).unwrap();

    assert_eq!(element, deserialized);

    // Re-serialize the deserialized element.
    let mut writer2 = ByteWriter::new();
    deserialized.write_to(&mut writer2);
    let bytes2 = writer2.take_buffer();

    // Verify that the round-trip serialization is lossless.
    assert_eq!(bytes, bytes2);
}
