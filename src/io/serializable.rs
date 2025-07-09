use std::io::Result;
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
    let deserialized = Serializable::read_from(&reader).expect("Deserialization should succeed");

    assert_eq!(element, deserialized);

    // Re-serialize the deserialized element.
    let mut writer2 = ByteWriter::new();
    deserialized.write_to(&mut writer2);
    let bytes2 = writer2.take_buffer();

    // Verify that the round-trip serialization is lossless.
    assert_eq!(bytes, bytes2);
}