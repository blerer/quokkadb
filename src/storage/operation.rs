use crate::io::byte_reader::ByteReader;
use crate::io::varint;
use crate::storage::internal_key::{encode_internal_key, encode_record_key};
use std::io::{Error, ErrorKind};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum OperationType {
    /// Represent the min bound for the OperationTypes. Used when performing queries.
    MinKey,
    /// A `Put` operation adds or updates a key-value pair.
    Put,
    /// A `Delete` operation removes a key from the store.
    Delete,
    /// Represent the max bound for the OperationTypes. Used when performing queries.
    MaxKey,
}

/// At the storage level, for a given key and sequence DELETE operation override PUT operation.
/// This logic is enforced by the byte order where DELETE as a lower byte representation that PUT.
impl From<OperationType> for u8 {
    fn from(item: OperationType) -> Self {
        match item {
            OperationType::MinKey => u8::MAX,
            OperationType::Put => 0x50,
            OperationType::Delete => 0x30,
            OperationType::MaxKey => 0x00,
        }
    }
}

impl TryFrom<u8> for OperationType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            u8::MAX => Ok(OperationType::MinKey),
            0x50 => Ok(OperationType::Put),
            0x30 => Ok(OperationType::Delete),
            0x00 => Ok(OperationType::MaxKey),
            _ => Err("Invalid value for OperationType"),
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct Operation {
    /// The type of operation
    operation_type: OperationType,
    /// The collection ID
    collection: u32,
    /// The index ID
    index: u32,
    /// The BSON key formatted in a byte comparable way.
    user_key: Vec<u8>,
    /// The bytes representing the BSON document
    value: Vec<u8>,
}

impl Operation {
    pub fn new_put(collection: u32, index: u32, user_key: Vec<u8>, value: Vec<u8>) -> Operation {
        Operation {
            operation_type: OperationType::Put,
            collection,
            index,
            user_key,
            value,
        }
    }

    pub fn new_delete(collection: u32, index: u32, user_key: Vec<u8>) -> Operation {
        Operation {
            operation_type: OperationType::Delete,
            collection,
            index,
            user_key,
            value: Vec::with_capacity(0),
        }
    }

    pub fn internal_key(&self, sequence: u64) -> Vec<u8> {
        encode_internal_key(
            &encode_record_key(self.collection, self.index, &self.user_key),
            sequence,
            self.operation_type,
        )
    }
    pub fn operation_type(&self) -> OperationType {
        self.operation_type.clone()
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn wal_record_size(&self) -> usize {
        1 + varint::compute_u32_vint_size(self.collection)
            + varint::compute_u32_vint_size(self.index)
            + varint::compute_u64_vint_size(self.user_key.len() as u64)
            + self.user_key.len()
            + varint::compute_u64_vint_size(self.value.len() as u64)
            + self.value.len()
    }

    pub fn append_wal_record(&self, vec: &mut Vec<u8>) {
        vec.push(u8::from(self.operation_type));
        varint::write_u32(self.collection, vec);
        varint::write_u32(self.index, vec);
        varint::write_u64(self.user_key.len() as u64, vec);
        vec.extend_from_slice(&self.user_key);
        varint::write_u64(self.value.len() as u64, vec);
        vec.extend_from_slice(&self.value);
    }

    pub fn from_wal_record<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> std::io::Result<Operation> {
        let op_byte = reader.read_u8()?;
        let operation_type = OperationType::try_from(op_byte)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid operation type"))?;

        let collection = reader.read_varint_u32()?;
        let index = reader.read_varint_u32()?;

        let key_len = reader.read_varint_u64()? as usize;
        let user_key = reader.read_fixed_slice(key_len)?;

        let value_len = reader.read_varint_u64()? as usize;
        let value = reader.read_fixed_slice(value_len)?;

        Ok(Operation {
            operation_type,
            collection,
            index,
            user_key: user_key.to_vec(),
            value: value.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::byte_reader::ByteReader;

    #[test]
    fn test_operation_wal_round_trip_put() {
        let original = Operation::new_put(42, 7, b"key123".to_vec(), b"value456".to_vec());
        let mut buf = Vec::new();
        original.append_wal_record(&mut buf);

        let computed_size = Operation::wal_record_size(&original);
        assert_eq!(computed_size, buf.len());

        let reader = ByteReader::new(&buf);
        let decoded = Operation::from_wal_record(&reader).expect("Deserialization failed");

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_operation_wal_round_trip_delete() {
        let original = Operation::new_delete(99, 3, b"delete_me".to_vec());
        let mut buf = Vec::new();
        original.append_wal_record(&mut buf);

        let computed_size = Operation::wal_record_size(&original);
        assert_eq!(computed_size, buf.len());

        let reader = ByteReader::new(&buf);
        let decoded = Operation::from_wal_record(&reader).expect("Deserialization failed");

        assert_eq!(original, decoded);
    }
}
