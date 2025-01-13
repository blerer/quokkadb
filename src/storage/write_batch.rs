use std::io::Result;
use crate::io::byte_reader::ByteReader;
use crate::io::varint;
use crate::storage::operation::Operation;

#[derive(Default, Debug)]
pub struct WriteBatch {
    operations: Vec<Operation>,
    precomputed_wal_record: Option<Vec<u8>>,
}

impl WriteBatch {

    pub fn new(operations: Vec<Operation>) -> WriteBatch {
        let precomputed_wal_record = Some(Self::precompute_wal_record(&operations));
        WriteBatch {
            operations,
            precomputed_wal_record,
        }
    }

    pub fn operations(&self) -> &[Operation] {
        &self.operations
    }

    pub fn to_wal_record(&self, seq: u64) -> Vec<u8> {
        let precomputed_wal_record  = if self.precomputed_wal_record.is_none() {
            &Self::precompute_wal_record(&self.operations)
        } else {
            self.precomputed_wal_record.as_ref().unwrap()
        };
        let mut vec = Vec::with_capacity(8 + precomputed_wal_record.len());
        vec.extend_from_slice(&seq.to_be_bytes());
        vec.extend_from_slice(precomputed_wal_record);
        vec

    }

    fn precompute_wal_record(operations: &[Operation]) -> Vec<u8> {
        let mut record_size = varint::compute_u64_vint_size(operations.len() as u64);
        for operation in operations {
            record_size += operation.wal_record_size();
        }
        let mut vec = Vec::with_capacity(record_size);
        varint::write_u64(operations.len() as u64, &mut vec);
        for operation in operations {
            operation.append_wal_record(&mut vec);
        }
        vec
    }

    pub fn from_wal_record(bytes: &[u8]) -> Result<Self> {
        let reader = ByteReader::new(bytes);
        let nbr_operations = reader.read_varint_u64()? as usize;
        let mut operations = Vec::with_capacity(nbr_operations);
        for _ in 0..nbr_operations {
            operations.push(Operation::from_wal_record(&reader)?);
        }

        Ok(WriteBatch {
            operations,
            precomputed_wal_record: None,
        })
    }
}

impl PartialEq for WriteBatch {
    fn eq(&self, other: &Self) -> bool {
        self.operations == other.operations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::operation::Operation;

    #[test]
    fn test_write_batch_wal_round_trip() {
        let batch = WriteBatch::new(vec![
            Operation::new_put(10, 5, b"key1".to_vec(), b"value1".to_vec()),
            Operation::new_delete(20, 6, b"key2".to_vec()),
        ]);

        let seq = 12345;
        let wal = batch.to_wal_record(seq);

        // Decode skipping the first 8 bytes (sequence number)
        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }

    #[test]
    fn test_empty_write_batch_round_trip() {
        let batch = WriteBatch::new(vec![]);

        let seq = 0;
        let wal = batch.to_wal_record(seq);

        let decoded = WriteBatch::from_wal_record(&wal[8..]).expect("Deserialization failed");

        assert_eq!(batch, decoded);
    }
}
