use crate::storage::operation::Operation;

pub struct WriteBatch {
    pub sequence_number : Option<u64>,
    pub operations: Vec<Operation>,
}

impl WriteBatch {

    pub fn new(sequence_number: u64, operations: Vec<Operation>) -> WriteBatch {
        WriteBatch {
            sequence_number: Some(sequence_number),
            operations,
        }
    }

}