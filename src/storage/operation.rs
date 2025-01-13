
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

impl From<OperationType> for u8 {
    fn from(item: OperationType) -> Self {
        match item {
            OperationType::MinKey => 0,
            OperationType::Put => 1,
            OperationType::Delete=> 2,
            OperationType::MaxKey => 255,
        }
    }
}

impl TryFrom<u8> for OperationType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(OperationType::MinKey),
            1 => Ok(OperationType::Put),
            2 => Ok(OperationType::Delete),
            255 => Ok(OperationType::MaxKey),
            _ => Err("Invalid value for OperationType"),
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct Operation {

    operation_type: OperationType,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Operation {
    pub fn new_put(key: Vec<u8>, value: Vec<u8>) -> Operation {
        Operation {
            operation_type: OperationType::Put,
            key,
            value,
        }
    }

    pub fn new_delete(key: Vec<u8>) -> Operation {
        Operation {
            operation_type: OperationType::Delete,
            key,
            value: vec![],
        }
    }

    pub fn deconstruct(self) -> (OperationType, Vec<u8>, Vec<u8>) {
        (self.operation_type, self.key, self.value)
    }
}
