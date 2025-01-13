/// Represents different storage units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageUnit {
    /// Bytes (B).
    Bytes,
    /// Kibibytes (KiB, 1024 bytes).
    Kibibytes,
    /// Mebibytes (MiB, 1024^2 bytes).
    Mebibytes,
    /// Gibibytes (GiB, 1024^3 bytes).
    Gibibytes,
}

/// Represents a storage quantity, combining a value and a unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageQuantity {
    /// The numerical value of the storage quantity.
    value: usize,
    /// The unit associated with the value.
    unit: StorageUnit,
}

impl StorageQuantity {
    /// Creates a new `StorageQuantity` with the given value and unit.
    ///
    /// # Arguments
    ///
    /// * `value` - The numerical value of the storage quantity.
    /// * `unit` - The unit of the storage quantity.
    ///
    /// # Example
    ///
    /// <pre>
    ///
    /// use options::{StorageQuantity, StorageUnit};
    ///
    /// let size = StorageQuantity::new(1500, StorageUnit::Mebibytes);
    ///
    /// </pre>
    ///
    pub fn new(value: usize, unit: StorageUnit) -> Self {
        Self { value, unit }
    }

    /// Converts the storage quantity to bytes.
    ///
    /// # Returns
    ///
    /// A `u64` representing the equivalent value in bytes.
    ///
    /// # Example
    ///
    /// <pre>
    ///
    /// use your_crate::{StorageQuantity, StorageUnit};
    ///
    /// let size = StorageQuantity::new(1, StorageUnit::Mebibytes);
    /// assert_eq!(size.to_bytes(), 1024 * 1024);
    ///
    /// </pre>
    ///
    pub fn to_bytes(&self) -> usize {
        match self.unit {
            StorageUnit::Bytes => self.value,
            StorageUnit::Kibibytes => self.value * 1024,
            StorageUnit::Mebibytes => self.value * 1024 * 1024,
            StorageUnit::Gibibytes => self.value * 1024 * 1024 * 1024,
        }
    }

    /// Converts the storage quantity to a specified unit.
    ///
    /// # Arguments
    ///
    /// * `target_unit` - The unit to convert to.
    ///
    /// # Returns
    ///
    /// A `StorageQuantity` in the target unit. The value is truncated if the conversion results in a non-integer value.
    ///
    /// # Example
    ///
    /// <pre>
    ///
    /// use options::{StorageQuantity, StorageUnit};
    ///
    /// let size = StorageQuantity::new(1536, StorageUnit::Kibibytes);
    /// let size_in_mib = size.convert_to(StorageUnit::Mebibytes);
    /// assert_eq!(size_in_mib.value, 1);
    ///
    /// </pre>
    ///
    pub fn convert_to(&self, target_unit: StorageUnit) -> StorageQuantity {
        let bytes = self.to_bytes();
        let target_value = match target_unit {
            StorageUnit::Bytes => bytes,
            StorageUnit::Kibibytes => bytes / 1024,
            StorageUnit::Mebibytes => bytes / (1024 * 1024),
            StorageUnit::Gibibytes => bytes / (1024 * 1024 * 1024),
        };
        StorageQuantity::new(target_value, target_unit)
    }
}