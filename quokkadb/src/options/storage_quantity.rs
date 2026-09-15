use std::fmt;

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

impl fmt::Display for StorageQuantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let unit_str = match self.unit {
            StorageUnit::Bytes => "B",
            StorageUnit::Kibibytes => "KiB",
            StorageUnit::Mebibytes => "MiB",
            StorageUnit::Gibibytes => "GiB",
        };
        write!(f, "{} {}", self.value, unit_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_bytes() {
        assert_eq!(StorageQuantity::new(1, StorageUnit::Bytes).to_bytes(), 1);
        assert_eq!(
            StorageQuantity::new(1, StorageUnit::Kibibytes).to_bytes(),
            1024
        );
        assert_eq!(
            StorageQuantity::new(1, StorageUnit::Mebibytes).to_bytes(),
            1024 * 1024
        );
        assert_eq!(
            StorageQuantity::new(1, StorageUnit::Gibibytes).to_bytes(),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_convert_to_same_unit() {
        let sq = StorageQuantity::new(42, StorageUnit::Mebibytes);
        assert_eq!(sq.convert_to(StorageUnit::Mebibytes), sq);
    }

    #[test]
    fn test_convert_to_lower_unit() {
        let sq = StorageQuantity::new(1, StorageUnit::Mebibytes);
        let converted = sq.convert_to(StorageUnit::Kibibytes);
        assert_eq!(
            converted,
            StorageQuantity::new(1024, StorageUnit::Kibibytes)
        );
    }

    #[test]
    fn test_convert_to_higher_unit_truncation() {
        let sq = StorageQuantity::new(1536, StorageUnit::Kibibytes); // = 1.5 MiB
        let converted = sq.convert_to(StorageUnit::Mebibytes);
        assert_eq!(converted, StorageQuantity::new(1, StorageUnit::Mebibytes)); // truncates
    }

    #[test]
    fn test_display_formatting() {
        assert_eq!(
            format!("{}", StorageQuantity::new(42, StorageUnit::Bytes)),
            "42 B"
        );
        assert_eq!(
            format!("{}", StorageQuantity::new(1, StorageUnit::Kibibytes)),
            "1 KiB"
        );
        assert_eq!(
            format!("{}", StorageQuantity::new(5, StorageUnit::Mebibytes)),
            "5 MiB"
        );
        assert_eq!(
            format!("{}", StorageQuantity::new(2, StorageUnit::Gibibytes)),
            "2 GiB"
        );
    }
}
