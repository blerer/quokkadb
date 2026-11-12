//! Configuration for database resources, durability, and storage behavior.
//!
//! Common configuration types for database resources, durability, and storage.

/// Database resource, durability, and storage-engine configuration.
pub mod options;
/// Types for expressing byte quantities in configuration values.
pub mod storage_quantity;

pub use options::{CompressorType, Options, WalDurability};
pub use storage_quantity::{StorageQuantity, StorageUnit};
