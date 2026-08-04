use crate::io::byte_reader::ByteReader;
use crate::io::byte_writer::ByteWriter;
use crate::io::serializable::Serializable;
use std::collections::BTreeMap;
use std::io::Result;


mod code {
    pub const COLLECTION: u8 = 0;
    pub const INDEX: u8 = 1;
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum CountStatsKey {
    Collection(u32),
    Index { collection: u32, index: u32 },
}

impl Serializable for CountStatsKey {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        match reader.read_u8()? {
            code::COLLECTION => Ok(CountStatsKey::Collection(reader.read_varint_u32()?)),
            code::INDEX => Ok(CountStatsKey::Index {
                collection: reader.read_varint_u32()?,
                index: reader.read_varint_u32()?,
            }),
            _ => unreachable!("Invalid CountStatsKey tag"),
        }
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        match self {
            CountStatsKey::Collection(collection) => {
                writer.write_u8(code::COLLECTION).write_varint_u32(*collection);
            }
            CountStatsKey::Index { collection, index } => {
                writer
                    .write_u8(code::INDEX)
                    .write_varint_u32(*collection)
                    .write_varint_u32(*index);
            }
        }
    }
}

/// Total-count cardinality deltas aggregated per collection and per index.
///
/// This is intentionally closer to `sqlite_stat1` than to a full per-prefix
/// distribution model: the first version only tracks bounded total counts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct CountStats {
    pub deltas: BTreeMap<CountStatsKey, i64>,
}

impl CountStats {
    pub fn new(deltas: BTreeMap<CountStatsKey, i64>) -> Self {
        assert!(
            deltas.values().all(|delta| *delta != 0),
            "CountStats must not contain zero-valued deltas"
        );

        Self { deltas }
    }

    pub fn count_stat(&self, key: &CountStatsKey) -> Option<i64> {
        self.deltas.get(key).copied()
    }
}

pub(crate) trait CountStatSource {
    fn count_stat(&self, key: &CountStatsKey) -> Option<i64>;
}

impl CountStatSource for CountStats {
    fn count_stat(&self, key: &CountStatsKey) -> Option<i64> {
        self.count_stat(key)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CountStatsBuilder {
    deltas: BTreeMap<CountStatsKey, i64>,
}

impl CountStatsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_collection(&mut self, collection: u32, delta: i64) {
        self.inc(CountStatsKey::Collection(collection), delta);
    }

    pub fn inc_index(&mut self, collection: u32, index: u32, delta: i64) {
        self.inc(CountStatsKey::Index { collection, index }, delta);
    }

    fn inc(&mut self, key: CountStatsKey, delta: i64) {
        assert_ne!(delta, 0, "CountStatsBuilder delta must not be zero");
        let entry = self.deltas.entry(key.clone()).or_default();
        *entry += delta;
        if *entry == 0 {
            self.deltas.remove(&key);
        }
    }

    pub fn build(self) -> CountStats {
        CountStats::new(self.deltas)
    }
}

impl Serializable for CountStats {
    fn read_from<B: AsRef<[u8]>>(reader: &ByteReader<B>) -> Result<Self> {
        let deltas = BTreeMap::<CountStatsKey, i64>::read_from(reader)?;
        Ok(CountStats::new(deltas))
    }

    fn write_to(&self, writer: &mut ByteWriter) {
        self.deltas.write_to(writer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::serializable::check_serialization_round_trip;

    #[test]
    fn count_stats_key_round_trip() {
        check_serialization_round_trip(CountStatsKey::Collection(12));
        check_serialization_round_trip(CountStatsKey::Index {
            collection: 12,
            index: 4,
        });
    }

    #[test]
    fn count_stats_round_trip() {
        let deltas = BTreeMap::from([
            (CountStatsKey::Collection(7), 3),
            (CountStatsKey::Collection(8), -2),
            (
                CountStatsKey::Index {
                    collection: 7,
                    index: 2,
                },
                -1,
            ),
            (
                CountStatsKey::Index {
                    collection: 8,
                    index: 5,
                },
                4,
            ),
        ]);

        check_serialization_round_trip(CountStats::new(deltas));
    }

    #[test]
    #[should_panic(expected = "CountStats must not contain zero-valued deltas")]
    fn count_stats_rejects_zero_delta() {
        let deltas = BTreeMap::from([(CountStatsKey::Collection(1), 0)]);
        let _ = CountStats::new(deltas);
    }

    #[test]
    fn count_stats_builder_drops_zeroed_entries() {
        let mut builder = CountStatsBuilder::new();
        builder.inc_collection(7, 1);
        builder.inc_collection(7, -1);
        builder.inc_index(7, 2, 3);
        builder.inc_index(7, 2, -3);

        assert_eq!(builder.build(), CountStats::default());
    }

}
