use std::path::Path;
use crate::io::file_name_as_str;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    Manifest,
    WriteAheadLog,
    SST,
}

#[derive(Debug)]
pub struct DbFile {
    pub file_type: FileType,
    pub id: u64,
}

impl DbFile {
    pub fn new(path: &Path) -> Option<DbFile> {
        let filename = file_name_as_str(path)?;

        if let Some(num_str) = filename.strip_prefix("MANIFEST-") {
            Some(DbFile { file_type: FileType::Manifest, id: num_str.parse().ok()? })
        } else if let Some(num_str) = filename.strip_suffix(".log") {
            Some(DbFile { file_type: FileType::WriteAheadLog, id: num_str.parse().ok()? })
        } else if let Some(num_str) = filename.strip_suffix(".sst") {
            Some(DbFile { file_type: FileType::SST, id: num_str.parse().ok()? })
        } else {
            None
        }
    }

    pub fn new_write_ahead_log(id: u64) -> DbFile {
        DbFile { file_type: FileType::WriteAheadLog, id }
    }

    pub fn new_manifest(id: u64) -> DbFile {
        DbFile { file_type: FileType::Manifest, id }
    }

    pub fn new_sst(id: u64) -> DbFile {
        DbFile { file_type: FileType::SST, id }
    }

    pub fn filename(&self) -> String {
        match self.file_type {
            FileType::Manifest => format!("MANIFEST-{:06}", self.id),
            FileType::WriteAheadLog => format!("{:06}.log", self.id),
            FileType::SST => format!("{:06}.sst", self.id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_db_file_parsing_manifest() {
        let path = Path::new("MANIFEST-000123");
        let db_file = DbFile::new(path).expect("should parse");
        assert_eq!(db_file.file_type, FileType::Manifest);
        assert_eq!(db_file.id, 123);
        assert_eq!(db_file.filename(), "MANIFEST-000123");
    }

    #[test]
    fn test_db_file_parsing_log() {
        let path = Path::new("000045.log");
        let db_file = DbFile::new(path).expect("should parse");
        assert_eq!(db_file.file_type, FileType::WriteAheadLog);
        assert_eq!(db_file.id, 45);
        assert_eq!(db_file.filename(), "000045.log");
    }

    #[test]
    fn test_db_file_parsing_sst() {
        let path = Path::new("000888.sst");
        let db_file = DbFile::new(path).expect("should parse");
        assert_eq!(db_file.file_type, FileType::SST);
        assert_eq!(db_file.id, 888);
        assert_eq!(db_file.filename(), "000888.sst");
    }

    #[test]
    fn test_db_file_parsing_invalid() {
        let path = Path::new("unknown.file");
        assert!(DbFile::new(path).is_none());
    }
}