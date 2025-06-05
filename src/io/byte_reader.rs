use crate::io::varint;
use crate::io::ZeroCopy;
use std::cell::Cell;
use std::io::{Error, ErrorKind, Result};

pub struct ByteReader<B: AsRef<[u8]>> {
    buffer: B,
    position: Cell<usize>,
}

impl<B: AsRef<[u8]>> ByteReader<B> {
    pub fn new(buffer: B) -> Self {
        Self {
            buffer,
            position: Cell::new(0),
        }
    }

    pub fn read_u8(&self) -> Result<u8> {
        let pos = self.position.get();
        let byte = self
            .buffer
            .as_ref()
            .get(pos)
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"))?;
        self.position.set(pos + 1);
        Ok(*byte)
    }

    pub fn read_u32_le(&self) -> Result<u32> {
        let pos = self.position.get();
        let value = self.buffer.as_ref().read_u32_le(pos);
        self.position.set(pos + 4);
        Ok(value)
    }

    pub fn read_u32_be(&self) -> Result<u32> {
        let pos = self.position.get();
        let value = self.buffer.as_ref().read_u32_be(pos);
        self.position.set(pos + 4);
        Ok(value)
    }

    pub fn read_u64_be(&self) -> Result<u64> {
        let pos = self.position.get();
        let value = self.buffer.as_ref().read_u64_be(pos);
        self.position.set(pos + 8);
        Ok(value)
    }

    pub fn peek_i32_le(&self) -> Result<i32> {
        Ok(self.buffer.as_ref().read_i32_le(self.position.get()))
    }

    pub fn read_i32_le(&self) -> Result<i32> {
        let pos = self.position.get();
        let value = self.buffer.as_ref().read_i32_le(pos);
        self.position.set(pos + 4);
        Ok(value)
    }

    pub fn read_i64_le(&self) -> Result<i64> {
        let pos = self.position.get();
        let value = self.buffer.as_ref().read_i64_le(pos);
        self.position.set(pos + 8);
        Ok(value)
    }

    pub fn read_length_prefixed_slice(&self) -> Result<&[u8]> {
        let len = self.read_varint_u64()? as usize;
        let pos = self.position.get();
        let buffer = self.buffer.as_ref();
        if pos + len > buffer.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"));
        }
        let slice = &buffer[pos..pos + len];
        self.position.set(pos + len);
        Ok(slice)
    }

    pub fn read_fixed_slice(&self, len: usize) -> Result<&[u8]> {
        let pos = self.position.get();
        let buffer = self.buffer.as_ref();
        if pos + len > buffer.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"));
        }
        let slice = &buffer[pos..pos + len];
        self.position.set(pos + len);
        Ok(slice)
    }

    pub fn read_str(&self) -> Result<&str> {
        let slice = self.read_length_prefixed_slice()?;
        std::str::from_utf8(slice)
            .or_else(|e| Err(Error::new(ErrorKind::InvalidData, e.to_string())))
    }

    pub fn position(&self) -> usize {
        self.position.get()
    }

    pub fn has_remaining(&self) -> bool {
        self.position.get() < self.buffer.as_ref().len()
    }

    pub fn remaining(&self) -> usize {
        self.buffer.as_ref().len() - self.position.get()
    }

    pub fn seek(&self, offset: usize) -> Result<()> {
        if offset > self.buffer.as_ref().len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Position out of bounds",
            ));
        }
        self.position.set(offset);
        Ok(())
    }

    pub fn find_next_by<F>(&self, predicate: F) -> Option<usize>
    where
        F: Fn(u8) -> bool,
    {
        let pos = self.position.get();
        self.buffer.as_ref()[pos..]
            .iter()
            .position(|&b| predicate(b))
            .map(|relative_pos| pos + relative_pos)
    }

    pub fn read_varint_u64(&self) -> Result<u64> {
        let (value, offset) = varint::read_u64(self.buffer.as_ref(), self.position.get());
        self.position.set(offset);
        Ok(value)
    }

    pub fn read_varint_u32(&self) -> Result<u32> {
        let (value, offset) = varint::read_u32(self.buffer.as_ref(), self.position.get());
        self.position.set(offset);
        Ok(value)
    }

    pub fn read_varint_i64(&self) -> Result<i64> {
        let (value, offset) = varint::read_i64(self.buffer.as_ref(), self.position.get());
        self.position.set(offset);
        Ok(value)
    }

    pub fn skip(&self, bytes: usize) -> Result<()> {
        let pos = self.position.get() + bytes;
        if pos > self.buffer.as_ref().len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Position out of bounds",
            ));
        }
        self.position.set(pos);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::io::varint::{write_u64, write_u32, write_i64}; // Adjust path as needed

    #[test]
    fn test_read_varint_u64() {
        let values = [0u64, 127, 128, 16_383, 16_384, u64::MAX, u64::MAX - 1];
        for &value in &values {
            let mut buf = Vec::new();
            write_u64(value, &mut buf);
            let reader = ByteReader::new(&buf[..]);
            assert_eq!(reader.read_varint_u64().unwrap(), value);
        }
    }

    #[test]
    fn test_read_varint_u32() {
        let values = [0u32, 127, 128, 16_383, 16_384, u32::MAX, u32::MAX - 1];
        for &value in &values {
            let mut buf = Vec::new();
            write_u32(value, &mut buf);
            let reader = ByteReader::new(&buf[..]);
            assert_eq!(reader.read_varint_u32().unwrap(), value);
        }
    }

    #[test]
    fn test_read_varint_i64() {
        let values = [
            0i64, -0i64, 127, -127, 128, -128, 16_383, -16_384,
            16_384, -16_384, i64::MIN, i64::MAX,
        ];
        for &value in &values {
            let mut buf = Vec::new();
            write_i64(value, &mut buf);
            let reader = ByteReader::new(&buf[..]);
            assert_eq!(reader.read_varint_i64().unwrap(), value);
        }
    }

    #[test]
    fn test_read_length_prefixed_slice() {
        // Length prefix = 3, then bytes 0xAA, 0xBB, 0xCC
        let mut buf = Vec::new();
        write_u64(3, &mut buf);
        buf.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let reader = ByteReader::new(&buf[..]);
        let slice = reader.read_length_prefixed_slice().unwrap();
        assert_eq!(slice, &[0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_read_str() {
        let s = "hello";
        let mut buf = Vec::new();
        write_u64(s.len() as u64, &mut buf);
        buf.extend_from_slice(s.as_bytes());
        let reader = ByteReader::new(&buf[..]);
        assert_eq!(reader.read_str().unwrap(), s);
    }

    #[test]
    fn test_read_u8_and_position() {
        let buf = [1, 2, 3];
        let reader = ByteReader::new(&buf);
        assert_eq!(reader.read_u8().unwrap(), 1);
        assert_eq!(reader.position(), 1);
        assert_eq!(reader.read_u8().unwrap(), 2);
        assert_eq!(reader.position(), 2);
        assert_eq!(reader.read_u8().unwrap(), 3);
        assert_eq!(reader.position(), 3);
        assert!(reader.read_u8().is_err()); // EOF
    }

    #[test]
    fn test_read_fixed_slice() {
        let buf = vec![1, 2, 3, 4, 5];
        let reader = ByteReader::new(buf);
        assert_eq!(reader.read_fixed_slice(2).unwrap(), &[1, 2]);
        assert_eq!(reader.read_fixed_slice(3).unwrap(), &[3, 4, 5]);
        assert!(reader.read_fixed_slice(1).is_err()); // EOF
    }

    #[test]
    fn test_seek_and_skip() {
        let buf = [1, 2, 3, 4, 5];
        let reader = ByteReader::new(buf);
        reader.seek(3).unwrap();
        assert_eq!(reader.position(), 3);
        reader.skip(1).unwrap();
        assert_eq!(reader.position(), 4);
        assert_eq!(reader.read_u8().unwrap(), 5);
        assert!(!reader.has_remaining());
        assert!(reader.seek(10).is_err()); // OOB
        assert!(reader.skip(1).is_err()); // OOB
    }

    #[test]
    fn test_remaining_and_has_remaining() {
        let buf = vec![1, 2, 3];
        let reader = ByteReader::new(buf);
        assert_eq!(reader.remaining(), 3);
        reader.read_u8().unwrap();
        assert_eq!(reader.remaining(), 2);
        assert!(reader.has_remaining());
        reader.skip(2).unwrap();
        assert_eq!(reader.remaining(), 0);
        assert!(!reader.has_remaining());
    }

    #[test]
    fn test_with_arc() {
        let buf = Arc::from(vec![1u8, 2, 3, 4]);
        let reader = ByteReader::new(buf);
        assert_eq!(reader.read_fixed_slice(2).unwrap(), &[1, 2]);
        assert_eq!(reader.read_fixed_slice(2).unwrap(), &[3, 4]);
    }

    #[test]
    fn test_find_next_by() {
        let buf = vec![1, 2, 5, 7, 8];
        let reader = ByteReader::new(buf);
        reader.skip(1).unwrap();
        let pos = reader.find_next_by(|b| b == 7);
        assert_eq!(pos, Some(3));
        let pos = reader.find_next_by(|b| b == 42);
        assert_eq!(pos, None);
    }
}
