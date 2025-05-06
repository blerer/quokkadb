use std::cell::Cell;
use std::io::{Result, Error, ErrorKind};
use crate::io::ZeroCopy;
use crate::io::varint;

pub struct ByteReader<'a> {
    buffer: &'a [u8],
    position: Cell<usize>,
}

impl<'a> ByteReader<'a> {
    pub fn new(buffer: &[u8]) -> ByteReader {
        ByteReader {
            buffer,
            position: Cell::new(0),
        }
    }

    pub fn read_u8(&self) -> Result<u8> {
        let pos = self.position.get();
        let byte = self.buffer.get(pos).ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"))?;
        self.position.set(pos + 1);
        Ok(*byte)
    }

    pub fn read_u32_le(&self) -> Result<u32> {
        let pos = self.position.get();
        let value = self.buffer.read_u32_le(pos);
        self.position.set(pos + 4);
        Ok(value)
    }

    pub fn read_u32_be(&self) -> Result<u32> {
        let pos = self.position.get();
        let value = self.buffer.read_u32_be(pos);
        self.position.set(pos + 4);
        Ok(value)
    }

    pub fn read_u64_be(&self) -> Result<u64> {
        let pos = self.position.get();
        let value = self.buffer.read_u64_be(pos);
        self.position.set(pos + 8);
        Ok(value)
    }

    pub fn peek_i32_le(&self) -> Result<i32> {
        Ok(self.buffer.read_i32_le(self.position.get()))
    }

    pub fn read_i32_le(&self) -> Result<i32> {
        let pos = self.position.get();
        let value = self.buffer.read_i32_le(pos);
        self.position.set(pos + 4);
        Ok(value)
    }

    pub fn read_i64_le(&self) -> Result<i64> {
        let pos = self.position.get();
        let value = self.buffer.read_i64_le(pos);
        self.position.set(pos + 8);
        Ok(value)
    }

    pub fn read_length_prefixed_slice(&self) -> Result<&[u8]> {
        let len = self.read_varint_u64()? as usize;
        let pos = self.position.get();
        if pos + len > self.buffer.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"));
        }
        let slice = &self.buffer[pos..pos + len];
        self.position.set(pos + len);
        Ok(slice)
    }

    pub fn read_fixed_slice(&self, len: usize) -> Result<&[u8]> {
        let pos = self.position.get();
        if pos + len > self.buffer.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"));
        }
        let slice = &self.buffer[pos..pos + len];
        self.position.set(pos + len);
        Ok(slice)
    }

    pub fn read_str(&self) -> Result<&str> {
        let slice = self.read_length_prefixed_slice()?;
        std::str::from_utf8(slice).or_else(|e| Err(Error::new(ErrorKind::InvalidData, e.to_string())))
    }

    pub fn position(&self) -> usize {
        self.position.get()
    }

    pub fn has_remaining(&self) -> bool {
        self.position.get() < self.buffer.len()
    }

    pub fn remaining(&self) -> usize {
        self.buffer.len() - self.position.get()
    }

    pub fn seek(&self, offset: usize) -> Result<()> {
        if offset > self.buffer.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Position out of bounds"));
        }
        self.position.set(offset);
        Ok(())
    }

    pub fn find_next_by<F>(&self, predicate: F) -> Option<usize>
    where
        F: Fn(u8) -> bool,
    {
        let pos = self.position.get();
        self.buffer[pos..]
            .iter()
            .position(|&b| predicate(b))
            .map(|relative_pos| pos + relative_pos)
    }

    pub fn read_varint_u64(&self) -> Result<u64> {
        let (value, offset) = varint::read_u64(self.buffer, self.position.get());
        self.position.set(offset);
        Ok(value)
    }

    pub fn read_varint_u32(&self) -> Result<u32> {
        let (value, offset) = varint::read_u32(self.buffer, self.position.get());
        self.position.set(offset);
        Ok(value)
    }

    pub fn read_varint_i64(&self) -> Result<i64> {
        let (value, offset) = varint::read_i64(self.buffer, self.position.get());
        self.position.set(offset);
        Ok(value)
    }

    pub fn skip(&self, bytes: usize) -> Result<()> {
        let pos = self.position.get() + bytes;
        if pos > self.buffer.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Position out of bounds"));
        }
        self.position.set(pos);
        Ok(())
    }
}