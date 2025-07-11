use crate::io::varint;
use std::mem;

pub struct ByteWriter {
    buffer: Vec<u8>,
}

impl ByteWriter {
    pub fn new() -> ByteWriter {
        ByteWriter { buffer: Vec::new() }
    }
    pub fn write_u8(&mut self, value: u8) -> &mut Self {
        self.buffer.push(value);
        self
    }

    pub fn write_varint_u32(&mut self, value: u32) -> &mut Self {
        varint::write_u32(value, &mut self.buffer);
        self
    }

    pub fn write_varint_i32(&mut self, value: i32) -> &mut Self {
        varint::write_i32(value, &mut self.buffer);
        self
    }

    pub fn write_varint_u64(&mut self, value: u64) -> &mut Self {
        varint::write_u64(value, &mut self.buffer);
        self
    }

    pub fn write_varint_i64(&mut self, value: i64) -> &mut Self {
        varint::write_i64(value, &mut self.buffer);
        self
    }

    pub fn write_str(&mut self, value: &str) -> &mut Self {
        self.write_length_prefixed_slice(value.as_bytes())
    }

    pub fn write_length_prefixed_slice(&mut self, value: &[u8]) -> &mut Self {
        self.write_varint_u64(value.len() as u64);
        self.buffer.extend_from_slice(value);
        self
    }

    pub fn take_buffer(&mut self) -> Vec<u8> {
        mem::take(&mut self.buffer)
    }
}
