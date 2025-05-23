use std::fs::File;
use std::io::Read;
use std::ptr;

/// A simple buffer for efficient sequential reads and writes.
///
/// This buffer manages a preallocated `Vec<u8>` to avoid frequent reallocations,
/// providing efficient direct memory operations.
///
/// # Examples
/// ```
/// use quokkadb::io::buffer::Buffer;
/// let mut buffer = Buffer::with_capacity(10);
/// buffer.write_u8(42).write_slice(&[1, 2, 3]);
/// assert_eq!(buffer.as_slice(), &[42, 1, 2, 3]);
/// ```
pub struct Buffer {
    data: Vec<u8>,
    read_index: usize,
    write_index: usize,
}

impl Buffer {
    /// Creates a new buffer with a specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Buffer {
            data: vec![0; capacity],
            read_index: 0,
            write_index: 0,
        }
    }

    /// Creates a buffer from an existing `Vec<u8>`.
    pub fn from_vec(data: Vec<u8>) -> Self {
        Buffer {
            data,
            read_index: 0,
            write_index: 0,
        }
    }

    /// Writes a single byte to the buffer.
    ///
    /// # Panics
    /// Panics if the buffer is full.
    pub fn write_u8(&mut self, byte: u8) -> &mut Self {
        assert!(self.writable_bytes() >= 1, "Buffer overflow in write_u8");
        self.data[self.write_index] = byte;
        self.write_index += 1;
        self
    }

    /// Writes a slice of bytes to the buffer.
    ///
    /// # Panics
    /// Panics if the slice does not fit within the remaining writable space.
    pub fn write_slice(&mut self, slice: &[u8]) -> &mut Self {
        assert!(
            slice.len() <= self.writable_bytes(),
            "Buffer overflow in write_slice"
        );
        unsafe {
            ptr::copy_nonoverlapping(
                slice.as_ptr(),
                self.data.as_mut_ptr().add(self.write_index),
                slice.len(),
            );
        }
        self.write_index += slice.len();
        self
    }

    /// Reads a single byte from the buffer.
    ///
    /// # Panics
    /// Panics if there are no bytes remaining to read.
    pub fn read_u8(&mut self) -> u8 {
        assert!(self.readable_bytes() >= 1, "Buffer underflow in read_u8");
        let byte = self.data[self.read_index];
        self.read_index += 1;
        byte
    }

    /// Reads a slice of bytes from the buffer.
    ///
    /// # Panics
    /// Panics if there aren't enough bytes remaining.
    pub fn read_slice(&mut self, len: usize) -> &[u8] {
        assert!(
            len <= self.readable_bytes(),
            "Buffer underflow in read_slice"
        );
        let slice = &self.data[self.read_index..self.read_index + len];
        self.read_index += len;
        slice
    }

    /// Pads the buffer with zeroes up to its full capacity.
    pub fn pad(&mut self) -> &mut Self {
        let remaining = self.writable_bytes();
        if remaining > 0 {
            unsafe {
                ptr::write_bytes(self.data.as_mut_ptr().add(self.write_index), 0, remaining);
            }
            self.write_index = self.data.len();
        }
        self
    }

    /// Returns the number of writable bytes remaining.
    pub fn writable_bytes(&self) -> usize {
        self.data.len() - self.write_index
    }

    /// Returns the number of readable bytes remaining.
    pub fn readable_bytes(&self) -> usize {
        self.write_index - self.read_index
    }

    /// Returns a slice of readable bytes without modifying indices.
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.read_index..self.write_index]
    }

    /// Clears the buffer indices, effectively marking the buffer as empty.
    pub fn clear(&mut self) -> &mut Self {
        self.write_index = 0;
        self.read_index = 0;
        self
    }

    pub fn fill(&mut self, reader: &mut File) -> std::io::Result<()> {
        self.read_index = 0;
        self.write_index = reader.read(&mut self.data)?;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.write_index == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_with_capacity() {
        let buffer = Buffer::with_capacity(10);
        assert_eq!(buffer.writable_bytes(), 10);
        assert_eq!(buffer.readable_bytes(), 0);
    }

    #[test]
    fn test_from_vec() {
        let data = vec![1, 2, 3];
        let buffer = Buffer::from_vec(data.clone());
        let empty: Vec<u8> = vec![];
        assert_eq!(buffer.as_slice(), &empty);
        assert_eq!(buffer.writable_bytes(), 3);
    }

    #[test]
    fn test_write_u8() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_u8(42);
        assert_eq!(buffer.as_slice(), &[42]);
    }

    #[test]
    #[should_panic(expected = "Buffer overflow in write_u8")]
    fn test_write_u8_overflow() {
        let mut buffer = Buffer::with_capacity(1);
        buffer.write_u8(42);
        buffer.write_u8(43);
    }

    #[test]
    fn test_write_slice() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_slice(&[1, 2, 3]);
        assert_eq!(buffer.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_read_u8() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_u8(10);
        buffer.write_u8(20);
        assert_eq!(buffer.read_u8(), 10);
        assert_eq!(buffer.read_u8(), 20);
    }

    #[test]
    #[should_panic(expected = "Buffer underflow in read_u8")]
    fn test_read_u8_underflow() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.read_u8();
    }

    #[test]
    fn test_read_slice() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_slice(&[5, 6, 7, 8]);
        assert_eq!(buffer.read_slice(2), &[5, 6]);
        assert_eq!(buffer.read_slice(2), &[7, 8]);
    }

    #[test]
    #[should_panic(expected = "Buffer underflow in read_slice")]
    fn test_read_slice_underflow() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_slice(&[1]);
        buffer.read_slice(2);
    }

    #[test]
    fn test_pad() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_u8(1).pad();
        assert_eq!(buffer.as_slice(), &[1, 0, 0, 0, 0]);
    }

    #[test]
    fn test_clear() {
        let mut buffer = Buffer::with_capacity(5);
        buffer.write_slice(&[1, 2, 3]).clear();
        let empty: Vec<u8> = vec![];
        assert_eq!(buffer.as_slice(), empty);
        assert_eq!(buffer.writable_bytes(), 5);
    }
}
