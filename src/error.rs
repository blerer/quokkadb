pub enum Error {
    Io(std::io::Error),
    DeserializationError(String)
}