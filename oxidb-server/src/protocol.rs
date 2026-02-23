use std::io::{self, Read, Write};

/// Read a length-prefixed message: [u32 LE length][json bytes].
pub fn read_message(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large (>16 MiB)",
        ));
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Write a length-prefixed message: [u32 LE length][json bytes].
pub fn write_message(writer: &mut impl Write, data: &[u8]) -> io::Result<()> {
    let len = (data.len() as u32).to_le_bytes();
    writer.write_all(&len)?;
    writer.write_all(data)?;
    writer.flush()
}
