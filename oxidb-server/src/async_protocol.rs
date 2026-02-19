use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Read a length-prefixed message: [u32 LE length][payload bytes].
pub async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large (>16 MiB)",
        ));
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a length-prefixed message: [u32 LE length][payload bytes].
pub async fn write_message<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    let len = (data.len() as u32).to_le_bytes();
    writer.write_all(&len).await?;
    writer.write_all(data).await?;
    writer.flush().await
}
