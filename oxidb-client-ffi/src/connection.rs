use std::ffi::CString;
use std::io::{self, Read, Write};
use std::net::TcpStream;

/// A connection to an OxiDB server.
pub struct OxiDbConnection {
    stream: TcpStream,
    last_error: Option<CString>,
}

impl OxiDbConnection {
    pub fn connect(host: &str, port: u16) -> io::Result<Self> {
        let stream = TcpStream::connect((host, port))?;
        Ok(Self {
            stream,
            last_error: None,
        })
    }

    /// Send a length-prefixed JSON request and read the length-prefixed response.
    pub fn request(&mut self, json_bytes: &[u8]) -> io::Result<Vec<u8>> {
        // Write: [u32 LE length][json]
        let len = (json_bytes.len() as u32).to_le_bytes();
        self.stream.write_all(&len)?;
        self.stream.write_all(json_bytes)?;
        self.stream.flush()?;

        // Read: [u32 LE length][json]
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let resp_len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; resp_len];
        self.stream.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn set_last_error(&mut self, err: String) {
        self.last_error = CString::new(err).ok();
    }
}
