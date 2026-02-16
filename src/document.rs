use serde_json::{Map, Value};

use crate::error::{Error, Result};

pub type DocumentId = u64;

#[derive(Debug, Clone)]
pub struct Document {
    pub id: DocumentId,
    pub data: Value,
}

impl Document {
    pub fn new(id: DocumentId, data: Value) -> Result<Self> {
        if !data.is_object() {
            return Err(Error::NotAnObject);
        }
        Ok(Self { id, data })
    }

    /// Access a nested field using dot notation: "user.address.city"
    pub fn get_field(&self, path: &str) -> Option<&Value> {
        let mut current = &self.data;
        for part in path.split('.') {
            match current {
                Value::Object(map) => {
                    current = map.get(part)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    pub fn as_object(&self) -> Option<&Map<String, Value>> {
        self.data.as_object()
    }
}
