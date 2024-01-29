use std::{fs, num::NonZeroU32, sync::Arc};

use hashbrown::HashMap;
use idx_binary::IdxBinary;

use crate::Data;

pub type Field = IdxBinary;

pub type FieldName = Arc<String>;
pub type Fields = HashMap<FieldName, Field>;

impl Data {
    /// Returns the value of the field with the specified name in the specified row as a slice.
    pub fn field_bytes(&self, row: NonZeroU32, name: &FieldName) -> &[u8] {
        self.fields
            .get(name)
            .and_then(|v| v.bytes(row))
            .unwrap_or(b"")
    }

    /// Returns the value of the field with the specified name in the specified row as a number.
    pub fn field_num(&self, row: NonZeroU32, name: &FieldName) -> f64 {
        self.fields
            .get(name)
            .and_then(|v| v.bytes(row))
            .and_then(|v| unsafe { std::str::from_utf8_unchecked(v) }.parse().ok())
            .unwrap_or(0.0)
    }

    pub(crate) fn create_field(&mut self, name: &FieldName) {
        if !self.fields.contains_key(name) {
            let mut fields_dir = self.fields_dir.clone();
            fields_dir.push(name.as_ref().to_string());
            fs::create_dir_all(&fields_dir).unwrap();
            let field = Field::new(fields_dir, self.option.allocation_lot);

            self.fields.insert(name.clone(), field);
        }
    }

    pub fn fields(&self) -> &Fields {
        &self.fields
    }
}
