use std::{fs, num::NonZeroU32};

use idx_binary::{DataAddress, IdxBinary};

use crate::Data;

pub type Field = IdxBinary<DataAddress>;

impl Data {
    #[inline(always)]
    pub fn field_names(&self) -> Vec<&String> {
        self.fields_cache.iter().map(|(key, _)| key).collect()
    }

    #[inline(always)]
    pub fn field_bytes(&self, row: NonZeroU32, name: &str) -> &[u8] {
        self.field(name).and_then(|v| v.bytes(row)).unwrap_or(b"")
    }

    #[inline(always)]
    pub fn field_num(&self, row: NonZeroU32, name: &str) -> f64 {
        self.field(name)
            .and_then(|v| v.bytes(row))
            .and_then(|v| unsafe { std::str::from_utf8_unchecked(v) }.parse().ok())
            .unwrap_or(0.0)
    }

    #[inline(always)]
    pub(crate) fn create_field(&mut self, field_name: &str) -> &mut Field {
        let mut fields_dir = self.fields_dir.clone();
        fields_dir.push(field_name);
        fs::create_dir_all(&fields_dir).unwrap();
        let field = Field::new(fields_dir, self.option.allocation_lot);
        self.fields_cache.entry(field_name.into()).or_insert(field);
        self.fields_cache.get_mut(field_name).unwrap()
    }
}
