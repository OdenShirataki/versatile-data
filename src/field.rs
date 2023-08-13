use std::{
    fs,
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, RwLock},
};

use idx_binary::{DataAddress, DataAddressHolder, IdxBinary};

use crate::Data;

#[derive(PartialEq, Clone, Debug)]
pub struct FieldEntity {
    data_address: DataAddress,
    num: f64,
}
impl FieldEntity {
    pub fn data_address(&self) -> &DataAddress {
        &self.data_address
    }
}

impl DataAddressHolder<FieldEntity> for FieldEntity {
    fn data_address(&self) -> &DataAddress {
        &self.data_address
    }
    fn new(data_address: DataAddress, input: &[u8]) -> FieldEntity {
        FieldEntity {
            data_address,
            num: unsafe { std::str::from_utf8_unchecked(input) }
                .parse()
                .unwrap_or(0.0),
        }
    }
}

pub struct Field {
    index: IdxBinary<FieldEntity>,
}

impl Deref for Field {
    type Target = IdxBinary<FieldEntity>;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}
impl DerefMut for Field {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.index
    }
}

impl Field {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            index: IdxBinary::new(path),
        }
    }

    pub fn num(&self, row: u32) -> Option<f64> {
        if let Some(value) = self.value(row) {
            Some(value.num)
        } else {
            None
        }
    }
}

impl Data {
    pub fn field_names(&self) -> Vec<&String> {
        self.fields_cache.iter().map(|(key, _)| key).collect()
    }
    pub fn field_bytes(&self, row: u32, name: &str) -> &[u8] {
        if let Some(f) = self.field(name) {
            if let Some(v) = f.read().unwrap().bytes(row) {
                v
            } else {
                b""
            }
        } else {
            b""
        }
    }
    pub fn field_num(&self, row: u32, name: &str) -> f64 {
        if let Some(f) = self.field(name) {
            if let Some(f) = f.read().unwrap().num(row) {
                f
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub(crate) fn create_field(&mut self, field_name: &str) -> &mut Arc<RwLock<Field>> {
        let mut fields_dir = self.fields_dir.clone();
        fields_dir.push(field_name);
        fs::create_dir_all(&fields_dir).unwrap();
        if fields_dir.exists() {
            let field = Field::new(fields_dir);
            self.fields_cache
                .entry(String::from(field_name))
                .or_insert(Arc::new(RwLock::new(field)));
        }
        self.fields_cache.get_mut(field_name).unwrap()
    }
}
