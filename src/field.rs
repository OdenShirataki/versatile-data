use std::{
    io,
    ops::{Deref, DerefMut},
    path::Path,
};

use idx_binary::{DataAddress, DataAddressHolder, IdxBinary};

#[derive(PartialEq, Clone, Debug)]
pub struct FieldEntity {
    data_address: DataAddress,
    num: f64,
}
impl FieldEntity {
    pub fn data_address(&self) -> &DataAddress {
        &self.data_address
    }
    pub fn num(&self) -> f64 {
        self.num
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
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Ok(Self {
            index: IdxBinary::new(path)?,
        })
    }

    pub fn num(&self, row: u32) -> Option<f64> {
        if let Some(value) = self.index.value(row) {
            Some(value.num)
        } else {
            None
        }
    }
    pub fn get(&self, row: u32) -> Option<&'static [u8]> {
        self.index.bytes(row)
    }
}
