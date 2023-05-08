use std::{cmp::Ordering, io, path::Path};

use anyhow::Result;
pub use idx_sized::anyhow;

use idx_sized::{Found, IdxSized};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

pub struct FieldData {
    pub(crate) index: IdxSized<FieldEntity>,
    pub(crate) data_file: VariousDataFile,
}
impl FieldData {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        Ok(FieldData {
            index: IdxSized::new({
                let mut path = path.to_path_buf();
                path.push(".i");
                path
            })?,
            data_file: VariousDataFile::new({
                let mut path = path.to_path_buf();
                path.push(".d");
                path
            })?,
        })
    }
    pub fn get(&self, row: u32) -> Option<&'static [u8]> {
        if let Some(e) = self.index.value(row) {
            Some(unsafe { self.data_file.bytes(e.data_address()) })
        } else {
            None
        }
    }

    pub fn num(&self, row: u32) -> Option<f64> {
        if let Some(e) = self.index.value(row) {
            Some(e.num())
        } else {
            None
        }
    }
    pub fn update(&mut self, row: u32, content: &[u8]) -> Result<u32> {
        if let Some(org) = self.index.value(row) {
            if unsafe { self.data_file.bytes(org.data_address()) } == content {
                return Ok(row);
            }
            if !unsafe { self.index.triee().has_same(row) } {
                self.data_file.delete(&org.data_address()).unwrap();
            }
            self.index.delete(row)?;
        }
        let found = self.search(content);
        self.index.update_manually(
            row,
            || -> Result<FieldEntity> {
                let data_address = self.data_file.insert(content)?;
                Ok(FieldEntity::new(
                    data_address.address(),
                    unsafe { std::str::from_utf8_unchecked(content) }
                        .parse()
                        .unwrap_or(0.0),
                ))
            },
            found,
        )
    }
    pub fn delete(&mut self, row: u32) -> std::io::Result<()> {
        self.index.delete(row)?;
        Ok(())
    }

    pub(super) fn search(&self, content: &[u8]) -> Found {
        self.index
            .triee()
            .search(|data| self.search_inner(data, content))
    }

    pub(crate) fn search_inner(&self, data: &FieldEntity, content: &[u8]) -> Ordering {
        self.cmpare_bytes(
            unsafe { self.data_file.bytes(data.data_address()) },
            content,
        )
    }
    pub(crate) fn cmpare_bytes(&self, left: &[u8], right: &[u8]) -> Ordering {
        if left == right {
            Ordering::Equal
        } else {
            unsafe {
                natord::compare(
                    std::str::from_utf8_unchecked(left),
                    std::str::from_utf8_unchecked(right),
                )
            }
        }
    }
}
