use std::{cmp::Ordering, io, path::Path};

use anyhow::Result;
pub use idx_file::anyhow;

use idx_file::{Avltriee, AvltrieeHolder, Found, IdxFile};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

pub struct FieldData {
    pub(crate) index: IdxFile<FieldEntity>,
    pub(crate) data_file: VariousDataFile,
}

impl AvltrieeHolder<FieldEntity, &[u8]> for FieldData {
    fn triee(&self) -> &Avltriee<FieldEntity> {
        self.index.triee()
    }
    fn triee_mut(&mut self) -> &mut Avltriee<FieldEntity> {
        self.index.triee_mut()
    }
    fn cmp(&self, left: &FieldEntity, right: &&[u8]) -> Ordering {
        self.search(left, right)
    }

    fn search(&self, input: &&[u8]) -> Found {
        self.index
            .triee()
            .search_uord(|data| self.search(data, input))
    }

    fn value(&mut self, input: &[u8]) -> Result<FieldEntity> {
        let data_address = self.data_file.insert(input)?;
        Ok(FieldEntity::new(
            data_address.address(),
            unsafe { std::str::from_utf8_unchecked(input) }
                .parse()
                .unwrap_or(0.0),
        ))
    }

    fn delete(&mut self, row: u32, delete_node: &FieldEntity) -> Result<()> {
        if !unsafe { self.index.triee().has_same(row) } {
            self.data_file.delete(&delete_node.data_address()).unwrap();
        }
        self.index.delete(row)?;
        Ok(())
    }
}

impl FieldData {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        Ok(FieldData {
            index: IdxFile::new({
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
        let row = self.index.new_row(row)?;
        unsafe {
            Avltriee::update_holder(self, row, content)?;
        }
        Ok(row)
    }
    pub fn delete(&mut self, row: u32) -> std::io::Result<()> {
        self.index.delete(row)?;
        Ok(())
    }

    pub(crate) fn search(&self, data: &FieldEntity, content: &[u8]) -> Ordering {
        let left = unsafe { self.data_file.bytes(data.data_address()) };
        if left == content {
            Ordering::Equal
        } else {
            unsafe {
                natord::compare(
                    std::str::from_utf8_unchecked(left),
                    std::str::from_utf8_unchecked(content),
                )
            }
        }
    }
}
