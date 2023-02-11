use idx_sized::IdxSized;
use std::{io, path::PathBuf};

use crate::RowFragment;

pub(crate) struct SerialNumber {
    index: IdxSized<u32>,
    fragment: RowFragment,
}
impl SerialNumber {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file_name = if let Some(file_name) = path.file_name() {
            file_name.to_string_lossy()
        } else {
            "".into()
        };

        Ok(SerialNumber {
            index: IdxSized::new({
                let mut path = path.clone();
                path.set_file_name(&(file_name.to_string() + ".i"));
                path
            })?,
            fragment: RowFragment::new({
                let mut path = path.clone();
                path.set_file_name(&(file_name.into_owned() + ".f"));
                path
            })?,
        })
    }
    pub fn index(&self) -> &IdxSized<u32> {
        &self.index
    }
    pub fn exists_blank(&self) -> bool {
        self.fragment.exists_blank()
    }
    pub fn add(&mut self) -> io::Result<u32> {
        self.index.insert(self.fragment.serial_increment())
    }
    pub fn pop_blank(&mut self) -> io::Result<Option<u32>> {
        Ok(if let Some(exists_row) = self.fragment.pop()? {
            self.index
                .update(exists_row, self.fragment.serial_increment())?;
            Some(exists_row)
        } else {
            None
        })
    }
    pub fn delete(&mut self, row: u32) -> io::Result<u64> {
        self.index.delete(row);
        self.fragment.insert_blank(row)
    }
}
