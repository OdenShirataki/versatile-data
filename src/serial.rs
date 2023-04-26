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
    pub fn delete(&mut self, row: u32) -> io::Result<u64> {
        self.index.delete(row)?;
        self.fragment.insert_blank(row)
    }
    pub fn next_row(&mut self) -> io::Result<u32> {
        if let Some(row) = self.fragment.pop()? {
            self.index.update(row, self.fragment.serial_increment())
        } else {
            self.index.insert(self.fragment.serial_increment())
        }
    }
}
