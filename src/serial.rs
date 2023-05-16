use idx_binary::{anyhow::Result, IdxFile};
use std::{io, path::PathBuf};

use crate::RowFragment;

pub(crate) struct SerialNumber {
    index: IdxFile<u32>,
    fragment: RowFragment,
}
impl std::ops::Deref for SerialNumber {
    type Target = IdxFile<u32>;
    fn deref(&self) -> &Self::Target {
        &self.index
    }
}
impl SerialNumber {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file_name = if let Some(file_name) = path.file_name() {
            file_name.to_string_lossy()
        } else {
            "".into()
        };

        Ok(SerialNumber {
            index: IdxFile::new({
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
    pub fn delete(&mut self, row: u32) -> io::Result<u64> {
        self.index.delete(row)?;
        self.fragment.insert_blank(row)
    }
    pub fn next_row(&mut self) -> Result<u32> {
        let row = self
            .index
            .new_row(if let Some(row) = self.fragment.pop()? {
                row
            } else {
                0
            })?;
        self.index.update(row, self.fragment.serial_increment())
    }
}
