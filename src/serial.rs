use idx_binary::IdxFile;
use std::{num::NonZeroU32, path::PathBuf};

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
    pub fn new(path: PathBuf) -> Self {
        let file_name = path.file_name().map_or("".into(), |f| f.to_string_lossy());
        SerialNumber {
            index: IdxFile::new({
                let mut path = path.clone();
                path.set_file_name(&(file_name.to_string() + ".i"));
                path
            }),
            fragment: RowFragment::new({
                let mut path = path.clone();
                path.set_file_name(&(file_name.into_owned() + ".f"));
                path
            }),
        }
    }

    #[inline(always)]
    pub fn delete(&mut self, row: u32) {
        self.index.delete(row);
        self.fragment.insert_blank(row);
    }

    #[inline(always)]
    pub fn next_row(&mut self) -> u32 {
        let row = if let Some(row) = self.fragment.pop() {
            self.index
                .allocate(unsafe { NonZeroU32::new_unchecked(row) });
            row
        } else {
            self.index.create_row()
        };
        self.index.update(row, self.fragment.serial_increment());
        row
    }
}
