use std::{io, mem::ManuallyDrop, path::PathBuf};

use file_mmap::FileMmap;

const U32_SIZE: usize = std::mem::size_of::<u32>();

pub struct RowFragment {
    filemmap: FileMmap,
    blank_list: ManuallyDrop<Box<u32>>,
}
impl RowFragment {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(U32_SIZE as u64)?;
        }
        let blank_list = filemmap.as_ptr() as *mut u32;
        Ok(Self {
            filemmap,
            blank_list: ManuallyDrop::new(unsafe { Box::from_raw(blank_list) }),
        })
    }
    fn blank_count(&self) -> u64 {
        self.filemmap.len().unwrap() / U32_SIZE as u64 - 1
    }
    pub fn insert_blank(&mut self, row: u32) -> io::Result<u64> {
        self.filemmap.append(&row.to_ne_bytes())
    }
    pub fn exists_blank(&self) -> bool {
        self.blank_count() > 0
    }
    pub fn pop(&mut self) -> io::Result<Option<u32>> {
        let count = self.blank_count();
        if count > 0 {
            let last = unsafe { *(&mut **self.blank_list as *mut u32).offset(count as isize) };
            self.filemmap.set_len(count * U32_SIZE as u64)?;
            return Ok(Some(last));
        }
        Ok(None)
    }
    pub fn serial_increment(&mut self) -> u32 {
        **self.blank_list += 1;
        **self.blank_list
    }
}
