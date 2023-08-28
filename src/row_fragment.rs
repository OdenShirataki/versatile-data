use std::path::PathBuf;

use super::FileMmap;

const U32_SIZE: usize = std::mem::size_of::<u32>();

pub struct RowFragment {
    filemmap: FileMmap,
}
impl RowFragment {
    pub fn new(path: PathBuf) -> Self {
        let mut filemmap = FileMmap::new(path).unwrap();
        if filemmap.len() == 0 {
            filemmap.set_len(U32_SIZE as u64).unwrap();
        }
        Self { filemmap }
    }
    fn blank_count(&self) -> u64 {
        self.filemmap.len() / U32_SIZE as u64 - 1
    }
    pub fn insert_blank(&mut self, row: u32) -> u64 {
        self.filemmap.append(&row.to_ne_bytes()).unwrap()
    }
    pub fn pop(&mut self) -> Option<u32> {
        let count = self.blank_count();
        (count > 0).then(|| {
            let last = unsafe { *(self.filemmap.as_ptr() as *mut u32).offset(count as isize) };
            self.filemmap.set_len(count * U32_SIZE as u64).unwrap();
            last
        })
    }
    pub fn serial_increment(&mut self) -> u32 {
        let blank_list = unsafe { &mut *(self.filemmap.as_ptr() as *mut u32) };
        *blank_list += 1;
        *blank_list
    }
}
