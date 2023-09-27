use std::{num::NonZeroU32, path::PathBuf};

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

    #[inline(always)]
    fn blank_count(&self) -> u64 {
        self.filemmap.len() / U32_SIZE as u64 - 1
    }

    #[inline(always)]
    pub fn insert_blank(&mut self, row: NonZeroU32) {
        self.filemmap.append(&row.get().to_ne_bytes()).unwrap();
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<NonZeroU32> {
        let count = self.blank_count();
        (count > 0).then(|| {
            let last = unsafe { *(self.filemmap.as_ptr() as *mut u32).offset(count as isize) };
            self.filemmap.set_len(count * U32_SIZE as u64).unwrap();
            unsafe { NonZeroU32::new_unchecked(last) }
        })
    }

    #[inline(always)]
    pub fn serial_increment(&mut self) -> NonZeroU32 {
        let blank_list = unsafe { &mut *(self.filemmap.as_ptr() as *mut u32) };
        *blank_list += 1;
        unsafe { NonZeroU32::new_unchecked(*blank_list) }
    }
}
