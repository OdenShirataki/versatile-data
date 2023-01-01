use file_mmap::FileMmap;
use idx_sized::IdxSized;
use std::{io, mem::ManuallyDrop, path::PathBuf};

const U32_SIZE: usize = std::mem::size_of::<u32>();
const INIT_SIZE: usize = U32_SIZE * 2;
struct Fragment {
    filemmap: FileMmap,
    increment: ManuallyDrop<Box<u32>>,
    blank_list: ManuallyDrop<Box<u32>>,
    blank_count: u32,
}
impl Fragment {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let mut filemmap = FileMmap::new(path)?;
        if filemmap.len()? == 0 {
            filemmap.set_len(INIT_SIZE as u64)?;
        }
        let increment = filemmap.as_ptr() as *mut u32;
        let blank_list = unsafe { filemmap.offset(U32_SIZE as isize) } as *mut u32;

        let len = filemmap.len()?;
        let blank_count = if len == INIT_SIZE as u64 {
            0
        } else {
            len / U32_SIZE as u64 - 2 //最後尾は常に0でterminateするので、12byte以上の場合のみblankがある
        } as u32;

        Ok(Fragment {
            filemmap,
            increment: ManuallyDrop::new(unsafe { Box::from_raw(increment) }),
            blank_list: ManuallyDrop::new(unsafe { Box::from_raw(blank_list) }),
            blank_count,
        })
    }
    pub fn increment(&mut self) -> u32 {
        **self.increment += 1;
        **self.increment
    }
    pub fn insert_blank(&mut self, id: u32) -> io::Result<()> {
        self.filemmap.append(&[0, 0, 0, 0])?;
        unsafe {
            *(&mut **self.blank_list as *mut u32).offset(self.blank_count as isize) = id;
        }
        self.blank_count += 1;

        Ok(())
    }

    pub fn pop(&mut self) -> Option<u32> {
        if self.blank_count > 0 {
            let p = unsafe {
                (&mut **self.blank_list as *mut u32).offset(self.blank_count as isize - 1)
            };
            let last = unsafe { *p };
            unsafe {
                *p = 0;
            }
            if let Ok(len) = self.filemmap.len() {
                let to = len - U32_SIZE as u64;
                let _ = self.filemmap.set_len(to);
                self.blank_count -= 1;
                return Some(last);
            }
        }
        None
    }
}

pub(crate) struct SerialNumber {
    index: IdxSized<u32>,
    fragment: Fragment,
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
            fragment: Fragment::new({
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
        self.fragment.blank_count > 0
    }
    pub fn add(&mut self) -> io::Result<u32> {
        self.index.insert(self.fragment.increment())
    }
    pub fn pop_blank(&mut self) -> io::Result<Option<u32>> {
        Ok(if let Some(exists_row) = self.fragment.pop() {
            self.index.update(exists_row, self.fragment.increment())?;
            Some(exists_row)
        } else {
            None
        })
    }
    pub fn delete(&mut self, row: u32) -> io::Result<()> {
        self.index.delete(row);
        self.fragment.insert_blank(row)
    }
}
