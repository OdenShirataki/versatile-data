use file_mmap::FileMmap;
use idx_sized::IdxSized;
use std::{io, mem::ManuallyDrop, path::Path};

const U32_SIZE: usize = std::mem::size_of::<u32>();
const INIT_SIZE: usize = U32_SIZE * 2;
struct Fragment {
    filemmap: FileMmap,
    increment: ManuallyDrop<Box<u32>>,
    blank_list: ManuallyDrop<Box<u32>>,
    blank_count: u32,
}
impl Fragment {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
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
    pub fn insert_blank(&mut self, id: u32) {
        self.filemmap.append(&[0, 0, 0, 0]).unwrap();
        unsafe {
            *(&mut **self.blank_list as *mut u32).offset(self.blank_count as isize) = id;
        }
        self.blank_count += 1;
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
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        let file_name_prefix = if let Some(file_name) = path.file_name() {
            file_name.to_string_lossy().into_owned()
        } else {
            "".to_owned()
        };

        let mut indx_file_name = path.to_path_buf();
        indx_file_name.set_file_name(&(file_name_prefix.to_owned() + ".i"));

        let mut fragment_file_name = path.to_path_buf();
        fragment_file_name.set_file_name(&(file_name_prefix + ".f"));

        Ok(SerialNumber {
            index: IdxSized::new(indx_file_name)?,
            fragment: Fragment::new(fragment_file_name)?,
        })
    }
    pub fn index(&self) -> &IdxSized<u32> {
        &self.index
    }
    pub fn exists_blank(&self) -> bool {
        self.fragment.blank_count > 0
    }
    pub fn add(&mut self) -> io::Result<u32> {
        //追加されたrowを返す
        let row = self.index.insert(self.fragment.increment())?;
        Ok(row)
    }
    pub fn pop_blank(&mut self) -> Option<u32> {
        if let Some(exists_row) = self.fragment.pop() {
            self.index
                .update(exists_row, self.fragment.increment())
                .unwrap();
            Some(exists_row)
        } else {
            None
        }
    }
    pub fn delete(&mut self, row: u32) {
        self.index.delete(row);
        self.fragment.insert_blank(row);
    }
}
