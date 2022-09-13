use indexed_data_file::IndexedDataFile;
use file_mmap::FileMmap;

struct Fragment{
    filemmap:FileMmap
    ,increment: *mut u32
    ,blank_list: *mut u32
    ,blank_count: u32
}
impl Fragment{
    pub fn new(path:&str) -> Result<Fragment,std::io::Error>{
        let u32size=std::mem::size_of::<u32>();

        let init_size=(u32size + u32size) as u64;   //初期サイズはincrementとblank_list(0)の分
        let filemmap=FileMmap::new(path,init_size)?;
        let increment=filemmap.as_ptr() as *mut u32;
        let blank_list=unsafe{(filemmap.as_ptr() as *mut u32).offset(1)};

        let len=filemmap.len();

        let blank_count=if len==init_size{
            0
        }else{
            len / u32size as u64 - 2    //最後尾は常に0でterminateするので、12byte以上の場合のみblankがある
        } as u32;
        
        Ok(Fragment{
            filemmap
            ,increment
            ,blank_list
            ,blank_count
        })
    }
    pub fn increment(&mut self)->u32{
        unsafe{
            *self.increment+=1;
            *self.increment
        }
    }
    pub fn insert_blank(&mut self,id:u32){
        self.filemmap.append(
            &[0,0,0,0]
        );
        unsafe{*(self.blank_list.offset(self.blank_count as isize))=id;}
        self.blank_count+=1;
    }
    pub fn pop(&mut self)->Option<u32>{
        if self.blank_count>0{
            let p=unsafe{
                self.blank_list.offset(self.blank_count as isize - 1)
            };
            let last=unsafe{*p};
            unsafe{*p=0;}
            let _=self.filemmap.set_len(self.filemmap.len()-std::mem::size_of::<u32>() as u64);
            self.blank_count-=1;
            Some(last)
        }else{
            None
        }
    }
}

pub struct SerialNumber{
    index:IndexedDataFile<u32>
    ,fragment:Fragment
}
impl SerialNumber{
    pub fn new(path:&str)->Result<SerialNumber,std::io::Error>{
        Ok(SerialNumber{
            index:IndexedDataFile::new(&(path.to_string()+".i"))?
            ,fragment:Fragment::new(&(path.to_string()+".f"))?
        })
    }
    pub fn exists_blank(&self)->bool{
        self.fragment.blank_count>0
    }
    pub fn add(&mut self)->Option<u32>{ //追加されたidを返す
        let serial_number=self.fragment.increment();
        let rowid=self.index.insert(serial_number)?;
        Some(rowid)
    }
    pub fn pop_blank(&mut self)->Option<u32>{
        if let Some(exists_id)=self.fragment.pop(){
            let serial_number=self.fragment.increment();
            self.index.update(exists_id,serial_number);
            Some(exists_id)
        }else{
            None
        }
    }
    pub fn delete(&mut self,id:u32){
        self.fragment.insert_blank(id);
    }
}