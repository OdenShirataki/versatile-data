use idx_sized::IdxSized;
use file_mmap::FileMmap;

const U32SIZE:usize=std::mem::size_of::<u32>();
struct Fragment{
    filemmap:FileMmap
    ,increment: Vec<u32>
    ,blank_list: Vec<u32>
    ,blank_count: u32
}
impl Fragment{
    pub fn new(path:&str) -> Result<Fragment,std::io::Error>{
        let init_size=(U32SIZE*2) as u64;   //初期サイズはincrementとblank_list(0)の分
        let filemmap=FileMmap::new(path,init_size)?;
        let increment=filemmap.as_ptr() as *mut u32;
        let blank_list=filemmap.offset(U32SIZE as isize) as *mut u32;

        let len=filemmap.len();

        let blank_count=if len==init_size{
            0
        }else{
            len / U32SIZE as u64 - 2    //最後尾は常に0でterminateするので、12byte以上の場合のみblankがある
        } as u32;
        
        Ok(Fragment{
            filemmap
            ,increment:unsafe {Vec::from_raw_parts(increment,1,0)}
            ,blank_list:unsafe {Vec::from_raw_parts(blank_list,1,0)}
            ,blank_count
        })
    }
    pub fn increment(&mut self)->u32{
        self.increment[0]+=1;
        self.increment[0]
    }
    pub fn insert_blank(&mut self,id:u32){
        self.filemmap.append(
            &[0,0,0,0]
        );
        unsafe{
            *(self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize)=id;
        }
        self.blank_count+=1;
    }
    pub fn pop(&mut self)->Option<u32>{
        if self.blank_count>0{
            let p=unsafe{
                (self.blank_list.as_ptr() as *mut u32).offset(self.blank_count as isize - 1)
            };
            let last=unsafe{*p};
            unsafe{*p=0;}
            let _=self.filemmap.set_len(self.filemmap.len() - U32SIZE as u64);
            self.blank_count-=1;
            Some(last)
        }else{
            None
        }
    }
}

pub struct SerialNumber{
    index:IdxSized<u32>
    ,fragment:Fragment
}
impl SerialNumber{
    pub fn new(path:&str)->Result<SerialNumber,std::io::Error>{
        Ok(SerialNumber{
            index:IdxSized::new(&(path.to_string()+".i"))?
            ,fragment:Fragment::new(&(path.to_string()+".f"))?
        })
    }
    pub fn index(&self)->&IdxSized<u32>{
        &self.index
    }
    pub fn exists_blank(&self)->bool{
        self.fragment.blank_count>0
    }
    pub fn add(&mut self)->Option<u32>{ //追加されたrowを返す
        let serial_number=self.fragment.increment();
        let row=self.index.insert(serial_number)?;
        Some(row)
    }
    pub fn pop_blank(&mut self)->Option<u32>{
        if let Some(exists_row)=self.fragment.pop(){
            let serial_number=self.fragment.increment();
            self.index.update(exists_row,serial_number);
            Some(exists_row)
        }else{
            None
        }
    }
    pub fn delete(&mut self,row:u32){
        self.fragment.insert_blank(row);
    }
}