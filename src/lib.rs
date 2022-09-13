use std::collections::HashMap;
use indexed_data_file::IndexedDataFile;

pub mod field;

mod serial;
use serial::SerialNumber;

pub mod basic;
use basic::BasicData;

pub struct Data{
    data_dir:String
    ,serial: SerialNumber
    ,uuid: IndexedDataFile<u128>
    ,activity: IndexedDataFile<u8>
    ,priority: IndexedDataFile<basic::Priority>
    ,term_begin: IndexedDataFile<i64>
    ,term_end: IndexedDataFile<i64>
    ,last_updated: IndexedDataFile<i64>
    ,fields_cache:HashMap<String,field::Field>
}

impl Data{
    pub fn new(dir:&str)-> Option<Data>{
        if let (
            Ok(serial)
            ,Ok(uuid)
            ,Ok(activity)
            ,Ok(priority)
            ,Ok(term_begin)
            ,Ok(term_end)
            ,Ok(last_updated)
        )=(
            SerialNumber::new(&(dir.to_string()+"/serial"))
            ,IndexedDataFile::new(&(dir.to_string()+"/uuid.i"))
            ,IndexedDataFile::new(&(dir.to_string()+"/activity.i"))
            ,IndexedDataFile::new(&(dir.to_string()+"/priority.i"))
            ,IndexedDataFile::new(&(dir.to_string()+"/term_begin.i"))
            ,IndexedDataFile::new(&(dir.to_string()+"/term_end.i"))
            ,IndexedDataFile::new(&(dir.to_string()+"/last_updated.i"))
        ){
            Some(Data{
                data_dir:dir.to_string()
                ,serial
                ,uuid
                ,activity
                ,priority
                ,term_begin
                ,term_end
                ,last_updated
                ,fields_cache:HashMap::new()
            })
        }else{
            None
        }
    }

    fn insert(&mut self,basic_data:&BasicData)->Option<u32>{
        let newid=self.serial.add()?;
        if let(
            Ok(_),Ok(_),Ok(_),Ok(_),Ok(_),Ok(_)
        )=(
            self.uuid.resize_to(newid)
            ,self.activity.resize_to(newid)
            ,self.priority.resize_to(newid)
            ,self.term_begin.resize_to(newid)
            ,self.term_end.resize_to(newid)
            ,self.last_updated.resize_to(newid)
        ){
            self.uuid.triee_mut().update(newid,basic_data.uuid());
            self.activity.triee_mut().update(newid,basic_data.activity());
            self.priority.triee_mut().update(newid,basic_data.priority());
            self.term_begin.triee_mut().update(newid,basic_data.term_begin());
            self.term_end.triee_mut().update(newid,basic_data.term_end());
            self.last_updated.triee_mut().update(newid,basic_data.last_updated());
            Some(newid)
        }else{
            None
        }
    }
    pub fn update(&mut self,id:u32,basic_data:&BasicData)->Option<u32>{
        if !self.serial.exists_blank()&&id==0{   //0は新規作成
            self.insert(basic_data)
        }else{
            if let Some(id)=self.serial.pop_blank(){
                self.uuid.update(id,basic_data.uuid());             //serial_number使いまわしの場合uuid再発行
                self.activity.update(id,basic_data.activity());
                self.priority.update(id,basic_data.priority());
                self.term_begin.update(id,basic_data.term_begin());
                self.term_end.update(id,basic_data.term_end());
                self.last_updated.update(id,basic_data.last_updated());
                Some(id)
            }else{
                self.activity.update(id,basic_data.activity());
                self.priority.update(id,basic_data.priority());
                self.term_begin.update(id,basic_data.term_begin());
                self.term_end.update(id,basic_data.term_end());
                self.last_updated.update(id,basic_data.last_updated());
                Some(id)
            }
        }
    }
    pub fn delete(&mut self,id:u32){
        self.serial.delete(id);
        self.uuid.delete(id);
        self.activity.delete(id);
        self.term_begin.delete(id);
        self.term_end.delete(id);
        self.last_updated.delete(id);
        if let Ok(d)=std::fs::read_dir(self.data_dir.to_string()+"/"){
            for p in d{
                if let Ok(p)=p{
                    let path=p.path();
                    if path.is_dir(){
                        if let Some(fname)=path.file_name(){
                            if let Some(str_fname)=fname.to_str(){
                                if self.fields_cache.contains_key(str_fname)==false{
                                    if let Some(p)=path.to_str(){
                                        if let Ok(field)=field::Field::new(&(p.to_string()+"/")){
                                            self.fields_cache.entry(String::from(str_fname)).or_insert(
                                                field
                                            );
                                        }
                                    }
                                    
                                }
                            }
                        }
                    }
                }
            }
        }
        for (_,v) in &mut self.fields_cache{
            v.delete(id);
        }
    }
    pub fn all(&self,result:&mut std::collections::HashSet<u32>){
        for (_local_index,id,_d) in self.activity.triee().iter(){
            result.replace(id);
        }
    }

    pub fn uuid(&self,id:u32)->Option<u128>{
        self.uuid.triee().entity_value(id).map(|v|*v)
    }

    pub fn activity_index(&self)->&IndexedDataFile<u8>{
        &self.activity
    }
    pub fn activity(&self,id:u32)->Option<u8>{
        self.activity.triee().entity_value(id).map(|v|*v)
    }

    pub fn priority(&self,id:u32)->Option<basic::Priority>{
        self.priority.triee().entity_value(id).map(|v|*v)
    }

    pub fn term_begin_index(&self)->&IndexedDataFile<i64>{
        &self.term_begin
    }
    pub fn term_begin(&self,id:u32)->Option<i64>{
        self.term_begin.triee().entity_value(id).map(|v|*v)
    }

    pub fn term_end_index(&self)->&IndexedDataFile<i64>{
        &self.term_end
    }
    pub fn term_end(&self,id:u32)->Option<i64>{
        self.term_end.triee().entity_value(id).map(|v|*v)
    }

    pub fn last_updated_index(&self)->&IndexedDataFile<i64>{
        &self.last_updated
    }
    pub fn last_updated(&self,id:u32)->Option<i64>{
        self.last_updated.triee().entity_value(id).map(|v|*v)
    }
    pub fn field(&mut self,name:&str)->Option<&field::Field>{
        if let Some(f)=self.field_mut(name,false){
            Some(f)
        }else{
            None
        }
    }
    pub fn field_mut(&mut self,name:&str,create:bool)->Option<&mut field::Field>{
        if self.fields_cache.contains_key(name){
            self.fields_cache.get_mut(name)
        }else{
            let dir_name=self.data_dir.to_string()+"/"+name+"/";
            if create{
                match std::fs::create_dir_all(dir_name.to_owned()){
                    _=>{}
                }
            }
            if std::path::Path::new(&dir_name).exists(){
                if let Ok(field)=field::Field::new(&dir_name){
                    return Some(self.fields_cache.entry(String::from(name)).or_insert(
                        field
                    ));
                }
            }
            None
        }
    }
    pub fn field_value(&mut self,id:u32,name:&str)->Option<&str>{
        if let Some(f)=self.fields_cache.get(name){
            return f.string(id)
        }else{
            let dir_name=self.data_dir.to_string()+"/"+name+"/";
            if std::path::Path::new(&dir_name).exists(){
                if let Ok(field)=field::Field::new(&dir_name){
                    return self.fields_cache.entry(String::from(name)).or_insert(
                        field
                    ).string(id);
                }
            }
        }
        None
    }
}