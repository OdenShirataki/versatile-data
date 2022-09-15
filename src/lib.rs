use std::ffi::CString;
use uuid::Uuid;
use std::collections::HashMap;

use indexed_data_file::IndexedDataFile;

mod serial;
use serial::SerialNumber;

mod field;
pub use field::Field;

mod priority;
pub use priority::Priority;

pub struct Data{
    data_dir:String
    ,serial: SerialNumber
    ,uuid: IndexedDataFile<u128>
    ,activity: IndexedDataFile<u8>
    ,priority: IndexedDataFile<Priority>
    ,term_begin: IndexedDataFile<i64>
    ,term_end: IndexedDataFile<i64>
    ,last_updated: IndexedDataFile<i64>
    ,fields_cache:HashMap<String,Field>
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

    fn update_new(
        &mut self
        ,activity: u8
        ,priority: f64
        ,term_begin: i64
        ,term_end: i64
    )->Option<u32>{
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
            self.uuid.triee_mut().update(newid,Uuid::new_v4().as_u128());
            self.activity.triee_mut().update(newid,activity);
            self.priority.triee_mut().update(newid,Priority::new(priority));
            self.term_begin.triee_mut().update(newid,term_begin);
            self.term_end.triee_mut().update(newid,term_end);
            self.last_updated.triee_mut().update(newid,chrono::Local::now().timestamp());
            Some(newid)
        }else{
            None
        }
    }
    pub fn insert(
        &mut self
        ,activity: u8
        ,priority: f64
        ,term_begin: i64
        ,term_end: i64
    )->Option<u32>{
        self.update(0,activity,priority,term_begin,term_end)
    }
    pub fn update(
        &mut self
        ,id:u32
        ,activity: u8
        ,priority: f64
        ,term_begin: i64
        ,term_end: i64
    )->Option<u32>{
        let term_begin=if term_begin==0{
            chrono::Local::now().timestamp()
        }else{
            term_begin
        };
        if !self.serial.exists_blank()&&id==0{   //0は新規作成
            self.update_new(activity,priority,term_begin,term_end)
        }else{
            if let Some(id)=self.serial.pop_blank(){
                self.uuid.update(id,Uuid::new_v4().as_u128());             //serial_number使いまわしの場合uuid再発行
                self.activity.update(id,activity);
                self.priority.update(id,Priority::new(priority));
                self.term_begin.update(id,term_begin);
                self.term_end.update(id,term_end);
                self.last_updated.update(id,chrono::Local::now().timestamp());
                Some(id)
            }else{
                self.activity.update(id,activity);
                self.priority.update(id,Priority::new(priority));
                self.term_begin.update(id,term_begin);
                self.term_end.update(id,term_end);
                self.last_updated.update(id,chrono::Local::now().timestamp());
                Some(id)
            }
        }
    }
    pub fn load_fields(&mut self){
        if let Ok(d)=std::fs::read_dir(self.data_dir.to_string()+"/fields/"){
            for p in d{
                if let Ok(p)=p{
                    let path=p.path();
                    if path.is_dir(){
                        if let Some(fname)=path.file_name(){
                            if let Some(str_fname)=fname.to_str(){
                                if !self.fields_cache.contains_key(str_fname){
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
    }
    pub fn delete(&mut self,id:u32){
        self.serial.delete(id);
        self.uuid.delete(id);
        self.activity.delete(id);
        self.term_begin.delete(id);
        self.term_end.delete(id);
        self.last_updated.delete(id);
        self.load_fields();
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
    pub fn uuid_str(&self,id:u32)->Option<String>{
        self.uuid.triee().entity_value(id).map(|v|uuid::Uuid::from_u128(*v).to_string())
    }

    pub fn activity_index(&self)->&IndexedDataFile<u8>{
        &self.activity
    }
    pub fn activity(&self,id:u32)->Option<u8>{
        self.activity.triee().entity_value(id).map(|v|*v)
    }

    pub fn priority(&self,id:u32)->Option<Priority>{
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
    pub fn field(&self,name:&str)->Option<&field::Field>{
        self.fields_cache.get(name)
    }
    pub fn update_field_with_ptr(&mut self,id:u32,field_name:&str,addr:*const i8){
        if let Some(field)=self.field_mut(field_name,true){
            field.update(id,addr);
        }
    }
    pub fn update_field(&mut self,id:u32,field_name:&str,cont:impl Into<String>){
        let c_string: CString = CString::new(cont.into()).unwrap();
        self.update_field_with_ptr(id,field_name,c_string.as_ptr());
    }
    pub fn field_str(&self,id:u32,name:&str)->Option<&str>{
        if let Some(f)=self.field(name){
            f.string(id)
        }else{
            None
        }
    }
    pub fn field_num(&self,id:u32,name:&str)->f64{
        if let Some(f)=self.field(name){
            if let Some(f)=f.num(id){
                f
            }else{
                0.0
            }
        }else{
            0.0
        }
    }
    
    fn field_mut(&mut self,name:&str,create:bool)->Option<&mut Field>{
        if self.fields_cache.contains_key(name){
            self.fields_cache.get_mut(name)
        }else{
            let dir_name=self.data_dir.to_string()+"/fields/"+name+"/";
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
}