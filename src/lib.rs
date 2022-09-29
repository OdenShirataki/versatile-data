use std::collections::HashMap;
use uuid::Uuid;

pub use idx_sized::{
    IdxSized
    ,RowSet
};

mod serial;
use serial::SerialNumber;

mod field;
pub use field::FieldData;
pub use search::Field;

mod search;
pub use search::{
    Term
    ,Number
    ,Search
    ,Condition
    ,Order
};

pub mod prelude;

#[derive(Clone,Copy,PartialEq)]
pub enum Activity{
    Inactive=0
    ,Active=1
}

type KeyValu<'a>=(&'a str,String);

pub struct Data{
    data_dir:String
    ,serial: SerialNumber
    ,uuid: IdxSized<u128>
    ,activity: IdxSized<u8>
    ,term_begin: IdxSized<i64>
    ,term_end: IdxSized<i64>
    ,last_updated: IdxSized<i64>
    ,fields_cache:HashMap<String,FieldData>
}
impl Data{
    pub fn new(dir:&str)-> Option<Data>{
        if !std::path::Path::new(dir).exists(){
            std::fs::create_dir_all(dir).unwrap();
        }
        if let (
            Ok(serial)
            ,Ok(uuid)
            ,Ok(activity)
            ,Ok(term_begin)
            ,Ok(term_end)
            ,Ok(last_updated)
        )=(
            SerialNumber::new(&(dir.to_string()+"/serial"))
            ,IdxSized::new(&(dir.to_string()+"/uuid.i"))
            ,IdxSized::new(&(dir.to_string()+"/activity.i"))
            ,IdxSized::new(&(dir.to_string()+"/term_begin.i"))
            ,IdxSized::new(&(dir.to_string()+"/term_end.i"))
            ,IdxSized::new(&(dir.to_string()+"/last_updated.i"))
        ){
            Some(Data{
                data_dir:dir.to_string()
                ,serial
                ,uuid
                ,activity
                ,term_begin
                ,term_end
                ,last_updated
                ,fields_cache:HashMap::new()
            })
        }else{
            None
        }
    }
    pub fn insert(
        &mut self
        ,activity: Activity
        ,term_begin: i64
        ,term_end: i64
        ,fields:&Vec<KeyValu>
    )->Option<u32>{
        self.update(0,activity,term_begin,term_end,fields)
    }
    pub fn update(
        &mut self
        ,row:u32
        ,activity: Activity
        ,term_begin: i64
        ,term_end: i64
        ,fields:&Vec<KeyValu>
    )->Option<u32>{
        let term_begin=if term_begin==0{
            chrono::Local::now().timestamp()
        }else{
            term_begin
        };
        if !self.serial.exists_blank()&&row==0{   //0 is new 
            self.update_new(activity,term_begin,term_end,fields)
        }else{
            if let Some(row)=self.serial.pop_blank(){
                self.uuid.update(row,Uuid::new_v4().as_u128()); //recycled serial_number,uuid recreate.
                self.activity.update(row,activity as u8);
                self.term_begin.update(row,term_begin);
                self.term_end.update(row,term_end);
                self.last_updated.update(row,chrono::Local::now().timestamp());
                self.update_fields(row,fields);
                Some(row)
            }else{
                self.activity.update(row,activity as u8);
                self.term_begin.update(row,term_begin);
                self.term_end.update(row,term_end);
                self.last_updated.update(row,chrono::Local::now().timestamp());
                self.update_fields(row,fields);
                Some(row)
            }
        }
    }
    pub fn update_activity(&mut self,row:u32,activity: Activity){
        self.activity.update(row,activity as u8);
        self.last_updated.update(row,chrono::Local::now().timestamp());
    }
    pub fn update_term_begin(&mut self,row:u32,from: i64){
        self.term_begin.update(row,from);
        self.last_updated.update(row,chrono::Local::now().timestamp());
    }
    pub fn update_term_end(&mut self,row:u32,to: i64){
        self.term_end.update(row,to);
        self.last_updated.update(row,chrono::Local::now().timestamp());
    }
    pub fn update_fields(&mut self,row:u32,fields:&Vec<KeyValu>){
        for (fk,fv) in fields.iter(){
            self.update_field(row,fk,fv);
        }
    }
    pub fn update_field(&mut self,row:u32,field_name:&str,cont:impl Into<String>){
        if let Some(field)=if self.fields_cache.contains_key(field_name){
            self.fields_cache.get_mut(field_name)
        }else{
            self.create_field(field_name)
        }{
            field.update(row,cont.into().as_bytes());
        }
    }
    fn create_field(&mut self,field_name:&str)->Option<&mut FieldData>{
        let dir_name=self.data_dir.to_string()+"/fields/"+field_name+"/";
        if let Ok(_)=std::fs::create_dir_all(dir_name.to_owned()){
            if std::path::Path::new(&dir_name).exists(){
                if let Ok(field)=FieldData::new(&dir_name){
                    self.fields_cache.entry(String::from(field_name)).or_insert(
                        field
                    );
                }
            }
        }
        self.fields_cache.get_mut(field_name)
    }
    pub fn delete(&mut self,row:u32){
        self.serial.delete(row);
        self.uuid.delete(row);
        self.activity.delete(row);
        self.term_begin.delete(row);
        self.term_end.delete(row);
        self.last_updated.delete(row);
        self.load_fields();
        for (_,v) in &mut self.fields_cache{
            v.delete(row);
        }
    }

    fn update_new(
        &mut self
        ,activity: Activity
        ,term_begin: i64
        ,term_end: i64
        ,fields:&Vec<KeyValu>
    )->Option<u32>{
        let row=self.serial.add()?;
        if let(
            Ok(_),Ok(_),Ok(_),Ok(_),Ok(_)
        )=(
            self.uuid.resize_to(row)
            ,self.activity.resize_to(row)
            ,self.term_begin.resize_to(row)
            ,self.term_end.resize_to(row)
            ,self.last_updated.resize_to(row)
        ){
            self.uuid.triee_mut().update(row,Uuid::new_v4().as_u128());
            self.activity.triee_mut().update(row,activity as u8);
            self.term_begin.triee_mut().update(row,term_begin);
            self.term_end.triee_mut().update(row,term_end);
            self.last_updated.triee_mut().update(row,chrono::Local::now().timestamp());
            self.update_fields(row,fields);
            Some(row)
        }else{
            None
        }
    }

    pub fn serial(&self,row:u32)->u32{
        if let Some(v)=self.serial.index().value(row){
            v
        }else{
            0
        }
    }
    pub fn uuid(&self,row:u32)->u128{
        if let Some(v)=self.uuid.value(row){
            v
        }else{
            0
        }
    }
    pub fn uuid_str(&self,row:u32)->String{
        if let Some(v)=self.uuid.value(row){
            uuid::Uuid::from_u128(v).to_string()
        }else{
            "".to_string()
        }
    }
    pub fn activity(&self,row:u32)->Activity{
        if let Some(v)=self.activity.value(row){
            if v!=0{
                Activity::Active
            }else{
                Activity::Inactive
            }
        }else{
            Activity::Inactive
        }
    }
    pub fn term_begin(&self,row:u32)->i64{
        if let Some(v)=self.term_begin.value(row){
            v
        }else{
            0
        }
    }
    pub fn term_end(&self,row:u32)->i64{
        if let Some(v)=self.term_end.value(row){
            v
        }else{
            0
        }
    }
    pub fn last_updated(&self,row:u32)->i64{
        if let Some(v)=self.last_updated.value(row){
            v
        }else{
            0
        }
    }
    pub fn field_str(&self,row:u32,name:&str)->&str{
        if let Some(f)=self.field(name){
            if let Some(v)=f.str(row){
                v
            }else{
                ""
            }
        }else{
            ""
        }
    }
    pub fn field_num(&self,row:u32,name:&str)->f64{
        if let Some(f)=self.field(name){
            if let Some(f)=f.num(row){
                f
            }else{
                0.0
            }
        }else{
            0.0
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
                                        if let Ok(field)=FieldData::new(&(p.to_string()+"/")){
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

    pub fn serial_index(&self)->&IdxSized<u32>{
        &self.serial.index()
    }
    pub fn activity_index(&self)->&IdxSized<u8>{
        &self.activity
    }
    pub fn term_begin_index(&self)->&IdxSized<i64>{
        &self.term_begin
    }
    pub fn term_end_index(&self)->&IdxSized<i64>{
        &self.term_end
    }
    pub fn last_updated_index(&self)->&IdxSized<i64>{
        &self.last_updated
    }
    
    pub fn field(&self,name:&str)->Option<&FieldData>{
        self.fields_cache.get(name)
    }

    pub fn all(&self)->RowSet{
        self.serial_index().triee().iter().map(|(_,row,_)|row).collect()
    }
    pub fn begin_search(&self)->Search{
        Search::new(self)
    }
    pub fn search(&self,condition:&Condition)->Search{
        let r=Search::new(self);
        r.search(condition)
    }
    pub fn search_default(&self)->Search{
        let r=Search::new(self);
        r.search_default()
    }
}
