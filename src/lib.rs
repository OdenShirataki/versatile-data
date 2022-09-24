use std::collections::HashMap;
use uuid::Uuid;

use idx_sized::IdxSized;
use idx_sized::RowSet;

mod serial;
use serial::SerialNumber;

mod field;
pub use field::{
    Field
    ,ConditionField
};

mod search;
pub use search::{
    ConditionTerm
    ,ConditionNumber
    ,Search
    ,Order
    ,SearchResult
};

pub mod prelude;

#[derive(Clone,Copy,PartialEq)]
pub enum Activity{
    Inactive=0
    ,Active=1
}

pub struct Data{
    data_dir:String
    ,serial: SerialNumber
    ,uuid: IdxSized<u128>
    ,activity: IdxSized<u8>
    ,term_begin: IdxSized<i64>
    ,term_end: IdxSized<i64>
    ,last_updated: IdxSized<i64>
    ,fields_cache:HashMap<String,Field>
}
impl Data{
    pub fn new(dir:&str)-> Option<Data>{
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
    )->Option<u32>{
        self.update(0,activity,term_begin,term_end)
    }
    pub fn update(
        &mut self
        ,row:u32
        ,activity: Activity
        ,term_begin: i64
        ,term_end: i64
    )->Option<u32>{
        let term_begin=if term_begin==0{
            chrono::Local::now().timestamp()
        }else{
            term_begin
        };
        if !self.serial.exists_blank()&&row==0{   //0 is new 
            self.update_new(activity,term_begin,term_end)
        }else{
            if let Some(row)=self.serial.pop_blank(){
                self.uuid.update(row,Uuid::new_v4().as_u128()); //recycled serial_number,uuid recreate.
                self.activity.update(row,activity as u8);
                self.term_begin.update(row,term_begin);
                self.term_end.update(row,term_end);
                self.last_updated.update(row,chrono::Local::now().timestamp());
                Some(row)
            }else{
                self.activity.update(row,activity as u8);
                self.term_begin.update(row,term_begin);
                self.term_end.update(row,term_end);
                self.last_updated.update(row,chrono::Local::now().timestamp());
                Some(row)
            }
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
    fn create_field(&mut self,field_name:&str)->Option<&mut Field>{
        let dir_name=self.data_dir.to_string()+"/fields/"+field_name+"/";
        if let Ok(_)=std::fs::create_dir_all(dir_name.to_owned()){
            if std::path::Path::new(&dir_name).exists(){
                if let Ok(field)=field::Field::new(&dir_name){
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
    
    pub fn field(&self,name:&str)->Option<&Field>{
        self.fields_cache.get(name)
    }

    pub fn all(&self)->RowSet{
        self.serial_index().triee().iter().map(|(_,row,_)|row).collect()
    }
    pub fn search_all(&self)->SearchResult{
        SearchResult::new(
            self
            ,None
        )
    }
    pub fn search(&self,condition:&Search)->SearchResult{
        match condition{
            Search::Activity(condition)=>{
                self.search_activity(condition)
            }
            ,Search::Term(condition)=>{
                self.search_term(condition)
            }
            ,Search::Field(field_name,condition)=>{
                self.search_field(&field_name,condition)
            }
            ,Search::Row(condition)=>{
                self.search_row(condition)
            }
            ,Search::LastUpdated(condition)=>{
                self.search_last_updated(condition)
            }
            ,Search::Uuid(uuid)=>{
                SearchResult::new(
                    self
                    ,Some(self.uuid.select_by_value(uuid))
                )
            }
        }
    }
    pub fn search_default(&self)->SearchResult{
        SearchResult::new(
            self
            ,Some(self.search_term_in(chrono::Local::now().timestamp()).intersection(
                &self.activity.select_by_value_from_to(&1,&1)
            ).map(|&x|x).collect())
        )
    }
    fn search_activity(&self,condition:&Activity)->SearchResult{
        let activity=*condition as u8;
        SearchResult::new(self,Some(self.activity.select_by_value_from_to(&activity,&activity)))
    }
    fn search_field(&self,field_name:&str,condition:&ConditionField)->SearchResult{
        if let Some(field)=self.field(field_name){
            SearchResult::new(self,Some(field.search(condition)))
        }else{
            SearchResult::new(self,Some(RowSet::default()))
        }
    }
    fn search_term_in(&self,base:i64)->RowSet{
        let mut result=RowSet::default();
        let tmp=self.term_begin.select_by_value_to(&base);
        for row in tmp{
            let end=self.term_end.value(row).unwrap_or(0);
            if end==0 || end>base {
                result.replace(row);
            }
        }
        result
    }
    fn search_term(&self,condition:&ConditionTerm)->SearchResult{
        SearchResult::new(self,Some(match condition{
            ConditionTerm::In(base)=>{
                self.search_term_in(*base)
            }
            ,ConditionTerm::Future(base)=>{
                self.term_begin_index().select_by_value_from(&base)
            }
            ,ConditionTerm::Past(base)=>{
                self.term_end_index().select_by_value_from_to(&1,&base)
            }
        }))
    }
    fn search_row(&self,condition:&ConditionNumber)->SearchResult{
        let mut r=RowSet::default();
        SearchResult::new(self,Some(match condition{
            ConditionNumber::Min(row)=>{
                for (_,i,_) in self.serial.index().triee().iter(){
                    if i as isize>=*row{
                        r.insert(i);
                    }
                }
                r
            }
            ,ConditionNumber::Max(row)=>{
                for (_,i,_) in self.serial.index().triee().iter(){
                    if i as isize<=*row{
                        r.insert(i);
                    }
                }
                r
            }
            ,ConditionNumber::Range(range)=>{
                for i in range.clone(){
                    if let Some(_)=self.serial.index().triee().node(i as u32){
                        r.insert(i as u32);
                    }
                }
                r
            }
            ,ConditionNumber::In(rows)=>{
                for i in rows{
                    if let Some(_)=self.serial.index().triee().node(*i as u32){
                        r.insert(*i as u32);
                    }
                }
                r
            }
        }))
    }
    fn search_last_updated(&self,condition:&ConditionNumber)->SearchResult{
        SearchResult::new(self,Some(match condition{
            ConditionNumber::Min(v)=>{
                self.last_updated.select_by_value_from(&(*v as i64))
            }
            ,ConditionNumber::Max(v)=>{
                self.last_updated.select_by_value_to(&(*v as i64))
            }
            ,ConditionNumber::Range(range)=>{
                self.last_updated.select_by_value_from_to(
                    &(*range.start() as i64)
                    ,&(*range.end() as i64)
                )
            }
            ,ConditionNumber::In(rows)=>{
                let mut r=RowSet::default();
                for i in rows{
                    for row in self.last_updated.select_by_value(&(*i as i64)){
                        r.insert(row);
                    }
                }
                r
            }
        }))
    }
}
