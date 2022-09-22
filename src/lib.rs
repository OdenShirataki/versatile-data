use idx_sized::IdSet;
use std::collections::HashSet;
use uuid::Uuid;
use std::collections::HashMap;

use idx_sized::IdxSized;

mod serial;
use serial::SerialNumber;

mod field;
pub use field::{
    Field
    ,ConditionField
};

mod priority;
pub use priority::Priority;

mod search;
pub use search::{
    ConditionActivity
    ,ConditionTerm
    ,SearchCondition
    ,Reducer
};

pub struct Data{
    data_dir:String
    ,serial: SerialNumber
    ,uuid: IdxSized<u128>
    ,activity: IdxSized<u8>
    ,priority: IdxSized<Priority>
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
            ,Ok(priority)
            ,Ok(term_begin)
            ,Ok(term_end)
            ,Ok(last_updated)
        )=(
            SerialNumber::new(&(dir.to_string()+"/serial"))
            ,IdxSized::new(&(dir.to_string()+"/uuid.i"))
            ,IdxSized::new(&(dir.to_string()+"/activity.i"))
            ,IdxSized::new(&(dir.to_string()+"/priority.i"))
            ,IdxSized::new(&(dir.to_string()+"/term_begin.i"))
            ,IdxSized::new(&(dir.to_string()+"/term_end.i"))
            ,IdxSized::new(&(dir.to_string()+"/last_updated.i"))
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
    pub fn insert(
        &mut self
        ,activity: bool
        ,priority: f64
        ,term_begin: i64
        ,term_end: i64
    )->Option<u32>{
        self.update(0,activity,priority,term_begin,term_end)
    }
    pub fn update(
        &mut self
        ,id:u32
        ,activity: bool
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
                self.activity.update(id,activity as u8);
                self.priority.update(id,Priority::new(priority));
                self.term_begin.update(id,term_begin);
                self.term_end.update(id,term_end);
                self.last_updated.update(id,chrono::Local::now().timestamp());
                Some(id)
            }else{
                self.activity.update(id,activity as u8);
                self.priority.update(id,Priority::new(priority));
                self.term_begin.update(id,term_begin);
                self.term_end.update(id,term_end);
                self.last_updated.update(id,chrono::Local::now().timestamp());
                Some(id)
            }
        }
    }
    pub fn update_field(&mut self,id:u32,field_name:&str,cont:impl Into<String>){
        if let Some(field)=self.field_mut(field_name,true){
            field.update(id,cont.into().as_bytes());
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

    fn update_new(
        &mut self
        ,activity: bool
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
            self.activity.triee_mut().update(newid,activity as u8);
            self.priority.triee_mut().update(newid,Priority::new(priority));
            self.term_begin.triee_mut().update(newid,term_begin);
            self.term_end.triee_mut().update(newid,term_end);
            self.last_updated.triee_mut().update(newid,chrono::Local::now().timestamp());
            Some(newid)
        }else{
            None
        }
    }

    pub fn all(&self)->HashSet<u32>{
        let mut result=HashSet::new();
        for (_local_index,id,_d) in self.activity.triee().iter(){
            result.replace(id);
        }
        result
    }

    pub fn uuid(&self,id:u32)->u128{
        if let Some(v)=self.uuid.value(id){
            v
        }else{
            0
        }
    }
    pub fn uuid_str(&self,id:u32)->String{
        if let Some(v)=self.uuid.value(id){
            uuid::Uuid::from_u128(v).to_string()
        }else{
            "".to_string()
        }
    }
    pub fn activity(&self,id:u32)->bool{
        if let Some(v)=self.activity.value(id){
            v!=0
        }else{
            false
        }
    }
    pub fn priority(&self,id:u32)->f64{
        if let Some(v)=self.priority.value(id){
            v.into()
        }else{
            0.0
        }
    }
    pub fn term_begin(&self,id:u32)->i64{
        if let Some(v)=self.term_begin.value(id){
            v
        }else{
            0
        }
    }
    pub fn term_end(&self,id:u32)->i64{
        if let Some(v)=self.term_end.value(id){
            v
        }else{
            0
        }
    }
    pub fn last_updated(&self,id:u32)->i64{
        if let Some(v)=self.last_updated.value(id){
            v
        }else{
            0
        }
    }
    pub fn field_str(&self,id:u32,name:&str)->&str{
        if let Some(f)=self.field(name){
            if let Some(v)=f.str(id){
                v
            }else{
                ""
            }
        }else{
            ""
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

    pub fn search(&self,condition:&SearchCondition)->Reducer{
        match condition{
            SearchCondition::Activity(condition)=>{
                self.search_activity(condition)
            }
            ,SearchCondition::Term(condition)=>{
                self.search_term(condition)
            }
            ,SearchCondition::Field(field_name,condition)=>{
                self.search_field(&field_name,condition)
            }
        }
    }
    pub fn search_default(&self)->Reducer{
        Reducer::new(
            self
            ,self.search_term_in(chrono::Local::now().timestamp()).intersection(
                &self.activity.select_by_value_from_to(&1,&1)
            ).map(|&x|x).collect()
        )
    }
    fn search_activity(&self,condition:&ConditionActivity)->Reducer{
        let activity=if *condition==ConditionActivity::Active{ 1 }else{ 0 };
        Reducer::new(self,self.activity.select_by_value_from_to(&activity,&activity))
    }
    fn search_field(&self,field_name:&str,condition:&ConditionField)->Reducer{
        if let Some(field)=self.field(field_name){
            Reducer::new(self,field.search(condition))
        }else{
            Reducer::new(self,IdSet::default())
        }
    }
    fn search_term_in(&self,base:i64)->IdSet{
        let mut result=IdSet::default();
        let tmp=self.term_begin_index().select_by_value_to(&base);
        let index_end=self.term_end_index();
        for id in tmp{
            let end=index_end.value(id).unwrap_or(0);
            if end==0 || end>base {
                result.replace(id);
            }
        }
        result
    }
    fn search_term(&self,condition:&ConditionTerm)->Reducer{
        Reducer::new(self,match condition{
            ConditionTerm::In(base)=>{
                self.search_term_in(*base)
            }
            ,ConditionTerm::Future(base)=>{ //公開開始が未来のもののみ
                self.term_begin_index().select_by_value_from(&base)
            }
            ,ConditionTerm::Past(base)=>{   //公開終了のみ
                self.term_end_index().select_by_value_from_to(&1,&base)
            }
        })
    }
}
