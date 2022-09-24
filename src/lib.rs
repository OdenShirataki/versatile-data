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

mod priority;
pub use priority::Priority;

mod search;
pub use search::{
    ConditionActivity
    ,ConditionTerm
    ,ConditionNumber
    ,ConditionFloat
    ,Search
    ,Order
};
use search::Reducer;

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
        ,row:u32
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
        if !self.serial.exists_blank()&&row==0{   //0は新規作成
            self.update_new(activity,priority,term_begin,term_end)
        }else{
            if let Some(row)=self.serial.pop_blank(){
                self.uuid.update(row,Uuid::new_v4().as_u128());             //serial_number使いまわしの場合uuid再発行
                self.activity.update(row,activity as u8);
                self.priority.update(row,Priority::new(priority));
                self.term_begin.update(row,term_begin);
                self.term_end.update(row,term_end);
                self.last_updated.update(row,chrono::Local::now().timestamp());
                Some(row)
            }else{
                self.activity.update(row,activity as u8);
                self.priority.update(row,Priority::new(priority));
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
        ,activity: bool
        ,priority: f64
        ,term_begin: i64
        ,term_end: i64
    )->Option<u32>{
        let row=self.serial.add()?;
        if let(
            Ok(_),Ok(_),Ok(_),Ok(_),Ok(_),Ok(_)
        )=(
            self.uuid.resize_to(row)
            ,self.activity.resize_to(row)
            ,self.priority.resize_to(row)
            ,self.term_begin.resize_to(row)
            ,self.term_end.resize_to(row)
            ,self.last_updated.resize_to(row)
        ){
            self.uuid.triee_mut().update(row,Uuid::new_v4().as_u128());
            self.activity.triee_mut().update(row,activity as u8);
            self.priority.triee_mut().update(row,Priority::new(priority));
            self.term_begin.triee_mut().update(row,term_begin);
            self.term_end.triee_mut().update(row,term_end);
            self.last_updated.triee_mut().update(row,chrono::Local::now().timestamp());
            Some(row)
        }else{
            None
        }
    }

    pub fn all(&self)->RowSet{
        let mut result=RowSet::default();
        for (_local_index,row,_d) in self.activity.triee().iter(){
            result.replace(row);
        }
        result
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
    pub fn activity(&self,row:u32)->bool{
        if let Some(v)=self.activity.value(row){
            v!=0
        }else{
            false
        }
    }
    pub fn priority(&self,row:u32)->f64{
        if let Some(v)=self.priority.value(row){
            v.into()
        }else{
            0.0
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

    pub fn search(&self,condition:&Search)->Reducer{
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
            ,Search::Priority(condition)=>{
                self.search_priority(condition)
            }
            ,Search::Uuid(uuid)=>{
                Reducer::new(
                    self
                    ,self.uuid.select_by_value(uuid)
                )
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
            Reducer::new(self,RowSet::default())
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
    fn search_row(&self,condition:&ConditionNumber)->Reducer{
        let mut r=RowSet::default();
        Reducer::new(self,match condition{
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
        })
    }
    fn search_last_updated(&self,condition:&ConditionNumber)->Reducer{
        Reducer::new(self,match condition{
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
        })
    }
    fn search_priority(&self,condition:&ConditionFloat)->Reducer{
        Reducer::new(self,match condition{
            ConditionFloat::Min(v)=>{
                self.priority.select_by_value_from(&Priority::new(*v))
            }
            ,ConditionFloat::Max(v)=>{
                self.priority.select_by_value_to(&Priority::new(*v))
            }
            ,ConditionFloat::Range(range)=>{
                self.priority.select_by_value_from_to(
                    &Priority::new(*range.start())
                    ,&Priority::new(*range.end())
                )
            }
        })
    }
}
