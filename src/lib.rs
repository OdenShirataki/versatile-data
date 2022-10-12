use std::sync::{
    Arc,
    RwLock
};
use std::thread;
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

#[derive(Clone,Copy)]
pub enum UpdateTerm{
    Defalut
    ,Overwrite(i64)
}

pub type KeyValue<'a>=(&'a str,Vec<u8>);

#[derive(Clone)]
pub enum Operation<'a>{
    New{
        activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>
    }
    ,Update{
        row:u32
        ,activity:Activity
        ,term_begin:UpdateTerm
        ,term_end:UpdateTerm
        ,fields:Vec<KeyValue<'a>>}
    ,Delete{row:u32}
}


pub struct Data{
    data_dir:String
    ,serial: Arc<RwLock<SerialNumber>>
    ,uuid: Arc<RwLock<IdxSized<u128>>>
    ,activity: Arc<RwLock<IdxSized<u8>>>
    ,term_begin: Arc<RwLock<IdxSized<i64>>>
    ,term_end: Arc<RwLock<IdxSized<i64>>>
    ,last_updated: Arc<RwLock<IdxSized<i64>>>
    ,fields_cache:HashMap<String,Arc<RwLock<FieldData>>>
}
impl Data{
    pub fn new(dir:&str)-> Result<Data,std::io::Error>{
        if !std::path::Path::new(dir).exists(){
            std::fs::create_dir_all(dir).unwrap();
        }
        Ok(Data{
            data_dir:dir.to_string()
            ,serial:Arc::new(RwLock::new(SerialNumber::new(&(dir.to_string()+"/serial"))?))
            ,uuid:Arc::new(RwLock::new(IdxSized::new(&(dir.to_string()+"/uuid.i"))?))
            ,activity:Arc::new(RwLock::new(IdxSized::new(&(dir.to_string()+"/activity.i"))?))
            ,term_begin:Arc::new(RwLock::new(IdxSized::new(&(dir.to_string()+"/term_begin.i"))?))
            ,term_end:Arc::new(RwLock::new(IdxSized::new(&(dir.to_string()+"/term_end.i"))?))
            ,last_updated:Arc::new(RwLock::new(IdxSized::new(&(dir.to_string()+"/last_updated.i"))?))
            ,fields_cache:HashMap::new()
        })
    }

    pub fn update(
        &mut self
        ,operation:&Operation
    )->u32{
        match operation{
            Operation::New{
                activity
                ,term_begin
                ,term_end
                ,fields
            }=>{
                self.create_row(activity,term_begin,term_end,fields)
            }
            ,Operation::Update{
                row
                ,activity
                ,term_begin
                ,term_end
                ,fields
            }=>{
                self.update_row(*row,activity,term_begin,term_end,fields);
                *row
            }
            ,Operation::Delete{row}=>{
                self.delete(*row);
                0
            }
        }
    }

    pub fn create_row(
        &mut self
        ,activity:&Activity
        ,term_begin:&UpdateTerm
        ,term_end:&UpdateTerm
        ,fields:&Vec<KeyValue>
    )->u32{
        if self.serial.read().unwrap().exists_blank(){
            let row=self.serial.write().unwrap().pop_blank().unwrap();
            self.update_row(row,activity,term_begin,term_end,fields);
            row
        }else{
            let row=self.serial.write().unwrap().add().unwrap();
            self.update_row_with_resize(row,activity,term_begin,term_end,fields);
            row
        }
    }

    pub fn update_row(&mut self,row:u32,activity:&Activity,term_begin:&UpdateTerm,term_end:&UpdateTerm,fields:&Vec<KeyValue>){
        let mut handles=Vec::new();

        let index=self.uuid.clone();
        handles.push(thread::spawn(move||{
            index.write().unwrap().update(row,Uuid::new_v4().as_u128()); //recycled serial_number,uuid recreate.
        }));

        handles.push(self.update_activity_async(row,*activity));
    
        handles.push(self.update_term_begin_async(row,if let UpdateTerm::Overwrite(term_begin)=term_begin{
            *term_begin
        }else{
            chrono::Local::now().timestamp()
        }));

        handles.push(self.update_term_end_async(row,if let UpdateTerm::Overwrite(term_end)=term_end{
            *term_end
        }else{
            0
        }));

        handles.append(&mut self.update_fields(row,fields));

        for h in handles{
            h.join().unwrap();
        }
    }
    fn update_row_with_resize(
        &mut self
        ,row:u32
        ,activity:&Activity
        ,term_begin:&UpdateTerm
        ,term_end:&UpdateTerm
        ,fields:&Vec<KeyValue>
    ){
        let mut handles=Vec::new();

        let index=self.uuid.clone();
        handles.push(thread::spawn(move||{
            let mut index=index.write().unwrap();
            if let Ok(_)=index.resize_to(row){
                index.triee_mut().update(row,Uuid::new_v4().as_u128());
            }
        }));

        let activity=*activity as u8;
        let index=self.activity.clone();
        handles.push(thread::spawn(move||{
            let mut index=index.write().unwrap();
            if let Ok(_)=index.resize_to(row){
                index.triee_mut().update(row,activity);
            }
        }));

        let term_begin=if let UpdateTerm::Overwrite(term_begin)=term_begin{
            *term_begin
        }else{
            chrono::Local::now().timestamp()
        };
        let index=self.term_begin.clone();
        handles.push(thread::spawn(move||{
            let mut index=index.write().unwrap();
            if let Ok(_)=index.resize_to(row){
                index.triee_mut().update(row,term_begin);
            }
        }));

        let term_end=if let UpdateTerm::Overwrite(term_end)=term_end{
            *term_end
        }else{
            0
        };
        let index=self.term_end.clone();
        handles.push(thread::spawn(move||{
            let mut index=index.write().unwrap();
            if let Ok(_)=index.resize_to(row){
                index.triee_mut().update(row,term_end);
            }
        }));

        handles.append(&mut self.update_fields(row,fields));

        for h in handles{
            h.join().unwrap();
        }
    }

    fn last_update_now(&mut self,row:u32){
        self.last_updated.write().unwrap().update(row,chrono::Local::now().timestamp());
    } 
    fn update_activity_async(&mut self,row:u32,activity:Activity)->thread::JoinHandle<()>{
        let index=self.activity.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,activity as u8);
        })
    }
    pub fn update_activity(&mut self,row:u32,activity: Activity){
        let h=self.update_activity_async(row,activity);
        self.last_update_now(row);
        h.join().unwrap();
    }
    fn update_term_begin_async(&mut self,row:u32,from:i64)->thread::JoinHandle<()>{
        let index=self.term_begin.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,from);
        })
    }
    pub fn update_term_begin(&mut self,row:u32,from: i64){
        let h=self.update_term_begin_async(row,from);
        self.last_update_now(row);
        h.join().unwrap();
    }
    fn update_term_end_async(&mut self,row:u32,to:i64)->thread::JoinHandle<()>{
        let index=self.term_end.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,to);
        })
    }
    pub fn update_term_end(&mut self,row:u32,to: i64){
        let h=self.update_term_end_async(row,to);
        self.last_update_now(row);
        h.join().unwrap();
    }
    pub fn update_fields(&mut self,row:u32,fields:&Vec<KeyValue>)->Vec<thread::JoinHandle<()>>{
        let mut handles=Vec::new();
        for (fk,fv) in fields.iter(){
            handles.push(self.update_field_async(row,fk,fv));
        }
        self.last_update_now(row);
        handles
    }
    pub fn update_field_async(&mut self,row:u32,field_name:&str,cont:&Vec<u8>)->thread::JoinHandle<()>{
        let field=if self.fields_cache.contains_key(field_name){
            self.fields_cache.get_mut(field_name).unwrap()
        }else{
            self.create_field(field_name)
        };
        let cont=cont.to_owned();
        let index=field.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,&cont);
        })
    }
    pub fn update_field(&mut self,row:u32,field_name:&str,cont:impl Into<String>){
        let field=if self.fields_cache.contains_key(field_name){
            self.fields_cache.get_mut(field_name).unwrap()
        }else{
            self.create_field(field_name)
        };
        let index=field.clone();
        let cont=cont.into();
        let h=thread::spawn(move||{
            index.write().unwrap().update(row,cont.as_bytes());
        });
        self.last_update_now(row);
        h.join().unwrap();
    }
    fn create_field(&mut self,field_name:&str)->&mut Arc<RwLock<FieldData>>{
        let dir_name=self.data_dir.to_string()+"/fields/"+field_name+"/";
        std::fs::create_dir_all(dir_name.to_owned()).unwrap();
        if std::path::Path::new(&dir_name).exists(){
            let field=FieldData::new(&dir_name).unwrap();
            self.fields_cache.entry(String::from(field_name)).or_insert(
                Arc::new(RwLock::new(field))
            );
        }
        self.fields_cache.get_mut(field_name).unwrap()
    }
    pub fn delete(&mut self,row:u32){
        let mut handles=Vec::new();
        let index=self.serial.clone();
        handles.push(thread::spawn(move||{
            index.write().unwrap().delete(row);
        }));
        
        let index=self.uuid.clone();
        handles.push(thread::spawn(move||{
            index.write().unwrap().delete(row);
        }));

        let index=self.activity.clone();
        handles.push(thread::spawn(move||{
            index.write().unwrap().delete(row);
        }));

        let index=self.term_begin.clone();
        handles.push(thread::spawn(move||{
            index.write().unwrap().delete(row);
        }));

        let index=self.term_end.clone();
        handles.push(thread::spawn(move||{
            index.write().unwrap().delete(row);
        }));

        self.load_fields();
        for (_,v) in &mut self.fields_cache{
            let index=v.clone();
            handles.push(thread::spawn(move||{
                index.write().unwrap().delete(row);
            }));
        }

        self.last_updated.write().unwrap().delete(row);

        for h in handles{
            h.join().unwrap();
        }
    }

    pub fn serial(&self,row:u32)->u32{
        if let Some(v)=self.serial.read().unwrap().index().value(row){
            v
        }else{
            0
        }
    }
    pub fn uuid(&self,row:u32)->u128{
        if let Some(v)=self.uuid.read().unwrap().value(row){
            v
        }else{
            0
        }
    }
    pub fn uuid_str(&self,row:u32)->String{
        if let Some(v)=self.uuid.read().unwrap().value(row){
            uuid::Uuid::from_u128(v).to_string()
        }else{
            "".to_string()
        }
    }
    pub fn activity(&self,row:u32)->Activity{
        if let Some(v)=self.activity.read().unwrap().value(row){
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
        if let Some(v)=self.term_begin.read().unwrap().value(row){
            v
        }else{
            0
        }
    }
    pub fn term_end(&self,row:u32)->i64{
        if let Some(v)=self.term_end.read().unwrap().value(row){
            v
        }else{
            0
        }
    }
    pub fn last_updated(&self,row:u32)->i64{
        if let Some(v)=self.last_updated.read().unwrap().value(row){
            v
        }else{
            0
        }
    }
    pub fn field_str(&self,row:u32,name:&str)->&str{
        if let Some(f)=self.field(name){
            if let Some(v)=f.read().unwrap().str(row){
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
            if let Some(f)=f.read().unwrap().num(row){
                f
            }else{
                0.0
            }
        }else{
            0.0
        }
    }

    pub fn fields(&self)->Vec<&String>{
        let mut fields=Vec::new();
        for (key,_) in &self.fields_cache{
            fields.push(key);
        }
        fields
    }
    pub fn load_fields(&mut self){
        let d=std::fs::read_dir(self.data_dir.to_string()+"/fields/").unwrap();
        for p in d{
            if let Ok(p)=p{
                let path=p.path();
                if path.is_dir(){
                    if let Some(fname)=path.file_name(){
                        if let Some(str_fname)=fname.to_str(){
                            if !self.fields_cache.contains_key(str_fname){
                                if let Some(p)=path.to_str(){
                                    let field=FieldData::new(&(p.to_string()+"/")).unwrap();
                                    self.fields_cache.entry(String::from(str_fname)).or_insert(
                                        Arc::new(RwLock::new(field))
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn field(&self,name:&str)->Option<&Arc<RwLock<FieldData>>>{
        self.fields_cache.get(name)
    }

    pub fn all(&self)->RowSet{
        self.serial.read().unwrap().index().triee().iter().map(|r|r.row()).collect()
    }
    pub fn begin_search(&self)->Search{
        Search::new(self)
    }
    pub fn search_field(&self,field_name:impl Into<String>,condition:Field)->Search{
        Search::new(self).search_field(field_name,condition)
    }
    pub fn search_activity(&self,condition:Activity)->Search{
        Search::new(self).search_activity(condition)
    }
    pub fn search_term(&self,condition:Term)->Search{
        Search::new(self).search_term(condition)
    }
    pub fn search_row(&self,condition:Number)->Search{
        Search::new(self).search_row(condition)
    }
    pub fn search_default(&self)->Search{
        Search::new(self).search_default()
    }
}
