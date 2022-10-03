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
pub enum Update{
    New
    ,Row(u32)
}

pub type KeyValue<'a>=(&'a str,String);

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
                ,serial:Arc::new(RwLock::new(serial))
                ,uuid:Arc::new(RwLock::new(uuid))
                ,activity:Arc::new(RwLock::new(activity))
                ,term_begin:Arc::new(RwLock::new(term_begin))
                ,term_end:Arc::new(RwLock::new(term_end))
                ,last_updated:Arc::new(RwLock::new(last_updated))
                ,fields_cache:HashMap::new()
            })
        }else{
            None
        }
    }
    pub fn update(
        &mut self
        ,update:Update
        ,activity: Activity
        ,term_begin: i64
        ,term_end: i64
        ,fields:&Vec<KeyValue>
    )->Option<u32>{
        let term_begin=if term_begin==0{
            chrono::Local::now().timestamp()
        }else{
            term_begin
        };
        match update{
            Update::New=>{
                if self.serial.read().unwrap().exists_blank(){
                    let row=self.serial.write().unwrap().pop_blank();
                    if let Some(row)=row{
                        let mut handles=Vec::new();

                        let index=self.uuid.clone();
                        handles.push(thread::spawn(move||{
                            index.write().unwrap().update(row,Uuid::new_v4().as_u128()); //recycled serial_number,uuid recreate.
                        }));

                        handles.push(self.update_activity_async(row,activity));
                        handles.push(self.update_term_begin_async(row,term_begin));
                        handles.push(self.update_term_endasync(row,term_end));

                        handles.append(&mut self.update_fields(row,fields));

                        for h in handles{
                            h.join().unwrap();
                        }
                        Some(row)
                    }else{
                        None
                    }
                }else{
                    self.update_new(activity,term_begin,term_end,fields)
                }
            }
            ,Update::Row(row)=>{
                let mut handles=Vec::new();

                handles.push(self.update_activity_async(row,activity));
                handles.push(self.update_term_begin_async(row,term_begin));
                handles.push(self.update_term_endasync(row,term_end));

                handles.append(&mut self.update_fields(row,fields));

                for h in handles{
                    h.join().unwrap();
                }
                Some(row)
            }
        }
        
    }
    fn last_update_now(&mut self,row:u32)->thread::JoinHandle<()>{
        let index=self.last_updated.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,chrono::Local::now().timestamp());
        })
    } 
    fn update_activity_async(&mut self,row:u32,activity:Activity)->thread::JoinHandle<()>{
        let index=self.activity.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,activity as u8);
        })
    }
    pub fn update_activity(&mut self,row:u32,activity: Activity){
        let h1=self.update_activity_async(row,activity);
        let h2=self.last_update_now(row);
        h1.join().unwrap();
        h2.join().unwrap();
    }
    fn update_term_begin_async(&mut self,row:u32,from:i64)->thread::JoinHandle<()>{
        let index=self.term_begin.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,from);
        })
    }
    pub fn update_term_begin(&mut self,row:u32,from: i64){
        let h1=self.update_term_begin_async(row,from);
        let h2=self.last_update_now(row);
        h1.join().unwrap();
        h2.join().unwrap();
    }
    fn update_term_endasync(&mut self,row:u32,to:i64)->thread::JoinHandle<()>{
        let index=self.term_end.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,to);
        })
    }
    pub fn update_term_end(&mut self,row:u32,to: i64){
        let h1=self.update_term_endasync(row,to);
        let h2=self.last_update_now(row);
        h1.join().unwrap();
        h2.join().unwrap();
    }
    pub fn update_fields(&mut self,row:u32,fields:&Vec<KeyValue>)->Vec<thread::JoinHandle<()>>{
        let mut handles=Vec::new();
        for (fk,fv) in fields.iter(){
            handles.push(self.update_field_async(row,fk,fv));
        }
        handles.push(self.last_update_now(row));
        handles
    }
    pub fn update_field_async(&mut self,row:u32,field_name:&str,cont:impl Into<String>)->thread::JoinHandle<()>{
        let field=if self.fields_cache.contains_key(field_name){
            self.fields_cache.get_mut(field_name)
        }else{
            self.create_field(field_name)
        }.unwrap();
        let cont=cont.into();
        let index=field.clone();
        thread::spawn(move||{
            index.write().unwrap().update(row,cont.as_bytes());
        })
    }
    pub fn update_field(&mut self,row:u32,field_name:&str,cont:impl Into<String>){
        if let Some(field)=if self.fields_cache.contains_key(field_name){
            self.fields_cache.get_mut(field_name)
        }else{
            self.create_field(field_name)
        }{
            let index=field.clone();
            let cont=cont.into();
            index.write().unwrap().update(row,cont.as_bytes());
        }
    }
    fn create_field(&mut self,field_name:&str)->Option<&mut Arc<RwLock<FieldData>>>{
        let dir_name=self.data_dir.to_string()+"/fields/"+field_name+"/";
        if let Ok(_)=std::fs::create_dir_all(dir_name.to_owned()){
            if std::path::Path::new(&dir_name).exists(){
                if let Ok(field)=FieldData::new(&dir_name){
                    self.fields_cache.entry(String::from(field_name)).or_insert(
                        Arc::new(RwLock::new(field))
                    );
                }
            }
        }
        self.fields_cache.get_mut(field_name)
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

        let index=self.last_updated.clone();
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

        for h in handles{
            h.join().unwrap();
        }
    }

    fn update_new(
        &mut self
        ,activity: Activity
        ,term_begin: i64
        ,term_end: i64
        ,fields:&Vec<KeyValue>
    )->Option<u32>{
        let row=self.serial.write().unwrap().add();
        if let Some(row)=row{
            let mut handles=Vec::new();

            let index=self.uuid.clone();
            handles.push(thread::spawn(move||{
                let mut index=index.write().unwrap();
                if let Ok(_)=index.resize_to(row){
                    index.triee_mut().update(row,Uuid::new_v4().as_u128());
                }
            }));

            let index=self.activity.clone();
            handles.push(thread::spawn(move||{
                let mut index=index.write().unwrap();
                if let Ok(_)=index.resize_to(row){
                    index.triee_mut().update(row,activity as u8);
                }
            }));

            let index=self.term_begin.clone();
            handles.push(thread::spawn(move||{
                let mut index=index.write().unwrap();
                if let Ok(_)=index.resize_to(row){
                    index.triee_mut().update(row,term_begin);
                }
            }));

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

            Some(row)
        }else{
            None
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
        }
    }

    pub fn field(&self,name:&str)->Option<&Arc<RwLock<FieldData>>>{
        self.fields_cache.get(name)
    }

    pub fn all(&self)->RowSet{
        self.serial.read().unwrap().index().triee().iter().map(|(_,row,_)|row).collect()
    }
    pub fn begin_search(&self)->Search{
        Search::new(self)
    }
    pub fn search(&self,condition:Condition)->Search{
        let r=Search::new(self);
        r.search(condition)
    }
    pub fn search_default(&self)->Search{
        let r=Search::new(self);
        r.search_default()
    }
}
