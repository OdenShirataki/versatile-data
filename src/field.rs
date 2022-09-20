use std::cmp::Ordering;

use idx_sized::{
    IdxSized
    ,IdSet
    ,RemoveResult
};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

#[derive(Clone)]
pub enum ConditionField<'a>{
    Match(&'a [u8])
    ,Range(&'a [u8],&'a [u8])
    ,Min(&'a [u8])
    ,Max(&'a [u8])
    ,Forward(&'a str)
    ,Partial(&'a str)
    ,Backward(&'a str)
}

pub struct Field{
    index: IdxSized<FieldEntity>
    ,strings:VariousDataFile
}
impl Field{
    pub fn new(path_prefix:&str) -> Result<Field,std::io::Error>{
        match IdxSized::new(&(path_prefix.to_string()+".i")){
            Err(e)=>Err(e)
            ,Ok(index)=>{
                match VariousDataFile::new(&(path_prefix.to_string()+".d")){
                    Ok(strings)=>Ok(Field{
                        index
                        ,strings
                    })
                    ,Err(e)=>Err(e)
                }
            }
        }
    }
    pub fn entity<'a>(&self,id:u32)->Option<&'a FieldEntity>{
        if let Some(v)=self.index.triee().entity_value(id){
            Some(&v)
        }else{
            None
        }
    }
    pub fn str<'a>(&self,id:u32)->Option<&'a str>{
        if let Some(e)=self.entity(id){
            std::str::from_utf8(unsafe{
                std::slice::from_raw_parts(self.strings.offset(e.addr()) as *const u8,e.len())
            }).ok()
        }else{
            None
        }
    }
    pub fn num(&self,id:u32)->Option<f64>{
        if let Some(e)=self.entity(id){
            Some(e.num())
        }else{
            None
        }
    }
    pub fn update(&mut self,id:u32,content:&[u8]) -> Option<u32>{
        //まずは消す(指定したidのデータが無い場合はスルーされる)
        if let RemoveResult::Unique(data)=self.index.delete(id){
            self.strings.remove(&data.data_address());    //削除対象がユニークの場合は対象文字列を完全削除
        }
        let cont=std::str::from_utf8(content).unwrap();
        let tree=self.index.triee();
        let (ord,found_id)=tree.search_cb(|data|->Ordering{
            let str2=std::str::from_utf8(self.strings.slice(data.data_address())).unwrap();
            if cont==str2{
                Ordering::Equal
            }else{
                natord::compare(cont,str2)
            }
        });
        if ord==Ordering::Equal && found_id!=0{
            if let Some(_node)=self.index.triee().node(id){
                //すでにデータがある場合
                self.index.triee_mut().update_same(found_id,id);
                Some(id)
            }else{
                self.index.insert_same(found_id)
            }
        }else{
            //新しく作る
            if let Some(data_address)=self.strings.insert(content){
                let e=FieldEntity::new(
                    data_address.address()
                    ,cont.parse().unwrap_or(0.0)
                );
                if let Some(_entity)=self.index.triee().node(id){
                    //既存データの更新処理
                    self.index.triee_mut().update_node(
                        found_id
                        ,id
                        ,e
                        ,ord
                    );
                    Some(id)
                }else{
                    //追加
                    self.index.insert_unique(e,found_id,ord,id)    //idが1から始まるとは限らない
                }
            }else{
                None
            }
        }
    }
    pub fn delete(&mut self,id:u32){
        self.index.delete(id);
    }
    
    pub fn search(&self,condition:ConditionField)->IdSet{
        match condition{
            ConditionField::Match(v)=>{
                self.search_match(v)
            }
            ,ConditionField::Min(min)=>{
                self.search_min(min)
            }
            ,ConditionField::Max(max)=>{
                self.search_max(max)
            }
            ,ConditionField::Range(min,max)=>{
                self.search_range(min,max)
            }
            ,ConditionField::Forward(cont)=>{
                self.search_forward(cont)
            }
            ,ConditionField::Partial(cont)=>{
                self.search_partial(cont)
            }
            ,ConditionField::Backward(cont)=>{
                self.search_backward(cont)
            }
            //,_=>IdSet::default()
        }
    }
    fn search_match(&self,cont:&[u8])->IdSet{
        let mut r:IdSet=IdSet::default();
        let (ord,found_id)=self.search_cb(0,cont);
        if ord==Ordering::Equal{
            r.insert(found_id);
            self.index.triee().sames(&mut r, found_id);
        }
        r
    }
    fn search_min(&self,min:&[u8])->IdSet{
        let mut r:IdSet=IdSet::default();
        let (_,min_found_id)=self.search_cb(0,min);
        for (_,id,_) in self.index.triee().iter_by_id_from(min_found_id){
            r.insert(id);
            self.index.triee().sames(&mut r, min_found_id);
        }
        r
    }
    fn search_max(&self,max:&[u8])->IdSet{
        let mut r:IdSet=IdSet::default();
        let (_,max_found_id)=self.search_cb(0,max);
        for (_,id,_) in self.index.triee().iter_by_id_to(max_found_id){
            r.insert(id);
            self.index.triee().sames(&mut r, max_found_id);
        }
        r
    }
    fn search_range(&self,min:&[u8],max:&[u8])->IdSet{
        let mut r:IdSet=IdSet::default();
        let (_,min_found_id)=self.search_cb(0,min);
        let (_,max_found_id)=self.search_cb(min_found_id,max);
        for (_,id,_) in self.index.triee().iter_by_id_from_to(min_found_id,max_found_id){
            r.insert(id);
            self.index.triee().sames(&mut r, max_found_id);
        }
        r
    }
    fn search_forward(&self,cont:&str)->IdSet{
        let mut r:IdSet=IdSet::default();
        let len=cont.len();
        for (_,id,v) in self.index.triee().iter(){
            let data=v.value();
            if len<=data.len(){
                if let Ok(str2)=std::str::from_utf8(unsafe{
                    std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,len)
                }){
                    if cont==str2{
                        r.insert(id);
                    }
                }
            }
        }
        r
    }
    fn search_partial(&self,cont:&str)->IdSet{
        let mut r:IdSet=IdSet::default();
        let len=cont.len();
        for (_,id,v) in self.index.triee().iter(){
            let data=v.value();
            if len<=data.len(){
                if let Ok(str2)=std::str::from_utf8(unsafe{
                    std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,data.len())
                }){
                    if str2.contains(cont){
                        r.insert(id);
                    }
                }
            }
        }
        r
    }
    fn search_backward(&self,cont:&str)->IdSet{
        let mut r:IdSet=IdSet::default();
        let len=cont.len();
        for (_,id,v) in self.index.triee().iter(){
            let data=v.value();
            if len<=data.len(){
                if let Ok(str2)=std::str::from_utf8(unsafe{
                    std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,data.len())
                }){
                    if str2.ends_with(cont){
                        r.insert(id);
                    }
                }
            }
        }
        r
    }
    fn search_cb(&self,from:u32,cont:&[u8])->(Ordering,u32){
        self.index.triee().search_cb_from(from,|data|->Ordering{
            let str2=unsafe{
                std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,data.len())
            };
            if cont==str2{
                Ordering::Equal
            }else{
                natord::compare(
                    std::str::from_utf8(cont).unwrap()
                    ,std::str::from_utf8(str2).unwrap()
                )
            }
        })
    }
    
}