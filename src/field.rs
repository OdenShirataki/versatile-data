use std::cmp::Ordering;

use idx_sized::{
    IdxSized
    ,RowSet
    ,RemoveResult
};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

pub enum ConditionField{
    Match(Vec<u8>)
    ,Range(Vec<u8>,Vec<u8>)
    ,Min(Vec<u8>)
    ,Max(Vec<u8>)
    ,Forward(String)
    ,Partial(String)
    ,Backward(String)
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
    pub fn entity<'a>(&self,row:u32)->Option<&'a FieldEntity>{
        if let Some(v)=self.index.triee().entity_value(row){
            Some(&v)
        }else{
            None
        }
    }
    pub fn str<'a>(&self,row:u32)->Option<&'a str>{
        if let Some(e)=self.entity(row){
            std::str::from_utf8(unsafe{
                std::slice::from_raw_parts(self.strings.offset(e.addr()) as *const u8,e.len())
            }).ok()
        }else{
            None
        }
    }
    pub fn num(&self,row:u32)->Option<f64>{
        if let Some(e)=self.entity(row){
            Some(e.num())
        }else{
            None
        }
    }
    pub fn index(&self)->&IdxSized<FieldEntity>{
        &self.index
    }
    pub fn update(&mut self,row:u32,content:&[u8]) -> Option<u32>{
        //まずは消す(指定したidのデータが無い場合はスルーされる)
        if let RemoveResult::Unique(data)=self.index.delete(row){
            self.strings.remove(&data.data_address());    //削除対象がユニークの場合は対象文字列を完全削除
        }
        let cont=std::str::from_utf8(content).unwrap();
        let tree=self.index.triee();
        let (ord,found_row)=tree.search_cb(|data|->Ordering{
            let str2=std::str::from_utf8(self.strings.slice(data.data_address())).unwrap();
            if cont==str2{
                Ordering::Equal
            }else{
                natord::compare(cont,str2)
            }
        });
        if ord==Ordering::Equal && found_row!=0{
            if let Some(_node)=self.index.triee().node(row){
                //すでにデータがある場合
                self.index.triee_mut().update_same(found_row,row);
                Some(row)
            }else{
                self.index.insert_same(found_row,row)
            }
        }else{
            //新しく作る
            if let Some(data_address)=self.strings.insert(content){
                let e=FieldEntity::new(
                    data_address.address()
                    ,cont.parse().unwrap_or(0.0)
                );
                if let Some(_entity)=self.index.triee().node(row){
                    //既存データの更新処理
                    self.index.triee_mut().update_node(
                        found_row
                        ,row
                        ,e
                        ,ord
                    );
                    Some(row)
                }else{
                    //追加
                    self.index.insert_unique(e,found_row,ord,row)
                }
            }else{
                None
            }
        }
    }
    pub fn delete(&mut self,row:u32){
        self.index.delete(row);
    }
    
    pub fn search(&self,condition:&ConditionField)->RowSet{
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
        }
    }
    fn search_match(&self,cont:&[u8])->RowSet{
        let mut r:RowSet=RowSet::default();
        let (ord,found_row)=self.search_cb(cont);
        if ord==Ordering::Equal{
            r.insert(found_row);
            for v in self.index.triee().sames(found_row){
                r.insert(v);
            }
        }
        r
    }
    fn search_min(&self,min:&[u8])->RowSet{
        let mut r:RowSet=RowSet::default();
        let (_,min_found_row)=self.search_cb(min);
        for (_,row,_) in self.index.triee().iter_by_row_from(min_found_row){
            r.insert(row);
            for v in self.index.triee().sames(min_found_row){
                r.insert(v);
            }
        }
        r
    }
    fn search_max(&self,max:&[u8])->RowSet{
        let mut r:RowSet=RowSet::default();
        let (_,max_found_row)=self.search_cb(max);
        for (_,row,_) in self.index.triee().iter_by_row_to(max_found_row){
            r.insert(row);
            for v in self.index.triee().sames(max_found_row){
                r.insert(v);
            }
        }
        r
    }
    fn search_range(&self,min:&[u8],max:&[u8])->RowSet{
        let mut r:RowSet=RowSet::default();
        let (_,min_found_row)=self.search_cb(min);
        let (_,max_found_row)=self.search_cb(max);
        for (_,row,_) in self.index.triee().iter_by_row_from_to(min_found_row,max_found_row){
            r.insert(row);
            for v in self.index.triee().sames(max_found_row){
                r.insert(v);
            }
        }
        r
    }
    fn search_forward(&self,cont:&str)->RowSet{
        let mut r:RowSet=RowSet::default();
        let len=cont.len();
        for (_,row,v) in self.index.triee().iter(){
            let data=v.value();
            if len<=data.len(){
                if let Ok(str2)=std::str::from_utf8(unsafe{
                    std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,len)
                }){
                    if cont==str2{
                        r.insert(row);
                    }
                }
            }
        }
        r
    }
    fn search_partial(&self,cont:&str)->RowSet{
        let mut r:RowSet=RowSet::default();
        let len=cont.len();
        for (_,row,v) in self.index.triee().iter(){
            let data=v.value();
            if len<=data.len(){
                if let Ok(str2)=std::str::from_utf8(unsafe{
                    std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,data.len())
                }){
                    if str2.contains(cont){
                        r.insert(row);
                    }
                }
            }
        }
        r
    }
    fn search_backward(&self,cont:&str)->RowSet{
        let mut r:RowSet=RowSet::default();
        let len=cont.len();
        for (_,row,v) in self.index.triee().iter(){
            let data=v.value();
            if len<=data.len(){
                if let Ok(str2)=std::str::from_utf8(unsafe{
                    std::slice::from_raw_parts(self.strings.offset(data.addr()) as *const u8,data.len())
                }){
                    if str2.ends_with(cont){
                        r.insert(row);
                    }
                }
            }
        }
        r
    }
    fn search_cb(&self,cont:&[u8])->(Ordering,u32){
        self.index.triee().search_cb(|data|->Ordering{
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