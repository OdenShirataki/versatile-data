use std::collections::HashSet;
use std::cmp::Ordering;

use idx_sized::{
    IdxSized
    ,IdSet
    ,RemoveResult
};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

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
            Some(unsafe{std::ffi::CStr::from_ptr(
                self.strings.offset(e.addr()) as *const libc::c_char
            )}.to_str().unwrap())
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
    pub fn search_match(&self,str:&str,and:Option<IdSet>)->IdSet{
        let mut r:IdSet=HashSet::default();
        let tree=self.index.triee();
        let (ord,found_id)=tree.search_cb(|data|->Ordering{
            let str2=unsafe{
                std::ffi::CStr::from_ptr(
                    self.strings.offset(data.addr()) as *const libc::c_char
                )
            }.to_str().unwrap();
            if str==str2{
                Ordering::Equal
            }else{
                natord::compare(str,str2)
            }
        });
        let found_id_i64=found_id as i64;
        if let Some(and)=and{
            if ord==Ordering::Equal{
                if and.contains(&found_id_i64){
                    r.insert(found_id_i64);
                }
                tree.sames_and(&mut r,&and, found_id);
            }
        }else{
            if ord==Ordering::Equal{
                r.insert(found_id_i64);
                tree.sames(&mut r, found_id);
            }
        }
        r
    }
    
    pub fn update(&mut self,id:u32,content:&[u8]) -> Option<u32>{
        //まずは消す(指定したidのデータが無い場合はスルーされる)
        if let RemoveResult::Unique(data)=self.index.delete(id){
            self.strings.remove(&data.word());    //削除対象がユニークの場合は対象文字列を完全削除
        }
        let cont=std::str::from_utf8(content).unwrap();
        let tree=self.index.triee();
        let (ord,found_id)=tree.search_cb(|data|->Ordering{
            let str2=std::str::from_utf8(self.strings.slice(data.word())).unwrap();

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
            if let Some(word)=self.strings.insert(content){
                let e=FieldEntity::new(
                    word.address()
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
                    self.index.insert_unique(e,found_id,ord)
                }
            }else{
                None
            }
        }
    }
    pub fn delete(&mut self,id:u32){
        self.index.delete(id);
    }
}