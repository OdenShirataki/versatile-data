use std::cmp::Ordering;

use idx_sized::{
    IdxSized
    ,Removed
    ,Avltriee
};
use various_data_file::VariousDataFile;

pub mod entity;
use entity::FieldEntity;

pub struct FieldData{
    index: IdxSized<FieldEntity>
    ,strings:VariousDataFile
}
impl FieldData{
    pub fn new(path_prefix:&str) -> Result<FieldData,std::io::Error>{
        let index=IdxSized::new(&(path_prefix.to_string()+".i"))?;
        let strings=VariousDataFile::new(&(path_prefix.to_string()+".d"))?;
        Ok(FieldData{
            index
            ,strings
        })
    }
    pub fn entity<'a>(&self,row:u32)->Option<&'a FieldEntity>{
        if let Some(v)=self.index.triee().entity_value(row){
            Some(&v)
        }else{
            None
        }
    }
    pub fn get<'a>(&self,row:u32)->Option<&[u8]>{
        if let Some(e)=self.entity(row){
            Some(unsafe{
                std::slice::from_raw_parts(self.strings.offset(e.addr()) as *const u8,e.len())
            })
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
    pub fn triee(&self)->&Avltriee<FieldEntity>{
        &self.index.triee()
    }
    pub fn update(&mut self,row:u32,content:&[u8]) -> Option<u32>{
        //まずは消す(指定したidのデータが無い場合はスルーされる)
        if let Removed::Last(data)=self.index.delete(row){
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
    
    pub fn search_cb(&self,cont:&[u8])->(Ordering,u32){
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