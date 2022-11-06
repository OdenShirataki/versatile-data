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
    ,data_file:VariousDataFile
}
impl FieldData{
    pub fn new(path_prefix:&str)->Result<Self,std::io::Error>{
        let index=IdxSized::new(&(path_prefix.to_string()+".i"))?;
        let data_file=VariousDataFile::new(&(path_prefix.to_string()+".d"))?;
        Ok(FieldData{
            index
            ,data_file
        })
    }
    pub fn entity(&self,row:u32)->Option<&FieldEntity>{
        if let Some(v)=self.index.triee().value(row){
            Some(&v)
        }else{
            None
        }
    }
    pub fn get<'a>(&self,row:u32)->Option<&'a [u8]>{
        if let Some(e)=self.entity(row){
            Some(unsafe{
                std::slice::from_raw_parts(self.data_file.offset(e.addr()) as *const u8,e.len())
            })
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
    pub fn update(&mut self,row:u32,content:&[u8])->Result<u32,std::io::Error>{
        //まずは消す(指定したidのデータが無い場合はスルーされる)
        if let Removed::Last(data)=self.index.delete(row){
            self.data_file.remove(&data.data_address());    //削除対象がユニークの場合は対象文字列を完全削除
        }
        //TODO:全く同じデータでアップデートしようとしている場合、処理を完全スルーで良いのでは？※データのロードと比較のコストより削除→再登録のコストが低ければこのままで良い
        let cont_str=std::str::from_utf8(content).unwrap();
        let tree=self.index.triee();
        let (ord,found_row)=tree.search_cb(|data|->Ordering{
            let bytes=self.data_file.bytes(data.data_address());
            if content==bytes{
                Ordering::Equal
            }else{
                natord::compare(cont_str,std::str::from_utf8(bytes).unwrap())
            }
        });
        if ord==Ordering::Equal && found_row!=0{
           self.index.insert_same(found_row,row)
        }else{
            //新しく作る
            let data_address=self.data_file.insert(content)?;
            let e=FieldEntity::new(
                data_address.address()
                ,cont_str.parse().unwrap_or(0.0)
            );
            if let Some(_entity)=self.index.triee().node(row){
                //既存データの更新処理
                self.index.triee_mut().update_node(
                    found_row
                    ,row
                    ,e
                    ,ord
                );
                Ok(row)
            }else{
                //追加
                self.index.insert_unique(e,found_row,ord,row)
            }
        }
    }
    pub fn delete(&mut self,row:u32){
        self.index.delete(row);
    }
    
    pub(crate) fn search_cb(&self,cont:&[u8])->(Ordering,u32){
        self.index.triee().search_cb(|data|->Ordering{
            let str2=unsafe{
                std::slice::from_raw_parts(self.data_file.offset(data.addr()) as *const u8,data.len())
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

#[test]
fn test(){
    let dir="./vd-test-fd/";
    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(dir).unwrap();
    if let Ok(mut fd)=FieldData::new(&(dir.to_owned()+"test")){
        fd.update(1,b"Noah").unwrap();
        fd.update(2,b"Liam").unwrap();
        fd.update(3,b"Olivia").unwrap();
        fd.update(1,b"Renamed Noah").unwrap();
        fd.update(2,b"Renamed Liam").unwrap();
        fd.update(3,b"Renamed Olivia").unwrap();
        fd.update(4,b"Noah").unwrap();
        fd.update(5,b"Liam").unwrap();
        fd.update(6,b"Olivia").unwrap();
        fd.update(1,b"Renamed Renamed Noah").unwrap();
        fd.update(2,b"Renamed Renamed Liam").unwrap();
        fd.update(3,b"Renamed Renamed Olivia").unwrap();
        fd.update(4,b"Renamed Noah").unwrap();
        fd.update(5,b"Renamed Liam").unwrap();
        fd.update(6,b"Renamed Olivia").unwrap();
    }
}