use std::ops::RangeInclusive;
use idx_sized::RowSet;

use crate::{
    Data
    ,Activity
    ,ConditionField
};

#[derive(Clone,Copy,PartialEq)]
pub enum ConditionTerm{
    In(i64)
    ,Past(i64)
    ,Future(i64)
}

pub enum ConditionNumber{
    Min(isize)
    ,Max(isize)
    ,Range(RangeInclusive<isize>)
    ,In(Vec<isize>)
}

pub enum Search{
    Activity(Activity)
    ,Term(ConditionTerm)
    ,Row(ConditionNumber)
    ,Uuid(u128)
    ,LastUpdated(ConditionNumber)
    ,Field(String,ConditionField)
}

pub enum Order<'a>{
    Serial
    ,Row
    ,TermBegin
    ,TermEnd
    ,LastUpdated
    ,Field(&'a str)
}

#[derive(Clone)]
pub struct SearchResult<'a>{
    data:&'a Data
    ,result:Option<RowSet>
}
impl<'a> SearchResult<'a>{
    pub fn new(data:&'a Data,result:Option<RowSet>)->SearchResult{
        SearchResult{
            data
            ,result
        }
    }
    pub fn get(self)->RowSet{
        if let Some(r)=self.result{
            r
        }else{
            self.data.all()
        }
    }
    pub fn search(mut self,condition:&Search)->Self{
        if let Some(ref r)=self.result{
            if r.len()>0{
                if let Some(sr)=self.data.search(condition).result{
                    self.reduce(sr);
                }
            }
        }else{
            self=self.data.search(condition);
        }
        self
    }
    pub fn reduce_default(mut self)->Self{
        if let Some(ref r)=self.result{
            if r.len()>0{
                if let Some(sr)=self.data.search_term(&ConditionTerm::In(chrono::Local::now().timestamp())).result{
                    self.reduce(sr);
                }
                if let Some(sr)=self.data.search_activity(&Activity::Active).result{
                    self.reduce(sr);
                }
            }
        }else{
            self=self.data.search_term(&ConditionTerm::In(chrono::Local::now().timestamp()));
            if let Some(sr)=self.data.search_activity(&Activity::Active).result{
                self.reduce(sr);
            }
        }
        self
    }
    pub fn union(mut self,from:SearchResult)->Self{
        if let Some(ref r)=self.result{
            if let Some(fr)=from.result{
                self.result=Some(r.union(&fr).map(|&x|x).collect());
            }
        }else{
            if let Some(fr)=from.result{
                self.result=Some(fr);
            }
        }
        self
    }
    pub fn get_sorted(&self,o:&Order)->Vec<u32>{
        let mut ret=Vec::new();
        if let Some(r)=&self.result{
            match o{
                Order::Serial=>{
                    for (_,row,_) in self.data.serial_index().triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::Row=>{
                    ret=r.iter().map(|&x|x).collect::<Vec<u32>>();
                }
                ,Order::TermBegin=>{
                    for (_,row,_) in self.data.term_begin.triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::TermEnd=>{
                    for (_,row,_) in self.data.term_end.triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::LastUpdated=>{
                    for (_,row,_) in self.data.last_updated.triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::Field(field_name)=>{
                    if let Some(field)=self.data.field(field_name){
                        for (_,row,_) in field.index().triee().iter(){
                            if r.contains(&row){
                                ret.push(row);
                            }
                        }
                    }
                }
            }
        }
        ret
    }
    fn reduce(&mut self,newset:RowSet){
        if let Some(r)=&self.result{
            self.result=Some(newset.intersection(&r).map(|&x|x).collect());
        }else{
            self.result=Some(newset);
        }
    }
}