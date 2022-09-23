use std::ops::RangeInclusive;
use idx_sized::RowSet;

use crate::ConditionField;
use crate::Data;

#[derive(Clone,Copy,PartialEq)]
pub enum ConditionActivity{
    Active
    ,Inactive
}

#[derive(Clone,Copy,PartialEq)]
pub enum ConditionTerm{
    In(i64)
    ,Past(i64)
    ,Future(i64)
}

pub enum ConditionRow{
    Min(u32)
    ,Max(u32)
    ,Range(RangeInclusive<u32>)
    ,In(Vec<u32>)
}

pub enum SearchCondition{
    Activity(ConditionActivity)
    ,Term(ConditionTerm)
    ,Field(String,ConditionField)
    ,Row(ConditionRow)
}

pub struct Reducer<'a>{
    data:&'a Data
    ,result:RowSet
}
impl<'a> Reducer<'a>{
    pub fn new(data:&'a Data,result:RowSet)->Reducer{
        Reducer{
            data
            ,result
        }
    }
    pub fn get(self)->RowSet{
        self.result
    }
    pub fn search(mut self,condition:&SearchCondition)->Self{
        if self.result.len()>0{
            let search=self.data.search(condition);
            self.reduce(search.result);
        }
        self
    }
    pub fn reduce_default(mut self)->Self{
        if self.result.len()>0{
            self.reduce(
                self.data.search_term(&ConditionTerm::In(chrono::Local::now().timestamp())).result
            );
            self.reduce(
                self.data.search_activity(&ConditionActivity::Active).result
            );
        }
        self
    }
    fn reduce(&mut self,newset:RowSet){
        self.result=newset.intersection(&self.result).map(|&x|x).collect();
    }
}