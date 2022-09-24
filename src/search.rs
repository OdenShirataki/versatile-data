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

pub enum ConditionNumber{
    Min(isize)
    ,Max(isize)
    ,Range(RangeInclusive<isize>)
    ,In(Vec<isize>)
}

pub enum ConditionFloat{
    Min(f64)
    ,Max(f64)
    ,Range(RangeInclusive<f64>)
}

pub enum Search{
    Activity(ConditionActivity)
    ,Term(ConditionTerm)
    ,Row(ConditionNumber)
    ,Priority(ConditionFloat)
    ,Uuid(u128)
    ,LastUpdated(ConditionNumber)
    ,Field(String,ConditionField)
}

pub enum Order<'a>{
    Serial
    ,Row
    ,Priority
    ,TermBegin
    ,TermEnd
    ,LastUpdated
    ,Field(&'a str)
}

#[derive(Clone)]
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
    pub fn search(mut self,condition:&Search)->Self{
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
    pub fn union(mut self,from:&Reducer)->Self{
        self.result=self.result.union(&from.result).map(|&x|x).collect();
        self
    }
    pub fn get_sorted(&self,o:&Order)->Vec<u32>{
        let mut r=Vec::new();
        match o{
            Order::Serial=>{
                for (_,row,_) in self.data.serial_index().triee().iter(){
                    if self.result.contains(&row){
                        r.push(row);
                    }
                }
            }
            ,Order::Row=>{
                r=self.result.iter().map(|&x|x).collect::<Vec<u32>>();
            }
            ,Order::Priority=>{
                for (_,row,_) in self.data.priority.triee().iter(){
                    if self.result.contains(&row){
                        r.push(row);
                    }
                }
            }
            ,Order::TermBegin=>{
                for (_,row,_) in self.data.term_begin.triee().iter(){
                    if self.result.contains(&row){
                        r.push(row);
                    }
                }
            }
            ,Order::TermEnd=>{
                for (_,row,_) in self.data.term_end.triee().iter(){
                    if self.result.contains(&row){
                        r.push(row);
                    }
                }
            }
            ,Order::LastUpdated=>{
                for (_,row,_) in self.data.last_updated.triee().iter(){
                    if self.result.contains(&row){
                        r.push(row);
                    }
                }
            }
            ,Order::Field(field_name)=>{
                if let Some(field)=self.data.field(field_name){
                    for (_,row,_) in field.index().triee().iter(){
                        if self.result.contains(&row){
                            r.push(row);
                        }
                    }
                }
            }
        }
        r
    }
    fn reduce(&mut self,newset:RowSet){
        self.result=newset.intersection(&self.result).map(|&x|x).collect();
    }
}