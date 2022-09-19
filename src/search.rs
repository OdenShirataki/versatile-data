use idx_sized::IdSet;

use crate::SearchCondition;
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

#[derive(Clone)]
pub struct Reducer<'a>{
    data:&'a Data
    ,result:IdSet
}
impl<'a> Reducer<'a>{
    pub fn new(data:&'a Data,result:IdSet)->Reducer{
        Reducer{
            data
            ,result
        }
    }
    pub fn get(&self)->IdSet{
        self.result.clone()
    }
    pub fn search_activity(&'a mut self,condition: ConditionActivity)->&'a mut Reducer{
        if self.result.len()>0{
            let search=self.data.search_activity(condition);
            self.reduce(search.result);
        }
        self
    }
    pub fn search_field(&'a mut self,field_name:&str,condition: SearchCondition)->&'a mut Reducer{
        if self.result.len()>0{
            let search=self.data.search_field(field_name,condition);
            self.reduce(search.result);
        }
        self
    }
    pub fn search_term(&'a mut self,condition:ConditionTerm)->&'a mut Reducer{
        if self.result.len()>0{
            let search=self.data.search_term(condition);
            self.reduce(search.result);
        }
        self
    }
    pub fn reduce_default(&'a mut self)->&'a mut Reducer{
        if self.result.len()>0{
            self.reduce(
                self.data.search_term(ConditionTerm::In(chrono::Local::now().timestamp())).result
            );
            self.reduce(
                self.data.search_activity(ConditionActivity::Active).result
            );
        }
        self
    }
    fn reduce(&mut self,newset:IdSet){
        let mut ret=IdSet::default();
        if newset.len()<self.result.len(){
            for i in newset{
                if self.result.contains(&i){
                    ret.insert(i);
                }
            }
        }else{
            for i in &self.result{
                if newset.contains(&i){
                    ret.insert(*i);
                }
            }
        }
        self.result=ret;
    }
}