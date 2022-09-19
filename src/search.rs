use idx_sized::IdSet;

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

pub enum SearchCondition<'a>{
    Activity(ConditionActivity)
    ,Term(ConditionTerm)
    ,Field(&'a str,ConditionField<'a>)
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
    pub fn get(self)->IdSet{
        self.result
    }
    pub fn search(mut self,condition: SearchCondition)->Self{
        if self.result.len()>0{
            let search=self.data.search(condition);
            self.reduce(search.result);
        }
        self
    }
    pub fn reduce_default(mut self)->Self{
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
        for i in newset.intersection(&self.result){
            ret.insert(*i);
        }
        self.result=ret;
    }
}