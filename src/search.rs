use idx_sized::IdSet;

use crate::SearchCondition;
use crate::Data;

#[derive(Clone,Copy,PartialEq)]
pub enum ConditionActivity{
    Active
    ,Inactive
}

#[derive(Clone,Copy,PartialEq)]
pub enum TermScope{
    In(i64)
    ,Past(i64)
    ,Future(i64)
    ,All
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
            ,result:result
        }
    }
    pub fn get(&self)->IdSet{
        self.result.clone()
    }
    pub fn search_activity(&'a mut self,condition: ConditionActivity)->&'a Reducer{
        let activity=if condition==ConditionActivity::Active{ 1 }else{ 0 };
        let newset=self.data.activity.select_by_value_from_to(&activity,&activity);
        self.reduce(newset);
        self
    }
    pub fn search_field(&'a mut self,field_name:&str,condition: SearchCondition)->&'a Reducer{
        let newset=if let Some(field)=self.data.field(field_name){
            field.search(condition)
        }else{
            IdSet::default()
        };
        self.reduce(newset);
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