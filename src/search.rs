use std::cmp::Ordering;
use std::ops::RangeInclusive;
use idx_sized::RowSet;

use crate::{
    Data
    ,Activity
};

pub enum Term{
    In(i64)
    ,Past(i64)
    ,Future(i64)
}

pub enum Number{
    Min(isize)
    ,Max(isize)
    ,Range(RangeInclusive<isize>)
    ,In(Vec<isize>)
}

pub enum Field{
    Match(Vec<u8>)
    ,Range(Vec<u8>,Vec<u8>)
    ,Min(Vec<u8>)
    ,Max(Vec<u8>)
    ,Forward(String)
    ,Partial(String)
    ,Backward(String)
}

pub enum Condition{
    Activity(Activity)
    ,Term(Term)
    ,Row(Number)
    ,Uuid(u128)
    ,LastUpdated(Number)
    ,Field(String,Field)
}

pub enum Order<'a>{
    Serial
    ,Row
    ,TermBegin
    ,TermEnd
    ,LastUpdated
    ,Field(&'a str)
}

pub struct Search<'a>{
    data:&'a Data
    ,conditions:Vec<Condition>
    ,result:Option<RowSet>
}
impl<'a> Search<'a>{
    pub fn new(data:&'a Data)->Search{
        Search{
            data
            ,conditions:Vec::new()
            ,result:None
        }
    }
    pub fn search_default(mut self)->Self{
        self.conditions.push(Condition::Term(Term::In(chrono::Local::now().timestamp())));
        self.conditions.push(Condition::Activity(Activity::Active));
        self
    }
    pub fn search(mut self,condition:Condition)->Self{
        self.conditions.push(condition);
        self
    }
    fn search_exec(&mut self){
        let mut r=Vec::new();
        for c in &self.conditions{
            r.push(match c{
                Condition::Activity(condition)=>{
                    self.search_activity(&condition)
                }
                ,Condition::Term(condition)=>{
                    self.search_term(&condition)
                }
                ,Condition::Field(field_name,condition)=>{
                    self.search_field(&field_name,&condition)
                }
                ,Condition::Row(condition)=>{
                    self.search_row(&condition)
                }
                ,Condition::LastUpdated(condition)=>{
                    self.search_last_updated(&condition)
                }
                ,Condition::Uuid(uuid)=>{
                    self.search_uuid(&uuid)
                }
            });
        }
        for r in r{
            self.reduce(r);
        }
    }
    pub fn result(mut self)->RowSet{
        self.search_exec();
        if let Some(r)=self.result{
            r
        }else{
            self.data.all()
        }
    }
    pub fn result_with_sort(&mut self,o:&Order)->Vec<u32>{
        self.search_exec();
        let mut ret=Vec::new();
        if let Some(r)=&self.result{
            match o{
                Order::Serial=>{
                    for (_,row,_) in self.data.serial.read().unwrap().index().triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                    
                }
                ,Order::Row=>{
                    ret=r.iter().map(|&x|x).collect::<Vec<u32>>();
                }
                ,Order::TermBegin=>{
                    for (_,row,_) in self.data.term_begin.read().unwrap().triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::TermEnd=>{
                    for (_,row,_) in self.data.term_end.read().unwrap().triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::LastUpdated=>{
                    for (_,row,_) in self.data.last_updated.read().unwrap().triee().iter(){
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::Field(field_name)=>{
                    if let Some(field)=self.data.field(field_name){
                        for (_,row,_) in field.read().unwrap().index().triee().iter(){
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
    fn search_activity(&self,condition:&'a Activity)->RowSet{
        let activity=*condition as u8;
        self.data.activity.read().unwrap().select_by_value_from_to(&activity,&activity)
    }
    fn search_term_in(&self,base:i64)->RowSet{
        let mut result=RowSet::default();
        let tmp=self.data.term_begin.read().unwrap().select_by_value_to(&base);
        for row in tmp{
            let end=self.data.term_end.read().unwrap().value(row).unwrap_or(0);
            if end==0 || end>base {
                result.replace(row);
            }
        }
        result
    }
    fn search_term(&self,condition:&'a Term)->RowSet{
        match condition{
            Term::In(base)=>{
                self.search_term_in(*base)
            }
            ,Term::Future(base)=>{
                self.data.term_begin.read().unwrap().select_by_value_from(&base)
            }
            ,Term::Past(base)=>{
                self.data.term_end.read().unwrap().select_by_value_from_to(&1,&base)
            }
        }
    }
    fn search_row(&self,condition:&'a Number)->RowSet{
        let mut r=RowSet::default();
        match condition{
            Number::Min(row)=>{
                for (_,i,_) in self.data.serial.read().unwrap().index().triee().iter(){
                    if i as isize>=*row{
                        r.insert(i);
                    }
                }
                
            }
            ,Number::Max(row)=>{
                for (_,i,_) in self.data.serial.read().unwrap().index().triee().iter(){
                    if i as isize<=*row{
                        r.insert(i);
                    }
                }
            }
            ,Number::Range(range)=>{
                for i in range.clone(){
                    if let Some(_)=self.data.serial.read().unwrap().index().triee().node(i as u32){
                        r.insert(i as u32);
                    }
                }
            }
            ,Number::In(rows)=>{
                for i in rows{
                    if let Some(_)=self.data.serial.read().unwrap().index().triee().node(*i as u32){
                        r.insert(*i as u32);
                    }
                }
            }
        };
        r
    }
    fn search_field(&self,field_name:&'a str,condition:&'a Field)->RowSet{
        let mut r:RowSet=RowSet::default();
        if let Some(field)=self.data.field(field_name){
            match condition{
                Field::Match(v)=>{
                    let (ord,found_row)=field.read().unwrap().search_cb(v);
                    if ord==Ordering::Equal{
                        r.insert(found_row);
                        r.append(&mut field.read().unwrap().triee().sames(found_row).iter().map(|&x|x).collect());
                    }
                }
                ,Field::Min(min)=>{
                    let (_,min_found_row)=field.read().unwrap().search_cb(min);
                    for (_,row,_) in field.read().unwrap().triee().iter_by_row_from(min_found_row){
                        r.insert(row);
                        r.append(&mut field.read().unwrap().triee().sames(row).iter().map(|&x|x).collect());
                    }
                }
                ,Field::Max(max)=>{
                    let (_,max_found_row)=field.read().unwrap().search_cb(max);
                    for (_,row,_) in field.read().unwrap().triee().iter_by_row_to(max_found_row){
                        r.insert(row);
                        r.append(&mut field.read().unwrap().triee().sames(row).iter().map(|&x|x).collect());
                    }
                }
                ,Field::Range(min,max)=>{
                    let (_,min_found_row)=field.read().unwrap().search_cb(min);
                    let (_,max_found_row)=field.read().unwrap().search_cb(max);
                    for (_,row,_) in field.read().unwrap().triee().iter_by_row_from_to(min_found_row,max_found_row){
                        r.insert(row);
                        r.append(&mut field.read().unwrap().triee().sames(row).iter().map(|&x|x).collect());
                    }
                }
                ,Field::Forward(cont)=>{
                    let len=cont.len();
                    for (_,row,v) in field.read().unwrap().triee().iter(){
                        let data=v.value();
                        if len<=data.len(){
                            if let Some(str2)=field.read().unwrap().str(row){
                                if str2.starts_with(cont){
                                    r.insert(row);
                                }
                            }
                        }
                    }
                }
                ,Field::Partial(cont)=>{
                    let len=cont.len();
                    for (_,row,v) in field.read().unwrap().triee().iter(){
                        let data=v.value();
                        if len<=data.len(){
                            if let Some(str2)=field.read().unwrap().str(row){
                                if str2.contains(cont){
                                    r.insert(row);
                                }
                            }
                        }
                    }
                }
                ,Field::Backward(cont)=>{
                    let len=cont.len();
                    for (_,row,v) in field.read().unwrap().triee().iter(){
                        let data=v.value();
                        if len<=data.len(){
                            if let Some(str2)=field.read().unwrap().str(row){
                                if str2.ends_with(cont){
                                    r.insert(row);
                                }
                            }
                        }
                    }
                }
            }
        }
        r
    }
    fn search_last_updated(&self,condition:&'a Number)->RowSet{
        match condition{
            Number::Min(v)=>{
                self.data.last_updated.read().unwrap().select_by_value_from(&(*v as i64))
            }
            ,Number::Max(v)=>{
                self.data.last_updated.read().unwrap().select_by_value_to(&(*v as i64))
            }
            ,Number::Range(range)=>{
                self.data.last_updated.read().unwrap().select_by_value_from_to(
                    &(*range.start() as i64)
                    ,&(*range.end() as i64)
                )
            }
            ,Number::In(rows)=>{
                let mut r=RowSet::default();
                for i in rows{
                    for row in self.data.last_updated.read().unwrap().select_by_value(&(*i as i64)){
                        r.insert(row);
                    }
                }
                r
            }
        }
    }
    pub fn search_uuid(&self,uuid:&'a u128)->RowSet{
        if let Ok(index)=self.data.uuid.read(){
            index.select_by_value(uuid)
        }else{
            RowSet::default()
        }
    }
    pub fn union(mut self,from:Search)->Self{
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
    
}