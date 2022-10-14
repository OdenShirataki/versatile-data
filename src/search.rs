use std::thread;
use std::sync::mpsc::Sender;
use std::cmp::Ordering;
use std::ops::RangeInclusive;
use idx_sized::RowSet;

use crate::{
    Data
    ,Activity
};

#[derive(Clone)]
pub enum Term{
    In(i64)
    ,Past(i64)
    ,Future(i64)
}

#[derive(Clone)]
pub enum Number{
    Min(isize)
    ,Max(isize)
    ,Range(RangeInclusive<isize>)
    ,In(Vec<isize>)
}

#[derive(Clone)]
pub enum Field{
    Match(Vec<u8>)
    ,Range(Vec<u8>,Vec<u8>)
    ,Min(Vec<u8>)
    ,Max(Vec<u8>)
    ,Forward(String)
    ,Partial(String)
    ,Backward(String)
}

#[derive(Clone)]
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
    pub fn search_field(self,field_name:impl Into<String>,condition:Field)->Self{
        self.search(Condition::Field(field_name.into(),condition))
    }
    pub fn search_term(self,condition:Term)->Self{
        self.search(Condition::Term(condition))
    }
    pub fn search_activity(self,condition:Activity)->Self{
        self.search(Condition::Activity(condition))
    }
    pub fn search_row(self,condition:Number)->Self{
        self.search(Condition::Row(condition))
    }

    pub fn search(mut self,condition:Condition)->Self{
        self.conditions.push(condition);
        self
    }

    fn search_exec(&mut self){
        let (tx, rx) = std::sync::mpsc::channel();
        for c in &self.conditions{
            let tx=tx.clone();
            match c{
                Condition::Activity(condition)=>{
                    self.search_exec_activity(condition,tx)
                }
                ,Condition::Term(condition)=>{
                    self.search_exec_term(condition,tx)
                }
                ,Condition::Field(field_name,condition)=>{
                    self.search_exec_field(field_name,condition,tx)
                }
                ,Condition::Row(condition)=>{
                    self.search_exec_row(condition,tx)
                }
                ,Condition::LastUpdated(condition)=>{
                    self.search_exec_last_updated(condition,tx)
                }
                ,Condition::Uuid(uuid)=>{
                    self.search_exec_uuid(uuid,tx)
                }
            };
        }
        drop(tx);
        for rs in rx{
            self.reduce(rs);
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
                    for row in self.data.serial.read().unwrap().index().triee().iter(){
                        let row=row.row();
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::Row=>{
                    ret=r.iter().map(|&x|x).collect::<Vec<u32>>();
                }
                ,Order::TermBegin=>{
                    for row in self.data.term_begin.read().unwrap().triee().iter(){
                        let row=row.row();
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::TermEnd=>{
                    for row in self.data.term_end.read().unwrap().triee().iter(){
                        let row=row.row();
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::LastUpdated=>{
                    for row in self.data.last_updated.read().unwrap().triee().iter(){
                        let row=row.row();
                        if r.contains(&row){
                            ret.push(row);
                        }
                    }
                }
                ,Order::Field(field_name)=>{
                    if let Some(field)=self.data.field(field_name){
                        for row in field.read().unwrap().index().triee().iter(){
                            let row=row.row();
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
    fn search_exec_activity(&self,condition:&Activity,tx:Sender<RowSet>){
        let activity=*condition as u8;
        let index=self.data.activity.clone();
        thread::spawn(move||{
            tx.send(index.read().unwrap().select_by_value_from_to(&activity,&activity)).unwrap();
        });
    }
    fn search_exec_term_in(&self,base:i64,tx:Sender<RowSet>){
        let term_begin=self.data.term_begin.clone();
        let term_end=self.data.term_end.clone();

        thread::spawn(move||{
            let mut result=RowSet::default();
            let tmp=term_begin.read().unwrap().select_by_value_to(&base);
            for row in tmp{
                let end=term_end.read().unwrap().value(row).unwrap_or(0);
                if end==0 || end>base {
                    result.replace(row);
                }
            }
            tx.send(result).unwrap();
        });
    }
    fn search_exec_term(&self,condition:&Term,tx:Sender<RowSet>){
        match condition{
            Term::In(base)=>{
                self.search_exec_term_in(*base,tx);
            }
            ,Term::Future(base)=>{
                let index=self.data.term_begin.clone();
                let base=base.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_from(&base)).unwrap();
                });
            }
            ,Term::Past(base)=>{
                let index=self.data.term_end.clone();
                let base=base.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_from_to(&1,&base)).unwrap();
                });
            }
        }
    }
    fn search_exec_row(&self,condition:&Number,tx:Sender<RowSet>){
        let serial=self.data.serial.clone();
        let mut r=RowSet::default();
        match condition{
            Number::Min(row)=>{
                let row=row.clone();
                thread::spawn(move||{
                    for i in serial.read().unwrap().index().triee().iter(){
                        let i=i.row();
                        if i as isize>=row{
                            r.insert(i);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
            ,Number::Max(row)=>{
                let row=row.clone();
                std::thread::spawn(move||{
                    for i in serial.read().unwrap().index().triee().iter(){
                        let i=i.row();
                        if i as isize<=row{
                            r.insert(i);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
            ,Number::Range(range)=>{
                let range=range.clone();
                std::thread::spawn(move||{
                    for i in range{
                        if let Some(_)=serial.read().unwrap().index().triee().node(i as u32){
                            r.insert(i as u32);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
            ,Number::In(rows)=>{
                let rows=rows.clone();
                std::thread::spawn(move||{
                    for i in rows{
                        if let Some(_)=serial.read().unwrap().index().triee().node(i as u32){
                            r.insert(i as u32);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
        }
    }
    fn search_exec_field(&self,field_name:&'a str,condition:&Field,tx:Sender<RowSet>){
        if let Some(field)=self.data.field(field_name){
            let field=field.clone();
            let mut r:RowSet=RowSet::default();
            match condition{
                Field::Match(v)=>{
                    let v=v.clone();
                    std::thread::spawn(move||{
                        let (ord,found_row)=field.read().unwrap().search_cb(&v);
                        if ord==Ordering::Equal{
                            r.insert(found_row);
                            r.append(&mut field.read().unwrap().triee().sames(found_row).iter().map(|&x|x).collect());
                        }
                        tx.send(r).unwrap();
                    });
                }
                ,Field::Min(min)=>{
                    let min=min.clone();
                    std::thread::spawn(move||{
                        let (_,min_found_row)=field.read().unwrap().search_cb(&min);
                        for row in field.read().unwrap().triee().iter_by_row_from(min_found_row){
                            let row=row.row();
                            r.insert(row);
                            r.append(&mut field.read().unwrap().triee().sames(row).iter().map(|&x|x).collect());
                        }
                        tx.send(r).unwrap();
                    });
                }
                ,Field::Max(max)=>{
                    let max=max.clone();
                    std::thread::spawn(move||{
                        let (_,max_found_row)=field.read().unwrap().search_cb(&max);
                        for row in field.read().unwrap().triee().iter_by_row_to(max_found_row){
                            let row=row.row();
                            r.insert(row);
                            r.append(&mut field.read().unwrap().triee().sames(row).iter().map(|&x|x).collect());
                        }
                        tx.send(r).unwrap();
                    });
                }
                ,Field::Range(min,max)=>{
                    let min=min.clone();
                    let max=max.clone();
                    std::thread::spawn(move||{
                        let (_,min_found_row)=field.read().unwrap().search_cb(&min);
                        let (_,max_found_row)=field.read().unwrap().search_cb(&max);
                        for row in field.read().unwrap().triee().iter_by_row_from_to(min_found_row,max_found_row){
                            let row=row.row();
                            r.insert(row);
                            r.append(&mut field.read().unwrap().triee().sames(row).iter().map(|&x|x).collect());
                        }
                        tx.send(r).unwrap();
                    });
                }
                ,Field::Forward(cont)=>{
                    let cont=cont.clone();
                    std::thread::spawn(move||{
                        let len=cont.len();
                        for row in field.read().unwrap().triee().iter(){
                            let data=row.value();
                            let row=row.row();
                            if len<=data.len(){
                                if let Some(str2)=field.read().unwrap().str(row){
                                    if str2.starts_with(&cont){
                                        r.insert(row);
                                    }
                                }
                            }
                        }
                        tx.send(r).unwrap();
                    });
                }
                ,Field::Partial(cont)=>{
                    let cont=cont.clone();
                    std::thread::spawn(move||{
                        let len=cont.len();
                        for row in field.read().unwrap().triee().iter(){
                            let data=row.value();
                            let row=row.row();
                            if len<=data.len(){
                                if let Some(str2)=field.read().unwrap().str(row){
                                    if str2.contains(&cont){
                                        r.insert(row);
                                    }
                                }
                            }
                        }
                        tx.send(r).unwrap();
                    });
                }
                ,Field::Backward(cont)=>{
                    let cont=cont.clone();
                    std::thread::spawn(move||{
                        let len=cont.len();
                        for row in field.read().unwrap().triee().iter(){
                            let data=row.value();
                            let row=row.row();
                            if len<=data.len(){
                                if let Some(str2)=field.read().unwrap().str(row){
                                    if str2.ends_with(&cont){
                                        r.insert(row);
                                    }
                                }
                            }
                        }
                        tx.send(r).unwrap();
                    });
                }
            }
        }
    }
    fn search_exec_last_updated(&self,condition:&'a Number,tx:Sender<RowSet>){
        let index=self.data.last_updated.clone();
        match condition{
            Number::Min(v)=>{
                let v=v.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_from(&(v as i64))).unwrap();
                });
            }
            ,Number::Max(v)=>{
                let v=v.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_to(&(v as i64))).unwrap();
                });
            }
            ,Number::Range(range)=>{
                let range=range.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_from_to(
                        &(*range.start() as i64)
                        ,&(*range.end() as i64)
                    )).unwrap();
                });
            }
            ,Number::In(rows)=>{
                let rows=rows.clone();
                std::thread::spawn(move||{
                    let mut r=RowSet::default();
                    for i in rows{
                        for row in index.read().unwrap().select_by_value(&(i as i64)){
                            r.insert(row);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
        }
        
    }
    pub fn search_exec_uuid(&self,uuid:&'a u128,tx:Sender<RowSet>){
        let index=self.data.uuid.clone();
        let uuid=uuid.clone();
        std::thread::spawn(move||{
            tx.send(if let Ok(index)=index.read(){
                index.select_by_value(&uuid)
            }else{
                RowSet::default()
            }).unwrap();
        });
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