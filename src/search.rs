use std::thread;
use std::sync::mpsc::Sender;
use std::cmp::Ordering;
use idx_sized::RowSet;

use super::{
    Data
    ,Activity
};

mod enums;
pub use enums::*;

pub struct Search<'a>{
    data:&'a Data
    ,conditions:Vec<Condition>
}
impl<'a> Search<'a>{
    pub fn new(data:&'a Data)->Search{
        Search{
            data
            ,conditions:Vec::new()
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

    pub fn search_exec_cond(data:&Data,condition:&Condition,tx:Sender<RowSet>){
        match condition{
            Condition::Activity(condition)=>{
                Self::search_exec_activity(data,condition,tx)
            }
            ,Condition::Term(condition)=>{
                Self::search_exec_term(data,condition,tx)
            }
            ,Condition::Field(field_name,condition)=>{
                Self::search_exec_field(data,field_name,condition,tx)
            }
            ,Condition::Row(condition)=>{
                Self::search_exec_row(data,condition,tx)
            }
            ,Condition::LastUpdated(condition)=>{
                Self::search_exec_last_updated(data,condition,tx)
            }
            ,Condition::Uuid(uuid)=>{
                Self::search_exec_uuid(data,uuid,tx)
            }
            ,Condition::Narrow(conditions)=>{
                let mut new_search=Search::new(data);
                for c in conditions{
                    new_search=new_search.search(c.clone());
                }
                tx.send(new_search.result()).unwrap();
            }
            ,Condition::Broad(conditions)=>{
                let (tx_inner, rx) = std::sync::mpsc::channel();
                for c in conditions{
                    let tx_inner=tx_inner.clone();
                    Self::search_exec_cond(data,c,tx_inner);
                }
                drop(tx_inner);
                std::thread::spawn(move||{
                    let mut tmp=RowSet::default();
                    for ref mut rs in rx{
                        tmp.append(rs);
                    }
                    tx.send(tmp).unwrap();
                });
            }
        };
    }
    fn search_exec(&mut self)->RowSet{
        let mut rows=RowSet::default();
        if self.conditions.len()>0{
            let (tx, rx) = std::sync::mpsc::channel();
            for c in &self.conditions{
                let tx=tx.clone();
                Self::search_exec_cond(self.data, c, tx);
            }
            drop(tx);
            let mut fst=true;
            for rs in rx{
                if fst{
                    rows=rs;
                    fst=false;
                }else{
                    rows=rows.intersection(&rs).map(|&x|x).collect()
                }
            }
        }else{
            for row in self.data.serial.read().unwrap().index().triee().iter(){
                rows.insert(row.row());
            }
        }
        rows
    }
    pub fn result(mut self)->RowSet{
        self.search_exec()
    }
    pub fn result_with_sort(&mut self,o:&Order)->Vec<u32>{
        let rows=self.search_exec();
        let mut ret=Vec::new();
        match o{
            Order::Serial=>{
                for row in self.data.serial.read().unwrap().index().triee().iter(){
                    let row=row.row();
                    if rows.contains(&row){
                        ret.push(row);
                    }
                }
            }
            ,Order::Row=>{
                ret=rows.iter().map(|&x|x).collect::<Vec<u32>>();
            }
            ,Order::TermBegin=>{
                for row in self.data.term_begin.read().unwrap().triee().iter(){
                    let row=row.row();
                    if rows.contains(&row){
                        ret.push(row);
                    }
                }
            }
            ,Order::TermEnd=>{
                for row in self.data.term_end.read().unwrap().triee().iter(){
                    let row=row.row();
                    if rows.contains(&row){
                        ret.push(row);
                    }
                }
            }
            ,Order::LastUpdated=>{
                for row in self.data.last_updated.read().unwrap().triee().iter(){
                    let row=row.row();
                    if rows.contains(&row){
                        ret.push(row);
                    }
                }
            }
            ,Order::Field(field_name)=>{
                if let Some(field)=self.data.field(field_name){
                    for row in field.read().unwrap().index().triee().iter(){
                        let row=row.row();
                        if rows.contains(&row){
                            ret.push(row);
                        }
                    }
                }
            }
        }
        ret
    }
    fn search_exec_activity(data:&Data,condition:&Activity,tx:Sender<RowSet>){
        let activity=*condition as u8;
        let index=data.activity.clone();
        thread::spawn(move||{
            tx.send(index.read().unwrap().select_by_value_from_to(&activity,&activity)).unwrap();
        });
    }
    fn search_exec_term_in(data:&Data,base:i64,tx:Sender<RowSet>){
        let term_begin=data.term_begin.clone();
        let term_end=data.term_end.clone();
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
    fn search_exec_term(data:&Data,condition:&Term,tx:Sender<RowSet>){
        match condition{
            Term::In(base)=>{
                Self::search_exec_term_in(data,*base,tx);
            }
            ,Term::Future(base)=>{
                let index=data.term_begin.clone();
                let base=base.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_from(&base)).unwrap();
                });
            }
            ,Term::Past(base)=>{
                let index=data.term_end.clone();
                let base=base.clone();
                std::thread::spawn(move||{
                    tx.send(index.read().unwrap().select_by_value_from_to(&1,&base)).unwrap();
                });
            }
        }
    }
    fn search_exec_row(data:&Data,condition:&Number,tx:Sender<RowSet>){
        let serial=data.serial.clone();
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
    fn search_exec_field(data:&Data,field_name:&str,condition:&Field,tx:Sender<RowSet>){
        if let Some(field)=data.field(field_name){
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
    fn search_exec_last_updated(data:&Data,condition:&Number,tx:Sender<RowSet>){
        let index=data.last_updated.clone();
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
    pub fn search_exec_uuid(data:&Data,uuid:&u128,tx:Sender<RowSet>){
        let index=data.uuid.clone();
        let uuid=uuid.clone();
        std::thread::spawn(move||{
            tx.send(if let Ok(index)=index.read(){
                index.select_by_value(&uuid)
            }else{
                RowSet::default()
            }).unwrap();
        });
    }
}