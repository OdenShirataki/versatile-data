use std::{
    sync::mpsc::{channel, SendError, Sender},
    thread::spawn,
    time::{SystemTime, UNIX_EPOCH},
};

use super::{Activity, Data, RowSet};

mod enums;
pub use enums::*;

pub struct Search<'a> {
    data: &'a Data,
    conditions: Vec<Condition>,
}
impl<'a> Search<'a> {
    pub fn new(data: &'a Data) -> Self {
        Search {
            data,
            conditions: Vec::new(),
        }
    }
    pub fn search_default(mut self) -> Result<Self, std::time::SystemTimeError> {
        self.conditions.push(Condition::Term(Term::In(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        )));
        self.conditions.push(Condition::Activity(Activity::Active));
        Ok(self)
    }
    pub fn search_field(self, field_name: impl Into<String>, condition: Field) -> Self {
        self.search(Condition::Field(field_name.into(), condition))
    }
    pub fn search_term(self, condition: Term) -> Self {
        self.search(Condition::Term(condition))
    }
    pub fn search_activity(self, condition: Activity) -> Self {
        self.search(Condition::Activity(condition))
    }
    pub fn search_row(self, condition: Number) -> Self {
        self.search(Condition::Row(condition))
    }

    pub fn search(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }

    pub fn search_exec_cond(
        data: &Data,
        condition: &Condition,
        tx: Sender<RowSet>,
    ) -> Result<(), SendError<RowSet>> {
        match condition {
            Condition::Activity(condition) => Self::search_exec_activity(data, condition, tx),
            Condition::Term(condition) => Self::search_exec_term(data, condition, tx),
            Condition::Field(field_name, condition) => {
                Self::search_exec_field(data, field_name, condition, tx)
            }
            Condition::Row(condition) => Self::search_exec_row(data, condition, tx),
            Condition::LastUpdated(condition) => {
                Self::search_exec_last_updated(data, condition, tx)
            }
            Condition::Uuid(uuid) => Self::search_exec_uuid(data, uuid, tx),
            Condition::Narrow(conditions) => {
                let mut new_search = Search::new(data);
                for c in conditions {
                    new_search = new_search.search(c.clone());
                }
                tx.send(new_search.result()?)?;
            }
            Condition::Wide(conditions) => {
                let (tx_inner, rx) = channel();
                for c in conditions {
                    let tx_inner = tx_inner.clone();
                    Self::search_exec_cond(data, c, tx_inner)?;
                }
                drop(tx_inner);
                spawn(move || {
                    let mut tmp = RowSet::default();
                    for ref mut rs in rx {
                        tmp.append(rs);
                    }
                    tx.send(tmp).unwrap();
                });
            }
        };
        Ok(())
    }
    fn search_exec(&mut self) -> Result<RowSet, SendError<RowSet>> {
        let mut rows = RowSet::default();
        if self.conditions.len() > 0 {
            let (tx, rx) = channel();
            for c in self.conditions.iter() {
                let tx = tx.clone();
                Self::search_exec_cond(self.data, c, tx)?;
            }
            drop(tx);
            let mut fst = true;
            for rs in rx {
                if fst {
                    rows = rs;
                    fst = false;
                } else {
                    rows = rows.intersection(&rs).map(|&x| x).collect()
                }
            }
        } else {
            for row in self.data.serial.read().unwrap().index().triee().iter() {
                rows.insert(row.row());
            }
        }
        Ok(rows)
    }
    pub fn result(mut self) -> Result<RowSet, SendError<RowSet>> {
        self.search_exec()
    }
    pub fn result_with_sort(&mut self, orders: Vec<Order>) -> Result<Vec<u32>, SendError<RowSet>> {
        let rows = self.search_exec()?;
        Ok(self.data.sort(rows, &orders))
    }
    fn search_exec_activity(data: &Data, condition: &Activity, tx: Sender<RowSet>) {
        let activity = *condition as u8;
        let index = data.activity.clone();
        spawn(move || {
            tx.send(
                index
                    .read()
                    .unwrap()
                    .triee()
                    .iter_by(|v| v.cmp(&activity))
                    .map(|v| v.row())
                    .collect(),
            )
            .unwrap();
        });
    }
    fn search_exec_term_in(data: &Data, base: u64, tx: Sender<RowSet>) {
        let term_begin = data.term_begin.clone();
        let term_end = data.term_end.clone();
        spawn(move || {
            let mut result = RowSet::default();
            for node in term_begin.read().unwrap().triee().iter_to(|v| v.cmp(&base)) {
                let row = node.row();
                let end = *term_end.read().unwrap().value(row).unwrap_or(&0);
                if end == 0 || end > base {
                    result.replace(row);
                }
            }
            tx.send(result).unwrap();
        });
    }
    fn search_exec_term(data: &Data, condition: &Term, tx: Sender<RowSet>) {
        match condition {
            Term::In(base) => {
                Self::search_exec_term_in(data, *base, tx);
            }
            Term::Future(base) => {
                let index = data.term_begin.clone();
                let base = base.clone();
                spawn(move || {
                    tx.send(
                        index
                            .read()
                            .unwrap()
                            .triee()
                            .iter_from(|v| v.cmp(&base))
                            .map(|v| v.row())
                            .collect(),
                    )
                    .unwrap();
                });
            }
            Term::Past(base) => {
                let index = data.term_end.clone();
                let base = base.clone();
                spawn(move || {
                    tx.send(
                        index
                            .read()
                            .unwrap()
                            .triee()
                            .iter_range(|v| v.cmp(&1), |v| v.cmp(&base))
                            .map(|v| v.row())
                            .collect(),
                    )
                    .unwrap();
                });
            }
        }
    }
    fn search_exec_row(data: &Data, condition: &Number, tx: Sender<RowSet>) {
        let serial = data.serial.clone();
        let mut r = RowSet::default();
        match condition {
            Number::Min(row) => {
                let row = *row;
                spawn(move || {
                    for i in serial.read().unwrap().index().triee().iter() {
                        let i = i.row();
                        if i as isize >= row {
                            r.insert(i);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
            Number::Max(row) => {
                let row = *row;
                spawn(move || {
                    for i in serial.read().unwrap().index().triee().iter() {
                        let i = i.row();
                        if i as isize <= row {
                            r.insert(i);
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
            Number::Range(range) => {
                let range = range.clone();
                spawn(move || {
                    for i in range {
                        if i > 0 {
                            if serial.read().unwrap().index().exists(i as u32) {
                                r.insert(i as u32);
                            }
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
            Number::In(rows) => {
                let rows = rows.clone();
                spawn(move || {
                    for i in rows {
                        if i > 0 {
                            if serial.read().unwrap().index().exists(i as u32) {
                                r.insert(i as u32);
                            }
                        }
                    }
                    tx.send(r).unwrap();
                });
            }
        }
    }
    fn search_exec_field(data: &Data, field_name: &str, condition: &Field, tx: Sender<RowSet>) {
        if let Some(field) = data.field(field_name) {
            let field = field.clone();
            let mut r: RowSet = RowSet::default();
            match condition {
                Field::Match(v) => {
                    let v = v.clone();
                    spawn(move || {
                        let field = field.read().unwrap();
                        tx.send(
                            field
                                .index
                                .triee()
                                .iter_by(|data| field.search_inner(data, &v))
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Min(min) => {
                    let min = min.clone();
                    spawn(move || {
                        let field = field.read().unwrap();

                        tx.send(
                            field
                                .index
                                .triee()
                                .iter_from(|data| field.search_inner(data, &min))
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Max(max) => {
                    let max = max.clone();
                    spawn(move || {
                        let field = field.read().unwrap();
                        tx.send(
                            field
                                .index
                                .triee()
                                .iter_to(|data| field.search_inner(data, &max))
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Range(min, max) => {
                    let min = min.clone();
                    let max = max.clone();
                    spawn(move || {
                        let field = field.read().unwrap();

                        tx.send(
                            field
                                .index
                                .triee()
                                .iter_range(
                                    |data| field.search_inner(data, &min),
                                    |data| field.search_inner(data, &max),
                                )
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Forward(cont) => {
                    let cont = cont.clone();
                    spawn(move || {
                        let len = cont.len();
                        for row in field.read().unwrap().index.triee().iter() {
                            let data = row.value();
                            let row = row.row();
                            if len as u64 <= data.data_address().len() {
                                if let Some(bytes2) = field.read().unwrap().get(row) {
                                    if bytes2.starts_with(cont.as_bytes()) {
                                        r.insert(row);
                                    }
                                }
                            }
                        }
                        tx.send(r).unwrap();
                    });
                }
                Field::Partial(cont) => {
                    let cont = cont.clone();
                    spawn(move || {
                        let len = cont.len();
                        for row in field.read().unwrap().index.triee().iter() {
                            let data = row.value();
                            let row = row.row();
                            if len as u64 <= data.data_address().len() {
                                if let Some(bytes2) = field.read().unwrap().get(row) {
                                    let bytes = cont.as_bytes();
                                    if let Some(_) =
                                        bytes2.windows(len).position(|window| window == bytes)
                                    {
                                        r.insert(row);
                                    }
                                }
                            }
                        }
                        tx.send(r).unwrap();
                    });
                }
                Field::Backward(cont) => {
                    let cont = cont.clone();
                    spawn(move || {
                        let len = cont.len();
                        for row in field.read().unwrap().index.triee().iter() {
                            let data = row.value();
                            let row = row.row();
                            if len as u64 <= data.data_address().len() {
                                if let Some(bytes2) = field.read().unwrap().get(row) {
                                    if bytes2.ends_with(cont.as_bytes()) {
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
    fn search_exec_last_updated(data: &Data, condition: &Number, tx: Sender<RowSet>) {
        let index = data.last_updated.clone();
        match condition {
            Number::Min(min) => {
                let min = min.clone() as u64;
                spawn(move || {
                    tx.send(
                        index
                            .read()
                            .unwrap()
                            .triee()
                            .iter_from(|v| v.cmp(&min))
                            .map(|v| v.row())
                            .collect(),
                    )
                    .unwrap();
                });
            }
            Number::Max(max) => {
                let max = max.clone() as u64;
                spawn(move || {
                    tx.send(
                        index
                            .read()
                            .unwrap()
                            .triee()
                            .iter_to(|v| v.cmp(&max))
                            .map(|v| v.row())
                            .collect(),
                    )
                    .unwrap();
                });
            }
            Number::Range(range) => {
                let range = range.clone();
                spawn(move || {
                    tx.send(
                        index
                            .read()
                            .unwrap()
                            .triee()
                            .iter_range(
                                |v| v.cmp(&(*range.start() as u64)),
                                |v| v.cmp(&(*range.end() as u64)),
                            )
                            .map(|v| v.row())
                            .collect(),
                    )
                    .unwrap();
                });
            }
            Number::In(rows) => {
                let rows = rows.clone();
                spawn(move || {
                    let mut r = RowSet::default();
                    for i in rows {
                        r.append(
                            &mut index
                                .read()
                                .unwrap()
                                .triee()
                                .iter_by(|v| v.cmp(&(i as u64)))
                                .map(|x| x.row())
                                .collect(),
                        );
                    }
                    tx.send(r).unwrap();
                });
            }
        }
    }
    pub fn search_exec_uuid(data: &Data, uuids: &Vec<u128>, tx: Sender<RowSet>) {
        let index = data.uuid.clone();
        let uuids = uuids.clone();
        spawn(move || {
            let mut r = RowSet::default();
            for uuid in uuids {
                r.append(
                    &mut index
                        .read()
                        .unwrap()
                        .triee()
                        .iter_by(|v| v.cmp(&uuid))
                        .map(|x| x.row())
                        .collect(),
                );
            }
            tx.send(r).unwrap();
        });
    }
}
