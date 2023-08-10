use std::{
    sync::{
        mpsc::{channel, SendError, Sender},
        Arc, RwLock,
    },
    thread::spawn,
};

use futures::{executor::block_on, future, Future};

use crate::{Activity, Condition, Data, Order, RowSet, Search};

use super::{Field, Number, Term};

impl<'a> Search<'a> {
    pub fn result(mut self) -> Result<RowSet, SendError<RowSet>> {
        self.search_exec()
    }
    pub fn result_with_sort(&mut self, orders: Vec<Order>) -> Result<Vec<u32>, SendError<RowSet>> {
        let rows = self.search_exec()?;
        Ok(self.data.sort(&rows, &orders))
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
        let conditions_count = self.conditions.len();
        Ok(if conditions_count == 0 {
            let mut rows = RowSet::default();
            for row in self.data.serial.read().unwrap().iter() {
                rows.insert(row.row());
            }
            rows
        } else {
            let (tx, rx) = channel();
            for c in self.conditions.iter() {
                let tx = tx.clone();
                Self::search_exec_cond(self.data, c, tx)?;
            }
            drop(tx);
            let mut iter = rx.iter();
            let mut rows = iter.next().unwrap();
            for rs in iter {
                rows = rows.intersection(&rs).map(|&x| x).collect();
            }
            rows
        })
    }

    fn search_exec_activity(data: &Data, condition: &Activity, tx: Sender<RowSet>) {
        if let Some(ref activity) = data.activity {
            let index = Arc::clone(activity);
            let activity = *condition as u8;
            spawn(move || {
                tx.send(
                    index
                        .read()
                        .unwrap()
                        .iter_by(|v| v.cmp(&activity))
                        .map(|v| v.row())
                        .collect(),
                )
                .unwrap();
            });
        } else {
            unreachable!();
        }
    }
    fn search_exec_term_in(data: &Data, base: u64, tx: Sender<RowSet>) {
        if let Some(ref term_begin) = data.term_begin {
            let term_begin = Arc::clone(term_begin);
            if let Some(ref term_end) = data.term_end {
                let term_end = Arc::clone(term_end);
                spawn(move || {
                    let mut result = RowSet::default();
                    for node in term_begin.read().unwrap().iter_to(|v| v.cmp(&base)) {
                        let row = node.row();
                        let end = *term_end.read().unwrap().value(row).unwrap_or(&0);
                        if end == 0 || end > base {
                            result.replace(row);
                        }
                    }
                    tx.send(result).unwrap();
                });
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }
    fn search_exec_term(data: &Data, condition: &Term, tx: Sender<RowSet>) {
        match condition {
            Term::In(base) => {
                Self::search_exec_term_in(data, *base, tx);
            }
            Term::Future(base) => {
                if let Some(ref f) = data.term_begin {
                    let index = Arc::clone(f);
                    let base = base.clone();
                    spawn(move || {
                        tx.send(
                            index
                                .read()
                                .unwrap()
                                .iter_from(|v| v.cmp(&base))
                                .map(|v| v.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                } else {
                    unreachable!();
                }
            }
            Term::Past(base) => {
                if let Some(ref f) = data.term_end {
                    let index = Arc::clone(f);
                    let base = base.clone();
                    spawn(move || {
                        tx.send(
                            index
                                .read()
                                .unwrap()
                                .iter_range(|v| v.cmp(&1), |v| v.cmp(&base))
                                .map(|v| v.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                } else {
                    unreachable!();
                }
            }
        }
    }
    fn search_exec_row(data: &Data, condition: &Number, tx: Sender<RowSet>) {
        let serial = Arc::clone(&data.serial);
        let mut r = RowSet::default();
        match condition {
            Number::Min(row) => {
                let row = *row;
                spawn(move || {
                    for i in serial.read().unwrap().iter() {
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
                    for i in serial.read().unwrap().iter() {
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
                            if serial.read().unwrap().exists(i as u32) {
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
                            if serial.read().unwrap().exists(i as u32) {
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
            let field = Arc::clone(&field);
            match condition {
                Field::Match(v) => {
                    let v = Arc::clone(&v);
                    spawn(move || {
                        let field = field.read().unwrap();
                        tx.send(
                            field
                                .iter_by(|data| field.cmp(data, &v))
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Min(min) => {
                    let min = Arc::clone(&min);
                    spawn(move || {
                        let field = field.read().unwrap();
                        tx.send(
                            field
                                .iter_from(|data| field.cmp(data, &min))
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Max(max) => {
                    let max = Arc::clone(&max);
                    spawn(move || {
                        let field = field.read().unwrap();
                        tx.send(
                            field
                                .iter_to(|data| field.cmp(data, &max))
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Range(min, max) => {
                    let min = Arc::clone(&min);
                    let max = Arc::clone(&max);
                    spawn(move || {
                        let field = field.read().unwrap();
                        tx.send(
                            field
                                .iter_range(
                                    |data| field.cmp(data, &min),
                                    |data| field.cmp(data, &max),
                                )
                                .map(|x| x.row())
                                .collect(),
                        )
                        .unwrap();
                    });
                }
                Field::Forward(cont) => {
                    let cont = Arc::clone(&cont);
                    spawn(move || {
                        Self::field_result(field, cont, Self::forward, tx);
                    });
                }
                Field::Partial(cont) => {
                    let cont = Arc::clone(&cont);
                    spawn(move || {
                        Self::field_result(field, cont, Self::partial, tx);
                    });
                }
                Field::Backward(cont) => {
                    let cont = Arc::clone(&cont);
                    spawn(move || {
                        Self::field_result(field, cont, Self::backward, tx);
                    });
                }
                Field::ValueForward(cont) => {
                    let cont = Arc::clone(&cont);
                    spawn(move || {
                        Self::field_result(field, cont, Self::value_forward, tx);
                    });
                }
                Field::ValuePartial(cont) => {
                    let cont = Arc::clone(&cont);
                    spawn(move || {
                        Self::field_result(field, cont, Self::value_partial, tx);
                    });
                }
                Field::ValueBackward(cont) => {
                    let cont = Arc::clone(&cont);
                    spawn(move || {
                        Self::field_result(field, cont, Self::value_backward, tx);
                    });
                }
            }
        }
    }
    fn field_result<Fut>(
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
        func: fn(row: u32, field: Arc<RwLock<crate::Field>>, cont: Arc<String>) -> Fut,
        tx: Sender<RowSet>,
    ) where
        Fut: Future<Output = (u32, bool)>,
    {
        let mut rows: RowSet = RowSet::default();
        let mut fs = vec![];
        for row in field.read().unwrap().iter() {
            fs.push(func(row.row(), Arc::clone(&field), Arc::clone(&cont)));
        }
        let mut fs: Vec<_> = fs.into_iter().map(Box::pin).collect();
        block_on(async {
            while !fs.is_empty() {
                let (val, _index, remaining) = future::select_all(fs).await;
                if val.1 {
                    rows.insert(val.0);
                }
                fs = remaining;
            }
        });
        tx.send(rows).unwrap();
    }
    async fn forward(row: u32, field: Arc<RwLock<crate::Field>>, cont: Arc<String>) -> (u32, bool) {
        let mut ret = false;
        if let Some(bytes) = field.read().unwrap().bytes(row) {
            if bytes.starts_with(cont.as_bytes()) {
                ret = true;
            }
        }
        (row, ret)
    }
    async fn partial(row: u32, field: Arc<RwLock<crate::Field>>, cont: Arc<String>) -> (u32, bool) {
        let mut ret = false;
        if let Some(bytes) = field.read().unwrap().bytes(row) {
            let len = bytes.len();
            if let Some(_) = cont
                .as_bytes()
                .windows(len)
                .position(|window| window == bytes)
            {
                ret = true;
            }
        }
        (row, ret)
    }
    async fn backward(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        let mut ret = false;
        if let Some(bytes) = field.read().unwrap().bytes(row) {
            if bytes.ends_with(cont.as_bytes()) {
                ret = true;
            }
        }
        (row, ret)
    }
    async fn value_forward(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        let mut ret = false;
        if let Some(bytes) = field.read().unwrap().bytes(row) {
            if cont.as_bytes().starts_with(bytes) {
                ret = true;
            }
        }
        (row, ret)
    }
    async fn value_partial(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        let mut ret = false;
        if let Some(bytes) = field.read().unwrap().bytes(row) {
            let len = bytes.len();
            if let Some(_) = cont
                .as_bytes()
                .windows(len)
                .position(|window| window == bytes)
            {
                ret = true;
            }
        }
        (row, ret)
    }
    async fn value_backward(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        let mut ret = false;
        if let Some(bytes) = field.read().unwrap().bytes(row) {
            if cont.as_bytes().ends_with(bytes) {
                ret = true;
            }
        }
        (row, ret)
    }

    fn search_exec_last_updated(data: &Data, condition: &Number, tx: Sender<RowSet>) {
        if let Some(ref f) = data.last_updated {
            let index = Arc::clone(f);
            match condition {
                Number::Min(min) => {
                    let min = min.clone() as u64;
                    spawn(move || {
                        tx.send(
                            index
                                .read()
                                .unwrap()
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
                                    .iter_by(|v| v.cmp(&(i as u64)))
                                    .map(|x| x.row())
                                    .collect(),
                            );
                        }
                        tx.send(r).unwrap();
                    });
                }
            }
        } else {
            unreachable!();
        }
    }
    pub fn search_exec_uuid(data: &Data, uuids: &Vec<u128>, tx: Sender<RowSet>) {
        if let Some(ref uuid) = data.uuid {
            let index = Arc::clone(uuid);
            let uuids = uuids.clone();
            spawn(move || {
                let mut r = RowSet::default();
                for uuid in uuids {
                    r.append(
                        &mut index
                            .read()
                            .unwrap()
                            .iter_by(|v| v.cmp(&uuid))
                            .map(|x| x.row())
                            .collect(),
                    );
                }
                tx.send(r).unwrap();
            });
        } else {
            unreachable!();
        }
    }
}
