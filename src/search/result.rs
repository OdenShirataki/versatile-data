use std::sync::{Arc, RwLock};

use async_recursion::async_recursion;
use futures::{executor::block_on, future, Future};

use crate::{Condition, Data, Order, RowSet, Search};

use super::{Field, Number, Term};

impl<'a> Search<'a> {
    pub fn result(&mut self) -> RowSet {
        if self.conditions.len() > 0 {
            block_on(async { Self::result_async(&self.data, &self.conditions).await })
        } else {
            self.data.all()
        }
    }
    pub fn result_with_sort(&mut self, orders: Vec<Order>) -> Vec<u32> {
        let rows = self.result();
        self.data.sort(&rows, &orders)
    }

    #[async_recursion]
    pub async fn result_condition(data: &Data, condition: &Condition) -> RowSet {
        match condition {
            Condition::Activity(condition) => {
                if let Some(ref index) = data.activity {
                    let activity = *condition as u8;
                    index
                        .read()
                        .unwrap()
                        .iter_by(|v| v.cmp(&activity))
                        .collect()
                } else {
                    unreachable!();
                }
            }
            Condition::Term(condition) => Self::result_term(data, condition),
            Condition::Field(field_name, condition) => {
                Self::result_field(data, field_name, condition).await
            }
            Condition::Row(condition) => Self::result_row(data, condition),
            Condition::LastUpdated(condition) => Self::result_last_updated(data, condition),
            Condition::Uuid(uuid) => Self::result_uuid(data, uuid),
            Condition::Narrow(conditions) => {
                let mut new_search = Search::new(data);
                for c in conditions {
                    new_search = new_search.search(c.clone());
                }
                new_search.result()
            }
            Condition::Wide(conditions) => {
                let mut rows = RowSet::default();
                let mut fs: Vec<_> = conditions
                    .iter()
                    .map(|c| Self::result_condition(data, c))
                    .collect();
                while !fs.is_empty() {
                    let (ret, _index, remaining) = future::select_all(fs).await;
                    rows.extend(ret);
                    fs = remaining;
                }

                rows
            }
        }
    }

    async fn result_async(data: &Data, conditions: &Vec<Condition>) -> RowSet {
        let mut fs: Vec<_> = conditions
            .iter()
            .map(|c| Self::result_condition(data, c))
            .collect();
        let (ret, _index, remaining) = future::select_all(fs).await;
        let mut rows = ret;
        fs = remaining;
        while !fs.is_empty() {
            let (ret, _index, remaining) = future::select_all(fs).await;
            rows = rows.intersection(&ret).cloned().collect();
            fs = remaining;
        }

        rows
    }

    fn result_term(data: &Data, condition: &Term) -> RowSet {
        match condition {
            Term::In(base) => {
                if let Some(ref term_begin) = data.term_begin {
                    let term_begin = Arc::clone(term_begin);
                    if let Some(ref term_end) = data.term_end {
                        return term_begin
                            .read()
                            .unwrap()
                            .iter_to(|v| v.cmp(base))
                            .filter_map(|row| {
                                let end = *term_end.read().unwrap().value(row).unwrap_or(&0);
                                (end == 0 || end > *base).then_some(row)
                            })
                            .collect();
                    } else {
                        unreachable!();
                    }
                } else {
                    unreachable!();
                }
            }
            Term::Future(base) => {
                if let Some(ref index) = data.term_begin {
                    return index.read().unwrap().iter_from(|v| v.cmp(&base)).collect();
                } else {
                    unreachable!();
                }
            }
            Term::Past(base) => {
                if let Some(ref index) = data.term_end {
                    return index
                        .read()
                        .unwrap()
                        .iter_range(|v| v.cmp(&1), |v| v.cmp(&base))
                        .collect();
                } else {
                    unreachable!();
                }
            }
        }
    }

    fn result_row(data: &Data, condition: &Number) -> RowSet {
        match condition {
            Number::Min(row) => {
                let row = *row;
                data.serial
                    .read()
                    .unwrap()
                    .iter()
                    .filter_map(|i| (i as isize >= row).then_some(i))
                    .collect()
            }
            Number::Max(row) => {
                let row = *row;
                data.serial
                    .read()
                    .unwrap()
                    .iter()
                    .filter_map(|i| (i as isize <= row).then_some(i))
                    .collect()
            }
            Number::Range(range) => range
                .clone()
                .filter_map(|i| {
                    (i > 0 && data.serial.read().unwrap().exists(i as u32)).then_some(i as u32)
                })
                .collect(),
            Number::In(rows) => rows
                .iter()
                .filter_map(|i| {
                    let i = *i;
                    (i > 0 && data.serial.read().unwrap().exists(i as u32)).then_some(i as u32)
                })
                .collect(),
        }
    }

    async fn result_field(data: &Data, field_name: &str, condition: &Field) -> RowSet {
        if let Some(field) = data.field(field_name) {
            let field = Arc::clone(&field);
            match condition {
                Field::Match(v) => {
                    let field = field.read().unwrap();
                    field.iter_by(|data| field.cmp(data, &v)).collect()
                }
                Field::Min(min) => {
                    let field = field.read().unwrap();
                    field.iter_from(|data| field.cmp(data, &min)).collect()
                }
                Field::Max(max) => {
                    let field = field.read().unwrap();
                    field.iter_to(|data| field.cmp(data, &max)).collect()
                }
                Field::Range(min, max) => {
                    let field = field.read().unwrap();
                    field
                        .iter_range(|data| field.cmp(data, &min), |data| field.cmp(data, &max))
                        .collect()
                }
                Field::Forward(cont) => Self::result_field_sub(field, cont, Self::forward).await,
                Field::Partial(cont) => Self::result_field_sub(field, cont, Self::partial).await,
                Field::Backward(cont) => Self::result_field_sub(field, cont, Self::backward).await,
                Field::ValueForward(cont) => {
                    Self::result_field_sub(field, cont, Self::value_forward).await
                }
                Field::ValuePartial(cont) => {
                    Self::result_field_sub(field, cont, Self::value_partial).await
                }
                Field::ValueBackward(cont) => {
                    Self::result_field_sub(field, cont, Self::value_backward).await
                }
            }
        } else {
            RowSet::default()
        }
    }
    async fn result_field_sub<Fut>(
        field: Arc<RwLock<crate::Field>>,
        cont: &Arc<String>,
        func: fn(row: u32, field: Arc<RwLock<crate::Field>>, cont: Arc<String>) -> Fut,
    ) -> RowSet
    where
        Fut: Future<Output = (u32, bool)>,
    {
        let mut rows: RowSet = RowSet::default();

        let mut fs: Vec<_> = field
            .read()
            .unwrap()
            .iter()
            .map(|row| Box::pin(func(row, Arc::clone(&field), Arc::clone(cont))))
            .collect();
        while !fs.is_empty() {
            let (val, _index, remaining) = future::select_all(fs).await;
            if val.1 {
                rows.insert(val.0);
            }
            fs = remaining;
        }

        rows
    }
    async fn forward(row: u32, field: Arc<RwLock<crate::Field>>, cont: Arc<String>) -> (u32, bool) {
        (
            row,
            field
                .read()
                .unwrap()
                .bytes(row)
                .map_or(false, |bytes| bytes.starts_with(cont.as_bytes())),
        )
    }
    async fn partial(row: u32, field: Arc<RwLock<crate::Field>>, cont: Arc<String>) -> (u32, bool) {
        (
            row,
            field.read().unwrap().bytes(row).map_or(false, |bytes| {
                let len = cont.len();
                len <= bytes.len() && {
                    let cont_bytes = cont.as_bytes();
                    bytes
                        .windows(len)
                        .position(|window| window == cont_bytes)
                        .is_some()
                }
            }),
        )
    }
    async fn backward(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        (
            row,
            field
                .read()
                .unwrap()
                .bytes(row)
                .map_or(false, |bytes| bytes.ends_with(cont.as_bytes())),
        )
    }
    async fn value_forward(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        (
            row,
            field
                .read()
                .unwrap()
                .bytes(row)
                .map_or(false, |bytes| cont.as_bytes().starts_with(bytes)),
        )
    }
    async fn value_partial(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        (
            row,
            field.read().unwrap().bytes(row).map_or(false, |bytes| {
                cont.as_bytes()
                    .windows(bytes.len())
                    .position(|window| window == bytes)
                    .is_some()
            }),
        )
    }
    async fn value_backward(
        row: u32,
        field: Arc<RwLock<crate::Field>>,
        cont: Arc<String>,
    ) -> (u32, bool) {
        (
            row,
            field
                .read()
                .unwrap()
                .bytes(row)
                .map_or(false, |bytes| cont.as_bytes().ends_with(bytes)),
        )
    }

    fn result_last_updated(data: &Data, condition: &Number) -> RowSet {
        if let Some(ref f) = data.last_updated {
            let index = Arc::clone(f);
            match condition {
                Number::Min(min) => {
                    let min = *min as u64;
                    index.read().unwrap().iter_from(|v| v.cmp(&min)).collect()
                }
                Number::Max(max) => {
                    let max = *max as u64;
                    index.read().unwrap().iter_to(|v| v.cmp(&max)).collect()
                }
                Number::Range(range) => {
                    let range = range.clone();
                    index
                        .read()
                        .unwrap()
                        .iter_range(
                            |v| v.cmp(&(*range.start() as u64)),
                            |v| v.cmp(&(*range.end() as u64)),
                        )
                        .collect()
                }
                Number::In(rows) => {
                    let mut r = RowSet::default();
                    for i in rows {
                        r.extend(
                            index
                                .read()
                                .unwrap()
                                .iter_by(|v| v.cmp(&(*i as u64)))
                                .collect::<RowSet>(),
                        );
                    }
                    r
                }
            }
        } else {
            unreachable!();
        }
    }
    fn result_uuid(data: &Data, uuids: &Vec<u128>) -> RowSet {
        if let Some(ref index) = data.uuid {
            let mut r = RowSet::default();
            for uuid in uuids {
                r.extend(
                    index
                        .read()
                        .unwrap()
                        .iter_by(|v| v.cmp(&uuid))
                        .collect::<RowSet>(),
                );
            }
            r
        } else {
            unreachable!();
        }
    }
}
