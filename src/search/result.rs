use std::{num::NonZeroU32, ops::Deref};

use async_recursion::async_recursion;
use futures::future;

use crate::{Condition, CustomSort, Data, FieldName, Order, RowSet, Search};

use super::{Field, Number, Term};

impl<'a> Search<'a> {
    pub async fn result(&self) -> RowSet {
        if self.conditions.len() > 0 {
            self.data.result(&self.conditions).await
        } else {
            self.data.all()
        }
    }

    pub async fn result_with_sort<C: CustomSort>(&self, orders: Vec<Order<C>>) -> Vec<NonZeroU32> {
        self.data.sort(&self.result().await, &orders)
    }
}

impl Data {
    /// Returns search results by specifying [Condition].
    #[async_recursion(?Send)]
    pub async fn result_condition(&self, condition: &Condition) -> RowSet {
        match condition {
            Condition::Activity(condition) => {
                if let Some(ref index) = self.activity {
                    let activity = *condition as u8;
                    index.iter_by(|v| v.cmp(&activity)).collect()
                } else {
                    RowSet::default()
                }
            }
            Condition::Term(condition) => self.result_term(condition),
            Condition::Field(field_name, condition) => self.result_field(field_name, condition),
            Condition::Row(condition) => self.result_row(condition),
            Condition::LastUpdated(condition) => self.result_last_updated(condition),
            Condition::Uuid(uuid) => self.result_uuid(uuid),
            Condition::Narrow(conditions) => self.result(conditions).await,
            Condition::Wide(conditions) => {
                future::join_all(conditions.into_iter().map(|c| self.result_condition(c)))
                    .await
                    .into_iter()
                    .flatten()
                    .collect()
            }
        }
    }

    #[async_recursion(?Send)]
    async fn result(&self, conditions: &Vec<Condition>) -> RowSet {
        let (mut rows, _index, fs) =
            future::select_all(conditions.into_iter().map(|c| self.result_condition(c))).await;
        for r in future::join_all(fs).await.into_iter() {
            rows.retain(|v| r.contains(v));
        }
        rows
    }

    fn result_last_updated(&self, condition: &Number) -> RowSet {
        if let Some(ref f) = self.last_updated {
            match condition {
                Number::Min(min) => {
                    let min = *min as u64;
                    f.iter_from(|v| v.cmp(&min)).collect()
                }
                Number::Max(max) => {
                    let max = *max as u64;
                    f.iter_to(|v| v.cmp(&max)).collect()
                }
                Number::Range(range) => f
                    .iter_range(
                        |v| v.cmp(&(*range.start() as u64)),
                        |v| v.cmp(&(*range.end() as u64)),
                    )
                    .collect(),
                Number::In(rows) => rows
                    .into_iter()
                    .map(|i| f.iter_by(|v| v.cmp(&(*i as u64))))
                    .flatten()
                    .collect(),
            }
        } else {
            unreachable!();
        }
    }

    fn result_uuid(&self, uuids: &[u128]) -> RowSet {
        if let Some(ref index) = self.uuid {
            uuids
                .into_iter()
                .map(|uuid| index.iter_by(|v| v.cmp(&uuid)))
                .flatten()
                .collect()
        } else {
            unreachable!();
        }
    }
    fn result_term(&self, condition: &Term) -> RowSet {
        match condition {
            Term::In(base) => {
                if let Some(ref term_begin) = self.term_begin {
                    if let Some(ref term_end) = self.term_end {
                        return term_begin
                            .iter_to(|v| v.cmp(base))
                            .filter_map(|row| {
                                let end = term_end.get(row).map(|v| *v.deref()).unwrap_or(0);
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
                if let Some(ref index) = self.term_begin {
                    return index.iter_from(|v| v.cmp(&base)).collect();
                } else {
                    unreachable!();
                }
            }
            Term::Past(base) => {
                if let Some(ref index) = self.term_end {
                    return index.iter_range(|v| v.cmp(&1), |v| v.cmp(&base)).collect();
                } else {
                    unreachable!();
                }
            }
        }
    }

    fn result_row(&self, condition: &Number) -> RowSet {
        match condition {
            Number::Min(row) => {
                let row = *row;
                self.serial
                    .iter()
                    .filter_map(|i| (i.get() as isize >= row).then_some(i))
                    .collect()
            }
            Number::Max(row) => {
                let row = *row;
                self.serial
                    .iter()
                    .filter_map(|i| (i.get() as isize <= row).then_some(i))
                    .collect()
            }
            Number::Range(range) => range
                .clone()
                .filter_map(|i| {
                    (i > 0
                        && self
                            .serial
                            .get(unsafe { NonZeroU32::new_unchecked(i as u32) })
                            .is_some())
                    .then_some(unsafe { NonZeroU32::new_unchecked(i as u32) })
                })
                .collect(),
            Number::In(rows) => rows
                .into_iter()
                .filter_map(|i| {
                    let i = *i;
                    (i > 0
                        && self
                            .serial
                            .get(unsafe { NonZeroU32::new_unchecked(i as u32) })
                            .is_some())
                    .then_some(unsafe { NonZeroU32::new_unchecked(i as u32) })
                })
                .collect(),
        }
    }

    pub fn result_field(&self, name: &FieldName, condition: &Field) -> RowSet {
        if let Some(field) = self.fields.get(name) {
            match condition {
                Field::Match(v) => field.iter_by(|data| field.cmp(data, &v)).collect(),
                Field::Min(min) => field.iter_from(|data| field.cmp(data, &min)).collect(),
                Field::Max(max) => field.iter_to(|data| field.cmp(data, &max)).collect(),
                Field::Range(min, max) => field
                    .iter_range(|data| field.cmp(data, &min), |data| field.cmp(data, &max))
                    .collect(),
                Field::Forward(cont) => Self::result_field_sub(field, cont, Self::forward),
                Field::Partial(cont) => Self::result_field_sub(field, cont, Self::partial),
                Field::Backward(cont) => Self::result_field_sub(field, cont, Self::backward),
                Field::ValueForward(cont) => {
                    Self::result_field_sub(field, cont, Self::value_forward)
                }
                Field::ValuePartial(cont) => {
                    Self::result_field_sub(field, cont, Self::value_partial)
                }
                Field::ValueBackward(cont) => {
                    Self::result_field_sub(field, cont, Self::value_backward)
                }
            }
        } else {
            RowSet::default()
        }
    }

    fn result_field_sub(
        field: &crate::Field,
        cont: &str,
        func: fn(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool),
    ) -> RowSet {
        field
            .iter()
            .map(|row| func(row, field, cont))
            .filter_map(|(v, b)| b.then_some(v))
            .collect()
    }

    fn forward(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool) {
        (
            row,
            field
                .bytes(row)
                .map_or(false, |bytes| bytes.starts_with(cont.as_bytes())),
        )
    }

    fn partial(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool) {
        (
            row,
            field.bytes(row).map_or(false, |bytes| {
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

    fn backward(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool) {
        (
            row,
            field
                .bytes(row)
                .map_or(false, |bytes| bytes.ends_with(cont.as_bytes())),
        )
    }

    fn value_forward(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool) {
        (
            row,
            field
                .bytes(row)
                .map_or(false, |bytes| cont.as_bytes().starts_with(bytes)),
        )
    }

    fn value_partial(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool) {
        (
            row,
            field.bytes(row).map_or(false, |bytes| {
                cont.as_bytes()
                    .windows(bytes.len())
                    .position(|window| window == bytes)
                    .is_some()
            }),
        )
    }

    fn value_backward(row: NonZeroU32, field: &crate::Field, cont: &str) -> (NonZeroU32, bool) {
        (
            row,
            field
                .bytes(row)
                .map_or(false, |bytes| cont.as_bytes().ends_with(bytes)),
        )
    }
}
