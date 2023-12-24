use std::num::NonZeroU32;

use async_recursion::async_recursion;
use futures::future;

use crate::{Condition, Data, Order, RowSet, Search};

use super::{Field, Number, Term};

impl<'a> Search<'a> {
    pub async fn result(&mut self) -> RowSet {
        if self.conditions.len() > 0 {
            Self::result_inner(&self.data, &self.conditions).await
        } else {
            self.data.all()
        }
    }

    pub async fn result_with_sort(&mut self, orders: Vec<Order>) -> Vec<NonZeroU32> {
        self.data.sort(&self.result().await, &orders)
    }

    #[async_recursion(?Send)]
    pub async fn result_condition(data: &Data, condition: &Condition<'a>) -> RowSet {
        match condition {
            Condition::Activity(condition) => {
                if let Some(ref index) = data.activity {
                    let activity = *condition as u8;
                    index.iter_by(|v| v.cmp(&activity)).collect()
                } else {
                    //unreachable!();
                    RowSet::default()
                }
            }
            Condition::Term(condition) => Self::result_term(data, condition),
            Condition::Field(field_name, condition) => {
                Self::result_field(data, field_name, condition)
            }
            Condition::Row(condition) => Self::result_row(data, condition),
            Condition::LastUpdated(condition) => Self::result_last_updated(data, condition),
            Condition::Uuid(uuid) => Self::result_uuid(data, uuid),
            Condition::Narrow(conditions) => Self::result_inner(data, conditions).await,
            Condition::Wide(conditions) => future::join_all(
                conditions
                    .into_iter()
                    .map(|c| Self::result_condition(data, c)),
            )
            .await
            .into_iter()
            .flatten()
            .collect(),
        }
    }

    async fn result_inner(data: &Data, conditions: &Vec<Condition<'a>>) -> RowSet {
        let (mut rows, _index, fs) = future::select_all(
            conditions
                .into_iter()
                .map(|c| Self::result_condition(data, c)),
        )
        .await;
        for r in future::join_all(fs).await.into_iter() {
            rows.retain(|v| r.contains(v));
        }
        rows
    }

    fn result_term(data: &Data, condition: &Term) -> RowSet {
        match condition {
            Term::In(base) => {
                if let Some(ref term_begin) = data.term_begin {
                    if let Some(ref term_end) = data.term_end {
                        return term_begin
                            .iter_to(|v| v.cmp(base))
                            .filter_map(|row| {
                                let end = *term_end.value(row).unwrap_or(&0);
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
                    return index.iter_from(|v| v.cmp(&base)).collect();
                } else {
                    unreachable!();
                }
            }
            Term::Past(base) => {
                if let Some(ref index) = data.term_end {
                    return index.iter_range(|v| v.cmp(&1), |v| v.cmp(&base)).collect();
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
                    .iter()
                    .filter_map(|i| (i.get() as isize >= row).then_some(i))
                    .collect()
            }
            Number::Max(row) => {
                let row = *row;
                data.serial
                    .iter()
                    .filter_map(|i| (i.get() as isize <= row).then_some(i))
                    .collect()
            }
            Number::Range(range) => range
                .clone()
                .filter_map(|i| {
                    (i > 0
                        && data
                            .serial
                            .exists(unsafe { NonZeroU32::new_unchecked(i as u32) }))
                    .then_some(unsafe { NonZeroU32::new_unchecked(i as u32) })
                })
                .collect(),
            Number::In(rows) => rows
                .into_iter()
                .filter_map(|i| {
                    let i = *i;
                    (i > 0
                        && data
                            .serial
                            .exists(unsafe { NonZeroU32::new_unchecked(i as u32) }))
                    .then_some(unsafe { NonZeroU32::new_unchecked(i as u32) })
                })
                .collect(),
        }
    }

    pub fn result_field(data: &Data, field_name: &str, condition: &Field) -> RowSet {
        if let Some(field) = data.field(field_name) {
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

    fn result_last_updated(data: &Data, condition: &Number) -> RowSet {
        if let Some(ref f) = data.last_updated {
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

    fn result_uuid(data: &Data, uuids: &[u128]) -> RowSet {
        if let Some(ref index) = data.uuid {
            uuids
                .into_iter()
                .map(|uuid| index.iter_by(|v| v.cmp(&uuid)))
                .flatten()
                .collect()
        } else {
            unreachable!();
        }
    }
}
