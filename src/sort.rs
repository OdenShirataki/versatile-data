use std::{cmp::Ordering, fmt::Debug, num::NonZeroU32};

use idx_binary::{AvltrieeSearch, IdxFileAvlTriee};

use crate::{Data, FieldName, RowSet};

pub trait CustomSort {
    fn compare(&self, a: NonZeroU32, b: NonZeroU32) -> Ordering;
    fn asc(&self) -> Vec<NonZeroU32>;
    fn desc(&self) -> Vec<NonZeroU32>;
}
impl Debug for dyn CustomSort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CustomSort")?;
        Ok(())
    }
}

pub struct NoCustomSort {}

impl CustomSort for NoCustomSort {
    fn compare(&self, _: NonZeroU32, _: NonZeroU32) -> Ordering {
        unreachable!()
    }

    fn asc(&self) -> Vec<NonZeroU32> {
        unreachable!()
    }

    fn desc(&self) -> Vec<NonZeroU32> {
        unreachable!()
    }
}

#[derive(Debug)]
pub enum CustomOrderKey<C: CustomSort> {
    Serial,
    Row,
    TermBegin,
    TermEnd,
    LastUpdated,
    Field(FieldName),
    Custom(C),
}

pub type OrderKey = CustomOrderKey<NoCustomSort>;

#[derive(Debug)]
pub enum Order<C: CustomSort> {
    Asc(CustomOrderKey<C>),
    Desc(CustomOrderKey<C>),
}

impl Data {
    /// Sort search results.
    pub fn sort<C: CustomSort>(&self, rows: &RowSet, orders: &[Order<C>]) -> Vec<NonZeroU32> {
        let sub_orders = &orders[1..];
        match &orders[0] {
            Order::Asc(key) => self.sort_with_key(rows, key, sub_orders),
            Order::Desc(key) => self.sort_with_key_desc(rows, key, sub_orders),
        }
    }

    fn subsort<C: CustomSort>(
        &self,
        tmp: Vec<NonZeroU32>,
        sub_orders: &[Order<C>],
    ) -> Vec<NonZeroU32> {
        let mut tmp = tmp;
        tmp.sort_by(|a, b| {
            for i in 0..sub_orders.len() {
                match &sub_orders[i] {
                    Order::Asc(order_key) => match order_key {
                        CustomOrderKey::Serial => {
                            return unsafe {
                                self.serial
                                    .value_unchecked(*a)
                                    .cmp(self.serial.value_unchecked(*b))
                            };
                        }
                        CustomOrderKey::Row => return a.cmp(b),
                        CustomOrderKey::TermBegin => {
                            if let Some(ref f) = self.term_begin {
                                let ord =
                                    unsafe { f.value_unchecked(*a).cmp(f.value_unchecked(*b)) };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::TermEnd => {
                            if let Some(ref f) = self.term_end {
                                let ord =
                                    unsafe { f.value_unchecked(*a).cmp(f.value_unchecked(*b)) };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::LastUpdated => {
                            if let Some(ref f) = self.last_updated {
                                let ord =
                                    unsafe { f.value_unchecked(*a).cmp(f.value_unchecked(*b)) };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::Field(name) => {
                            if let Some(field) = self.fields.get(name) {
                                let ord = idx_binary::compare(
                                    field.value(*a).unwrap(),
                                    field.value(*b).unwrap(),
                                );
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::Custom(custom_order) => {
                            let ord = custom_order.compare(*a, *b);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                    },
                    Order::Desc(order_key) => match order_key {
                        CustomOrderKey::Serial => {
                            return unsafe {
                                self.serial
                                    .value_unchecked(*b)
                                    .cmp(self.serial.value_unchecked(*a))
                            };
                        }
                        CustomOrderKey::Row => {
                            return b.cmp(a);
                        }
                        CustomOrderKey::TermBegin => {
                            if let Some(ref f) = self.term_begin {
                                let ord =
                                    unsafe { f.value_unchecked(*b).cmp(f.value_unchecked(*a)) };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::TermEnd => {
                            if let Some(ref f) = self.term_end {
                                let ord =
                                    unsafe { f.value_unchecked(*b).cmp(f.value_unchecked(*a)) };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::LastUpdated => {
                            if let Some(ref f) = self.last_updated {
                                let ord =
                                    unsafe { f.value_unchecked(*b).cmp(f.value_unchecked(*a)) };
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::Field(name) => {
                            if let Some(field) = self.fields.get(name) {
                                let ord = idx_binary::compare(
                                    field.value(*b).unwrap(),
                                    field.value(*a).unwrap(),
                                );
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        CustomOrderKey::Custom(custom_order) => {
                            let ord = custom_order.compare(*b, *a);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                    },
                }
            }
            Ordering::Equal
        });
        tmp
    }

    fn sort_with_triee_inner<T: PartialEq, I: ?Sized, C: CustomSort>(
        &self,
        rows: &RowSet,
        triee: &IdxFileAvlTriee<T, I>,
        iter: impl Iterator<Item = NonZeroU32>,
        sub_orders: &[Order<C>],
    ) -> Vec<NonZeroU32> {
        if sub_orders.len() == 0 {
            iter.filter_map(|row| rows.contains(&row).then_some(row))
                .collect()
        } else {
            let mut ret = Vec::new();

            let mut before: Option<&T> = None;
            let mut tmp: Vec<NonZeroU32> = Vec::new();
            for r in iter {
                if rows.contains(&r) {
                    let value = unsafe { triee.node_unchecked(r) };
                    if let Some(before) = before {
                        if before.ne(value) {
                            ret.extend(if tmp.len() <= 1 {
                                tmp
                            } else {
                                self.subsort(tmp, sub_orders)
                            });
                            tmp = vec![];
                        }
                    } else {
                        ret.extend(tmp);
                        tmp = vec![];
                    }
                    tmp.push(r);
                    before = Some(value);
                }
            }
            ret.extend(if tmp.len() <= 1 {
                tmp
            } else {
                self.subsort(tmp, sub_orders)
            });
            ret
        }
    }

    fn sort_with_triee<T: PartialEq, I: ?Sized, C: CustomSort>(
        &self,
        rows: &RowSet,
        triee: &IdxFileAvlTriee<T, I>,
        sub_orders: &[Order<C>],
    ) -> Vec<NonZeroU32> {
        self.sort_with_triee_inner(rows, triee, triee.iter(), sub_orders)
    }

    fn sort_with_triee_desc<T: PartialEq, I: ?Sized, C: CustomSort>(
        &self,
        rows: &RowSet,
        index: &IdxFileAvlTriee<T, I>,
        sub_orders: &[Order<C>],
    ) -> Vec<NonZeroU32> {
        self.sort_with_triee_inner(rows, index, index.desc_iter(), sub_orders)
    }

    fn sort_with_key<C: CustomSort>(
        &self,
        rows: &RowSet,
        key: &CustomOrderKey<C>,
        sub_orders: &[Order<C>],
    ) -> Vec<NonZeroU32> {
        match key {
            CustomOrderKey::Serial => {
                self.sort_with_triee::<u32, u32, C>(rows, &self.serial, &vec![])
            }
            CustomOrderKey::Row => rows.into_iter().cloned().collect(),
            CustomOrderKey::TermBegin => self.term_begin.as_ref().map_or_else(
                || rows.into_iter().cloned().collect(),
                |f| self.sort_with_triee(rows, f, sub_orders),
            ),
            CustomOrderKey::TermEnd => self.term_end.as_ref().map_or_else(
                || rows.into_iter().cloned().collect(),
                |f| self.sort_with_triee(rows, f, sub_orders),
            ),
            CustomOrderKey::LastUpdated => self.term_end.as_ref().map_or_else(
                || rows.into_iter().cloned().collect(),
                |f| self.sort_with_triee(rows, f, sub_orders),
            ),
            CustomOrderKey::Field(name) => self.fields.get(name).map_or_else(
                || rows.into_iter().cloned().collect(),
                |f| self.sort_with_triee(rows, f.as_ref(), sub_orders),
            ),
            CustomOrderKey::Custom(custom_order) => custom_order.asc(),
        }
    }

    fn sort_with_key_desc<C: CustomSort>(
        &self,
        rows: &RowSet,
        key: &CustomOrderKey<C>,
        sub_orders: &[Order<C>],
    ) -> Vec<NonZeroU32> {
        match key {
            CustomOrderKey::Serial => {
                self.sort_with_triee_desc::<u32, u32, C>(rows, &self.serial, &vec![])
            }
            CustomOrderKey::Row => rows.into_iter().rev().cloned().collect(),
            CustomOrderKey::TermBegin => self.term_begin.as_ref().map_or_else(
                || rows.into_iter().rev().cloned().collect(),
                |f| self.sort_with_triee_desc(rows, f, sub_orders),
            ),
            CustomOrderKey::TermEnd => self.term_end.as_ref().map_or_else(
                || rows.into_iter().rev().cloned().collect(),
                |f| self.sort_with_triee_desc(rows, f, sub_orders),
            ),
            CustomOrderKey::LastUpdated => self.last_updated.as_ref().map_or_else(
                || rows.into_iter().rev().cloned().collect(),
                |f| self.sort_with_triee_desc(rows, f, sub_orders),
            ),
            CustomOrderKey::Field(name) => self.fields.get(name).map_or_else(
                || rows.into_iter().rev().cloned().collect(),
                |f| self.sort_with_triee_desc(rows, f.as_ref(), sub_orders),
            ),
            CustomOrderKey::Custom(custom_order) => custom_order.desc(),
        }
    }
}
