use std::{cmp::Ordering, fmt::Debug, num::NonZeroU32};

use idx_binary::Avltriee;

use crate::{Data, RowSet};

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

#[derive(Debug)]
pub enum OrderKey {
    Serial,
    Row,
    TermBegin,
    TermEnd,
    LastUpdated,
    Field(String),
    Custom(Box<dyn CustomSort>),
}

#[derive(Debug)]
pub enum Order {
    Asc(OrderKey),
    Desc(OrderKey),
}

impl Data {
    #[inline(always)]
    pub fn sort(&self, rows: &RowSet, orders: &[Order]) -> Vec<NonZeroU32> {
        let sub_orders = &orders[1..];
        match &orders[0] {
            Order::Asc(key) => self.sort_with_key(rows, key, sub_orders),
            Order::Desc(key) => self.sort_with_key_desc(rows, key, sub_orders),
        }
    }

    #[inline(always)]
    fn subsort(&self, tmp: Vec<NonZeroU32>, sub_orders: &[Order]) -> Vec<NonZeroU32> {
        let mut tmp = tmp;
        tmp.sort_by(|a, b| {
            for i in 0..sub_orders.len() {
                match &sub_orders[i] {
                    Order::Asc(order_key) => match order_key {
                        OrderKey::Serial => {
                            return self
                                .serial
                                .read()
                                .unwrap()
                                .value(a.get())
                                .unwrap()
                                .cmp(self.serial.read().unwrap().value(b.get()).unwrap());
                        }
                        OrderKey::Row => return a.cmp(b),
                        OrderKey::TermBegin => {
                            if let Some(ref f) = self.term_begin {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(a.get())
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(b.get()).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::TermEnd => {
                            if let Some(ref f) = self.term_end {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(a.get())
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(b.get()).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::LastUpdated => {
                            if let Some(ref f) = self.last_updated {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(a.get())
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(b.get()).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::Field(field_name) => {
                            if let Some(field) = self.field(&field_name) {
                                let ord = idx_binary::compare(
                                    field.read().unwrap().bytes(a.get()).unwrap(),
                                    field.read().unwrap().bytes(b.get()).unwrap(),
                                );
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::Custom(custom_order) => {
                            let ord = custom_order.compare(*a, *b);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                    },
                    Order::Desc(order_key) => match order_key {
                        OrderKey::Serial => {
                            return self
                                .serial
                                .read()
                                .unwrap()
                                .value(b.get())
                                .unwrap()
                                .cmp(self.serial.read().unwrap().value(a.get()).unwrap());
                        }
                        OrderKey::Row => {
                            return b.cmp(a);
                        }
                        OrderKey::TermBegin => {
                            if let Some(ref f) = self.term_begin {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(b.get())
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(a.get()).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::TermEnd => {
                            if let Some(ref f) = self.term_end {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(b.get())
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(a.get()).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::LastUpdated => {
                            if let Some(ref f) = self.last_updated {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(b.get())
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(a.get()).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::Field(field_name) => {
                            if let Some(field) = self.field(&field_name) {
                                let ord = idx_binary::compare(
                                    field.read().unwrap().bytes(b.get()).unwrap(),
                                    field.read().unwrap().bytes(a.get()).unwrap(),
                                );
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::Custom(custom_order) => {
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

    #[inline(always)]
    fn sort_with_triee_inner<T>(
        &self,
        rows: &RowSet,
        triee: &Avltriee<T>,
        iter: impl Iterator<Item = NonZeroU32>,
        sub_orders: &[Order],
    ) -> Vec<NonZeroU32>
    where
        T: PartialEq,
    {
        if sub_orders.len() == 0 {
            iter.filter_map(|row| rows.contains(&row).then_some(row))
                .collect()
        } else {
            let mut ret = Vec::new();

            let mut before: Option<&T> = None;
            let mut tmp: Vec<NonZeroU32> = Vec::new();
            for r in iter {
                if rows.contains(&r) {
                    let value = unsafe { triee.value_unchecked(r) };
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

    #[inline(always)]
    fn sort_with_triee<T>(
        &self,
        rows: &RowSet,
        triee: &Avltriee<T>,
        sub_orders: &[Order],
    ) -> Vec<NonZeroU32>
    where
        T: PartialEq,
    {
        self.sort_with_triee_inner(rows, triee, triee.iter(), sub_orders)
    }

    #[inline(always)]
    fn sort_with_triee_desc<T>(
        &self,
        rows: &RowSet,
        triee: &Avltriee<T>,
        sub_orders: &[Order],
    ) -> Vec<NonZeroU32>
    where
        T: PartialEq,
    {
        self.sort_with_triee_inner(rows, triee, triee.desc_iter(), sub_orders)
    }

    #[inline(always)]
    fn sort_with_key(
        &self,
        rows: &RowSet,
        key: &OrderKey,
        sub_orders: &[Order],
    ) -> Vec<NonZeroU32> {
        match key {
            OrderKey::Serial => self.sort_with_triee(rows, &self.serial.read().unwrap(), &vec![]),
            OrderKey::Row => rows.iter().copied().collect(),
            OrderKey::TermBegin => self.term_begin.as_ref().map_or_else(
                || rows.iter().copied().collect(),
                |f| self.sort_with_triee(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::TermEnd => self.term_end.as_ref().map_or_else(
                || rows.iter().copied().collect(),
                |f| self.sort_with_triee(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::LastUpdated => self.term_end.as_ref().map_or_else(
                || rows.iter().copied().collect(),
                |f| self.sort_with_triee(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::Field(field_name) => self.field(&field_name).map_or_else(
                || rows.iter().copied().collect(),
                |f| self.sort_with_triee(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::Custom(custom_order) => custom_order.asc(),
        }
    }

    #[inline(always)]
    fn sort_with_key_desc(
        &self,
        rows: &RowSet,
        key: &OrderKey,
        sub_orders: &[Order],
    ) -> Vec<NonZeroU32> {
        match key {
            OrderKey::Serial => {
                self.sort_with_triee_desc(rows, &self.serial.read().unwrap(), &vec![])
            }
            OrderKey::Row => rows.iter().rev().copied().collect(),
            OrderKey::TermBegin => self.term_begin.as_ref().map_or_else(
                || rows.iter().rev().copied().collect(),
                |f| self.sort_with_triee_desc(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::TermEnd => self.term_end.as_ref().map_or_else(
                || rows.iter().rev().copied().collect(),
                |f| self.sort_with_triee_desc(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::LastUpdated => self.last_updated.as_ref().map_or_else(
                || rows.iter().rev().copied().collect(),
                |f| self.sort_with_triee_desc(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::Field(field_name) => self.field(&field_name).map_or_else(
                || rows.iter().rev().copied().collect(),
                |f| self.sort_with_triee_desc(rows, &f.read().unwrap(), sub_orders),
            ),
            OrderKey::Custom(custom_order) => custom_order.desc(),
        }
    }
}
