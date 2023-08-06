use std::{cmp::Ordering, fmt::Debug};

use idx_binary::AvltrieeIter;

use crate::{Data, RowSet};

pub trait CustomSort {
    fn compare(&self, a: u32, b: u32) -> Ordering;
    fn asc(&self) -> Vec<u32>;
    fn desc(&self) -> Vec<u32>;
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
    pub fn sort(&self, rows: &RowSet, orders: &[Order]) -> Vec<u32> {
        let mut sub_orders = vec![];
        for i in 1..orders.len() {
            sub_orders.push(&orders[i]);
        }
        match &orders[0] {
            Order::Asc(key) => self.sort_with_key(rows, key, sub_orders),
            Order::Desc(key) => self.sort_with_key_desc(rows, key, sub_orders),
        }
    }
    fn subsort(&self, tmp: Vec<u32>, sub_orders: &[&Order]) -> Vec<u32> {
        let mut tmp = tmp;
        tmp.sort_by(|a, b| {
            for i in 0..sub_orders.len() {
                match sub_orders[i] {
                    Order::Asc(order_key) => match order_key {
                        OrderKey::Serial => {
                            return self
                                .serial
                                .read()
                                .unwrap()
                                .value(*a)
                                .unwrap()
                                .cmp(self.serial.read().unwrap().value(*b).unwrap());
                        }
                        OrderKey::Row => return a.cmp(b),
                        OrderKey::TermBegin => {
                            if let Some(ref f) = self.term_begin {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(*a)
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(*b).unwrap());
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
                                    .value(*a)
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(*b).unwrap());
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
                                    .value(*a)
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(*b).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::Field(field_name) => {
                            if let Some(field) = self.field(&field_name) {
                                let ord = idx_binary::compare(
                                    field.read().unwrap().bytes(*a).unwrap(),
                                    field.read().unwrap().bytes(*b).unwrap(),
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
                                .value(*b)
                                .unwrap()
                                .cmp(self.serial.read().unwrap().value(*a).unwrap());
                        }
                        OrderKey::Row => {
                            return b.cmp(a);
                        }
                        OrderKey::TermBegin => {
                            if let Some(ref f) = self.term_begin {
                                let ord = f
                                    .read()
                                    .unwrap()
                                    .value(*b)
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(*a).unwrap());
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
                                    .value(*b)
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(*a).unwrap());
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
                                    .value(*b)
                                    .unwrap()
                                    .cmp(f.read().unwrap().value(*a).unwrap());
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                        OrderKey::Field(field_name) => {
                            if let Some(field) = self.field(&field_name) {
                                let ord = idx_binary::compare(
                                    field.read().unwrap().bytes(*b).unwrap(),
                                    field.read().unwrap().bytes(*a).unwrap(),
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
    fn sort_with_iter<T>(
        &self,
        rows: &RowSet,
        iter: &mut AvltrieeIter<T>,
        sub_orders: Vec<&Order>,
    ) -> Vec<u32>
    where
        T: PartialEq,
    {
        let mut ret = Vec::new();
        if sub_orders.len() == 0 {
            for row in iter {
                let row = row.row();
                if rows.contains(&row) {
                    ret.push(row);
                }
            }
        } else {
            let mut before: Option<&T> = None;
            let mut tmp: Vec<u32> = Vec::new();
            for row in iter {
                let r = row.row();
                if rows.contains(&r) {
                    let value = row.value();
                    if let Some(before) = before {
                        if before.ne(value) {
                            if tmp.len() <= 1 {
                                ret.extend(tmp);
                            } else {
                                let tmp = self.subsort(tmp, &mut sub_orders.clone());
                                ret.extend(tmp);
                            }
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
            if tmp.len() <= 1 {
                ret.extend(tmp);
            } else {
                let tmp = self.subsort(tmp, &mut sub_orders.clone());
                ret.extend(tmp);
            }
        }
        ret
    }
    fn sort_with_key(&self, rows: &RowSet, key: &OrderKey, sub_orders: Vec<&Order>) -> Vec<u32> {
        match key {
            OrderKey::Serial => {
                self.sort_with_iter(rows, &mut self.serial.read().unwrap().iter(), vec![])
            }
            OrderKey::Row => rows.iter().map(|&x| x).collect(),
            OrderKey::TermBegin => {
                if let Some(ref f) = self.term_begin {
                    self.sort_with_iter(rows, &mut f.read().unwrap().iter(), sub_orders)
                } else {
                    rows.iter().map(|&x| x).collect()
                }
            }
            OrderKey::TermEnd => {
                if let Some(ref f) = self.term_end {
                    self.sort_with_iter(rows, &mut f.read().unwrap().iter(), sub_orders)
                } else {
                    rows.iter().map(|&x| x).collect()
                }
            }
            OrderKey::LastUpdated => {
                if let Some(ref f) = self.term_end {
                    self.sort_with_iter(rows, &mut f.read().unwrap().iter(), sub_orders)
                } else {
                    rows.iter().map(|&x| x).collect()
                }
            }
            OrderKey::Field(field_name) => {
                if let Some(field) = self.field(&field_name) {
                    self.sort_with_iter(rows, &mut field.read().unwrap().iter(), sub_orders)
                } else {
                    rows.iter().map(|&x| x).collect()
                }
            }
            OrderKey::Custom(custom_order) => custom_order.asc(),
        }
    }
    fn sort_with_key_desc(
        &self,
        rows: &RowSet,
        key: &OrderKey,
        sub_orders: Vec<&Order>,
    ) -> Vec<u32> {
        match key {
            OrderKey::Serial => {
                self.sort_with_iter(rows, &mut self.serial.read().unwrap().desc_iter(), vec![])
            }
            OrderKey::Row => rows.iter().rev().map(|&x| x).collect(),
            OrderKey::TermBegin => {
                if let Some(ref f) = self.term_begin {
                    self.sort_with_iter(rows, &mut f.read().unwrap().desc_iter(), sub_orders)
                } else {
                    rows.iter().rev().map(|&x| x).collect()
                }
            }
            OrderKey::TermEnd => {
                if let Some(ref f) = self.term_end {
                    self.sort_with_iter(rows, &mut f.read().unwrap().desc_iter(), sub_orders)
                } else {
                    rows.iter().rev().map(|&x| x).collect()
                }
            }
            OrderKey::LastUpdated => {
                if let Some(ref f) = self.last_updated {
                    self.sort_with_iter(rows, &mut f.read().unwrap().desc_iter(), sub_orders)
                } else {
                    rows.iter().rev().map(|&x| x).collect()
                }
            }
            OrderKey::Field(field_name) => {
                if let Some(field) = self.field(&field_name) {
                    self.sort_with_iter(rows, &mut field.read().unwrap().desc_iter(), sub_orders)
                } else {
                    rows.iter().rev().map(|&x| x).collect()
                }
            }
            OrderKey::Custom(custom_order) => custom_order.desc(),
        }
    }
}
