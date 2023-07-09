use crate::Activity;
use std::{ops::RangeInclusive, sync::Arc};

#[derive(Clone, Debug)]
pub enum Term {
    In(u64),
    Past(u64),
    Future(u64),
}

#[derive(Clone, Debug)]
pub enum Number {
    Min(isize),
    Max(isize),
    Range(RangeInclusive<isize>),
    In(Vec<isize>),
}

#[derive(Clone, Debug)]
pub enum Field {
    Match(Arc<Vec<u8>>),
    Range(Arc<Vec<u8>>,Arc< Vec<u8>>),
    Min(Arc<Vec<u8>>),
    Max(Arc<Vec<u8>>),
    Forward(Arc<String>),
    Partial(Arc<String>),
    Backward(Arc<String>),
    ValueForward(Arc<String>),
    ValueBackward(Arc<String>),
    ValuePartial(Arc<String>),
}

#[derive(Clone, Debug)]
pub enum Condition {
    Activity(Activity),
    Term(Term),
    Row(Number),
    Uuid(Vec<u128>),
    LastUpdated(Number),
    Field(String, Field),
    Narrow(Vec<Condition>),
    Wide(Vec<Condition>),
}

#[derive(Debug)]
pub enum OrderKey {
    Serial,
    Row,
    TermBegin,
    TermEnd,
    LastUpdated,
    Field(String),
}

#[derive(Debug)]
pub enum Order {
    Asc(OrderKey),
    Desc(OrderKey),
}
