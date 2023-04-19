use crate::Activity;
use std::ops::RangeInclusive;

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
    Match(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
    Min(Vec<u8>),
    Max(Vec<u8>),
    Forward(String),
    Partial(String),
    Backward(String),
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
