use crate::{Activity, FieldName};
use std::{
    ops::RangeInclusive,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Term {
    In(u64),
    Past(u64),
    Future(u64),
}

impl Default for Term {
    fn default() -> Self {
        Self::In(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Number {
    Min(isize),
    Max(isize),
    Range(RangeInclusive<isize>),
    In(Vec<isize>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    Match(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
    Min(Vec<u8>),
    Max(Vec<u8>),
    Forward(String),
    Partial(String),
    Backward(String),
    ValueForward(String),
    ValueBackward(String),
    ValuePartial(String),
}

#[derive(Debug)]
pub enum Condition<'a> {
    Activity(Activity),
    Term(Term),
    Row(&'a Number),
    Uuid(&'a [u128]),
    LastUpdated(&'a Number),
    Field(FieldName, &'a Field),
    Narrow(&'a Vec<Condition<'a>>),
    Wide(&'a Vec<Condition<'a>>),
}
