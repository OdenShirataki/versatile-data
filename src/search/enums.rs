use crate::Activity;
use std::{
    ops::RangeInclusive,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug)]
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
