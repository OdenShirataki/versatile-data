use std::ops::RangeInclusive;
use crate::Activity;

#[derive(Clone)]
pub enum Term{
    In(i64)
    ,Past(i64)
    ,Future(i64)
}

#[derive(Clone)]
pub enum Number{
    Min(isize)
    ,Max(isize)
    ,Range(RangeInclusive<isize>)
    ,In(Vec<isize>)
}

#[derive(Clone)]
pub enum Field{
    Match(Vec<u8>)
    ,Range(Vec<u8>,Vec<u8>)
    ,Min(Vec<u8>)
    ,Max(Vec<u8>)
    ,Forward(String)
    ,Partial(String)
    ,Backward(String)
}

#[derive(Clone)]
pub enum Condition{
    Activity(Activity)
    ,Term(Term)
    ,Row(Number)
    ,Uuid(u128)
    ,LastUpdated(Number)
    ,Field(String,Field)
    ,Narrow(Vec<Condition>)
    ,Wide(Vec<Condition>)
}

#[derive(Clone)]
pub enum OrderKey{
    Serial
    ,Row
    ,TermBegin
    ,TermEnd
    ,LastUpdated
    ,Field(String)
}

#[derive(Clone)]
pub enum Order{
    Asc(OrderKey)
    ,Desc(OrderKey)
}