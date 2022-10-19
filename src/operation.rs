#[derive(Clone)]
pub struct KeyValue{
    pub(super) key:String
    ,pub(super) value:Vec<u8>
}
impl KeyValue{
    pub fn new(key:impl Into<String>,value:impl Into<Vec<u8>>)->KeyValue{
        KeyValue{
            key:key.into()
            ,value:value.into()
        }
    }
    pub fn key(&self)->&str{
        &self.key
    }
    pub fn value(&self)->&[u8]{
        &self.value
    }
}

#[derive(Clone,Copy,PartialEq,Debug)]
pub enum Activity{
    Inactive=0
    ,Active=1
}

#[derive(Clone,Copy)]
pub enum Term{
    Defalut
    ,Overwrite(i64)
}

#[derive(Clone)]
pub enum Operation{
    New{
        activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue>
    }
    ,Update{
        row:u32
        ,activity:Activity
        ,term_begin:Term
        ,term_end:Term
        ,fields:Vec<KeyValue>}
    ,Delete{row:u32}
}