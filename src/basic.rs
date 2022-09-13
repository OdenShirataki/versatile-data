use std::cmp::Ordering;
use std::fmt;

#[derive(Default,Debug,Clone,Copy)]
pub struct Priority{
    priority:f64
}
impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Priority) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Priority{
    fn cmp(&self,other:&Priority)->Ordering{
        if self.priority==other.priority{
            Ordering::Equal
        }else if self.priority>other.priority{
            Ordering::Greater
        }else{
            Ordering::Less
        }
    }
}
impl PartialEq for Priority {
    fn eq(&self, other: &Priority) -> bool {
        self.priority == other.priority
    }
}
impl Eq for Priority {}

impl Into<f64> for Priority {
    fn into(self) -> f64 {
        self.priority
    }
}

#[derive(Clone,Copy)]
pub struct BasicData{
    activity: u8
    ,priority: Priority
    ,term_begin: i64
    ,term_end: i64
    ,last_updated: i64
    ,uuid:u128
}
impl fmt::Debug for BasicData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f
            ,"{{ activity:{} , priority:{} , term_begin:{} , term_end:{} , last_updated:{} ,uuid:{} }}"
            ,self.activity
            ,self.priority.priority
            ,self.term_begin
            ,self.term_end
            ,self.last_updated
            ,self.uuid
        )
    }
}
impl BasicData{
    pub fn new(
        activity: u8
        ,priority: f64
        ,term_begin: i64
        ,term_end: i64
        ,last_updated: i64
        ,uuid:u128
    )->BasicData{
        BasicData{
            activity
            ,priority:Priority{priority}
            ,term_begin
            ,term_end
            ,last_updated
            ,uuid
        }
    }
    pub fn activity(&self)->u8{
        self.activity
    }
    pub fn priority(&self)->Priority{
        self.priority
    }
    pub fn term_begin(&self)->i64{
        self.term_begin
    }
    pub fn term_end(&self)->i64{
        self.term_end
    }
    pub fn last_updated(&self)->i64{
        self.last_updated
    }
    pub fn uuid(&self)->u128{
        self.uuid
    }
}