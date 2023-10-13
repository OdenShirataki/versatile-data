use hashbrown::HashMap;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Activity {
    Inactive = 0,
    Active = 1,
}
impl Default for Activity {
    fn default() -> Self {
        Self::Active
    }
}

#[derive(Debug, Clone)]
pub enum Term {
    Default,
    Overwrite(u64),
}
impl Default for Term {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Default, Debug)]
pub struct Record {
    pub activity: Activity,
    pub term_begin: Term,
    pub term_end: Term,
    pub fields: HashMap<String, Vec<u8>>,
}

pub enum Operation {
    New(Record),
    Update { row: u32, record: Record },
    Delete { row: u32 },
}
