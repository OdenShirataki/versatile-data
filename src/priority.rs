use std::cmp::Ordering;

#[derive(Default,Debug,Clone,Copy)]
pub struct Priority{
    priority:f64
}
impl Priority{
    pub fn new(priority:f64)->Priority{
        Priority{priority}
    }
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