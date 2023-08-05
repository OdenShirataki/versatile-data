mod enums;
mod result;

use std::time::{SystemTime, UNIX_EPOCH};

use super::{Activity, Data};

pub use enums::*;

pub struct Search<'a> {
    data: &'a Data,
    conditions: Vec<Condition>,
}
impl<'a> Search<'a> {
    pub fn new(data: &'a Data) -> Self {
        Search {
            data,
            conditions: Vec::new(),
        }
    }
    pub fn search_default(mut self) -> Result<Self, std::time::SystemTimeError> {
        if let Some(_) = self.data.term_begin {
            self.conditions.push(Condition::Term(Term::In(
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            )));
        }
        if let Some(_) = self.data.activity {
            self.conditions.push(Condition::Activity(Activity::Active));
        }
        Ok(self)
    }
    pub fn search_field(self, field_name: impl Into<String>, condition: Field) -> Self {
        self.search(Condition::Field(field_name.into(), condition))
    }
    pub fn search_term(self, condition: Term) -> Self {
        if let Some(_) = self.data.term_begin {
            self.search(Condition::Term(condition))
        } else {
            self
        }
    }
    pub fn search_activity(self, condition: Activity) -> Self {
        if let Some(_) = self.data.term_begin {
            self.search(Condition::Activity(condition))
        } else {
            self
        }
    }
    pub fn search_row(self, condition: Number) -> Self {
        self.search(Condition::Row(condition))
    }

    pub fn search(mut self, condition: Condition) -> Self {
        self.conditions.push(condition);
        self
    }
}
