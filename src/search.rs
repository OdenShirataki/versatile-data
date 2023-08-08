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
    pub fn search_default(mut self) -> Self {
        if let Some(_) = self.data.term_begin {
            self.conditions.push(Condition::Term(Term::In(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            )));
        }
        if let Some(_) = self.data.activity {
            self.conditions.push(Condition::Activity(Activity::Active));
        }
        self
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

impl Data {
    pub fn begin_search(&self) -> Search {
        Search::new(self)
    }
    pub fn search_field(&self, field_name: impl Into<String>, condition: Field) -> Search {
        Search::new(self).search_field(field_name, condition)
    }
    pub fn search_activity(&self, condition: Activity) -> Search {
        Search::new(self).search_activity(condition)
    }
    pub fn search_term(&self, condition: Term) -> Search {
        Search::new(self).search_term(condition)
    }
    pub fn search_row(&self, condition: Number) -> Search {
        Search::new(self).search_row(condition)
    }
    pub fn search_default(&self) -> Search {
        Search::new(self).search_default()
    }
}
