mod enums;
mod result;

use super::{Activity, Data};

pub use enums::*;

pub struct Search<'a> {
    data: &'a Data,
    conditions: Vec<Condition<'a>>,
}
impl<'a> Search<'a> {
    fn new(data: &'a Data) -> Self {
        Search {
            data,
            conditions: Vec::new(),
        }
    }

    /// Searches for data whose term is greater than or equal to the current date and time and is active.
    pub fn search_default(mut self) -> Self {
        if self.data.term_begin.is_some() {
            self.conditions.push(Condition::Term(Term::default()));
        }
        if self.data.activity.is_some() {
            self.conditions.push(Condition::Activity(Activity::Active));
        }
        self
    }

    /// Search field.
    pub fn search_field(self, field_name: &'a str, condition: &'a Field) -> Self {
        self.search(Condition::Field(field_name, condition))
    }

    /// Search by data publication period.
    pub fn search_term(self, condition: Term) -> Self {
        if self.data.term_begin.is_some() {
            self.search(Condition::Term(condition))
        } else {
            self
        }
    }

    /// Search by data activity.
    pub fn search_activity(self, condition: Activity) -> Self {
        if self.data.term_begin.is_some() {
            self.search(Condition::Activity(condition))
        } else {
            self
        }
    }

    /// Search by row.
    pub fn search_row(self, condition: &'a Number) -> Self {
        self.search(Condition::Row(condition))
    }

    /// Search with any condition.
    pub fn search(mut self, condition: Condition<'a>) -> Self {
        self.conditions.push(condition);
        self
    }
}

impl Data {
    /// Create a new [Search] object.
    pub fn begin_search(&self) -> Search {
        Search::new(self)
    }

    /// Create a [Search] object with the field search set.
    pub fn search_field<'a>(&'a self, field_name: &'a str, condition: &'a Field) -> Search {
        Search::new(self).search_field(field_name, condition)
    }

    /// Create a [Search] object with the activity search set.
    pub fn search_activity<'a>(&'a self, condition: Activity) -> Search {
        Search::new(self).search_activity(condition)
    }

    /// Create a [Search] object with the term search set.
    pub fn search_term<'a>(&'a self, condition: Term) -> Search {
        Search::new(self).search_term(condition)
    }

    /// Create a [Search] object with the row search set.
    pub fn search_row<'a>(&'a self, condition: &'a Number) -> Search {
        Search::new(self).search_row(condition)
    }

    /// Creates a [Search] object with a default search set. Searches for data whose term is greater than or equal to the current date and time and is active.
    pub fn search_default(&self) -> Search {
        Search::new(self).search_default()
    }
}
