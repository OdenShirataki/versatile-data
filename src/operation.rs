use std::num::NonZeroU32;

use futures::FutureExt;
use hashbrown::HashMap;
use uuid::Uuid;

use crate::Data;

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
    Update { row: NonZeroU32, record: Record },
    Delete { row: NonZeroU32 },
}

pub fn create_uuid() -> u128 {
    Uuid::new_v4().as_u128()
}

impl Data {
    /// Perform operations to register, update, and delete data.
    pub async fn update(&mut self, operation: Operation) -> Option<NonZeroU32> {
        match operation {
            Operation::New(record) => {
                let row = self.serial.next_row();
                self.update_field(row, record, true).await;
                Some(row)
            }
            Operation::Update { row, record } => {
                self.update_field(row, record, false).await;
                Some(row)
            }
            Operation::Delete { row } => {
                self.delete(row).await;
                None
            }
        }
    }

    async fn delete(&mut self, row: NonZeroU32) {
        self.load_fields();

        futures::future::join(
            futures::future::join(async { self.serial.delete(row) }, async {
                futures::future::join_all(self.fields_cache.iter_mut().map(|(_, v)| async {
                    v.delete(row);
                }))
                .await
            }),
            async {
                let mut futs = vec![];
                if let Some(ref mut f) = self.uuid {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed_local(),
                    );
                }
                if let Some(ref mut f) = self.activity {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed_local(),
                    );
                }
                if let Some(ref mut f) = self.term_begin {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed_local(),
                    );
                }
                if let Some(ref mut f) = self.term_end {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed_local(),
                    );
                }
                if let Some(ref mut f) = self.last_updated {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed_local(),
                    );
                }
                futures::future::join_all(futs).await;
            },
        )
        .await;
    }

    async fn update_field(&mut self, row: NonZeroU32, record: Record, with_uuid: bool) {
        for (key, _) in &record.fields {
            if !self.fields_cache.contains_key(key) {
                self.create_field(key);
            }
        }

        futures::future::join_all([
            async {
                futures::future::join_all(self.fields_cache.iter_mut().filter_map(
                    |(key, field)| {
                        record
                            .fields
                            .get(key)
                            .map(|v| async { field.update(row, v) })
                    },
                ))
                .await;
            }
            .boxed_local(),
            async {
                if with_uuid {
                    if let Some(ref mut uuid) = self.uuid {
                        uuid.update(row, Uuid::new_v4().as_u128());
                    }
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.last_updated {
                    f.update(row, Self::now());
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.activity {
                    f.update(row, record.activity as u8);
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.term_begin {
                    f.update(
                        row,
                        if let Term::Overwrite(term) = record.term_begin {
                            term
                        } else {
                            Self::now()
                        },
                    );
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.term_end {
                    f.update(
                        row,
                        if let Term::Overwrite(term) = record.term_end {
                            term
                        } else {
                            0
                        },
                    );
                }
            }
            .boxed_local(),
        ])
        .await;
    }
}
