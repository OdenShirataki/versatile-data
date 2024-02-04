use std::num::NonZeroU32;

use futures::FutureExt;
use hashbrown::HashMap;
use uuid::Uuid;

use crate::{Data, FieldName};

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

pub fn create_uuid() -> u128 {
    Uuid::new_v4().as_u128()
}

impl Data {
    /// Delete row.
    pub async fn insert(
        &mut self,
        activity: Activity,
        term_begin: Term,
        term_end: Term,
        fields: HashMap<FieldName, Vec<u8>>,
    ) -> NonZeroU32 {
        let row = self.serial.next_row();
        self.update(row, activity, term_begin, term_end, fields)
            .await;
        row
    }

    /// Update row.
    pub async fn update(
        &mut self,
        row: NonZeroU32,
        activity: Activity,
        term_begin: Term,
        term_end: Term,
        fields: HashMap<FieldName, Vec<u8>>,
    ) {
        for (key, _) in &fields {
            if !self.fields.contains_key(key) {
                self.create_field(key);
            }
        }
        futures::future::join_all([
            async {
                futures::future::join_all(self.fields.iter_mut().filter_map(|(name, field)| {
                    fields
                        .get(&name.clone())
                        .map(|v| async { field.update(row, v) })
                }))
                .await;
            }
            .boxed_local(),
            async {
                if let Some(ref mut uuid) = self.uuid {
                    if uuid.get(row).is_none() {
                        uuid.update(row, &Uuid::new_v4().as_u128());
                    }
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.last_updated {
                    f.update(row, &Self::now());
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.activity {
                    f.update(row, &(activity as u8));
                }
            }
            .boxed_local(),
            async {
                if let Some(ref mut f) = self.term_begin {
                    f.update(
                        row,
                        &if let Term::Overwrite(term) = term_begin {
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
                        &if let Term::Overwrite(term) = term_end {
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

    /// Delete row.
    pub async fn delete(&mut self, row: NonZeroU32) {
        futures::future::join(
            futures::future::join(async { self.serial.delete(row) }, async {
                futures::future::join_all(self.fields.iter_mut().map(|(_name, v)| async {
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
}
