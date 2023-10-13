pub mod search;

mod field;
mod operation;
mod option;
mod row_fragment;
mod serial;
mod sort;

pub use field::Field;
use futures::FutureExt;
pub use idx_binary::{self, AvltrieeIter, FileMmap, IdxBinary, IdxFile};
pub use operation::*;
pub use option::DataOption;
pub use row_fragment::RowFragment;
pub use search::{Condition, Search};
pub use sort::{CustomSort, Order, OrderKey};
pub use uuid::Uuid;

use std::{
    collections::BTreeSet,
    fs,
    num::NonZeroU32,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use hashbrown::HashMap;
use serial::SerialNumber;

pub type RowSet = BTreeSet<NonZeroU32>;

pub fn create_uuid() -> u128 {
    Uuid::new_v4().as_u128()
}
pub fn uuid_string(uuid: u128) -> String {
    Uuid::from_u128(uuid).to_string()
}

pub struct Data {
    fields_dir: PathBuf,
    option: DataOption,
    serial: SerialNumber,
    uuid: Option<IdxFile<u128>>,
    activity: Option<IdxFile<u8>>,
    term_begin: Option<IdxFile<u64>>,
    term_end: Option<IdxFile<u64>>,
    last_updated: Option<IdxFile<u64>>,
    fields_cache: HashMap<String, Field>,
}

impl Data {
    pub fn new<P: AsRef<Path>>(dir: P, option: DataOption) -> Self {
        let dir = dir.as_ref();
        if !dir.exists() {
            fs::create_dir_all(dir).unwrap();
        }

        let mut fields_cache = HashMap::new();
        let mut fields_dir = dir.to_path_buf();
        fields_dir.push("fields");
        if fields_dir.exists() {
            for d in fields_dir.read_dir().unwrap() {
                let d = d.unwrap();
                if d.file_type().unwrap().is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let field = Field::new(d.path(), option.allocation_lot);
                        fields_cache.entry(String::from(fname)).or_insert(field);
                    }
                }
            }
        }

        let serial = SerialNumber::new(
            {
                let mut path = dir.to_path_buf();
                path.push("serial");
                path
            },
            option.allocation_lot,
        );
        let uuid = option.uuid.then_some(IdxFile::new(
            {
                let mut path = dir.to_path_buf();
                path.push("uuid.i");
                path
            },
            option.allocation_lot,
        ));
        let activity = option.activity.then_some(IdxFile::new(
            {
                let mut path = dir.to_path_buf();
                path.push("activity.i");
                path
            },
            option.allocation_lot,
        ));
        let term_begin = option.term.then_some(IdxFile::new(
            {
                let mut path = dir.to_path_buf();
                path.push("term_begin.i");
                path
            },
            option.allocation_lot,
        ));
        let term_end = option.term.then_some(IdxFile::new(
            {
                let mut path = dir.to_path_buf();
                path.push("term_end.i");
                path
            },
            option.allocation_lot,
        ));
        let last_updated = option.last_updated.then_some(IdxFile::new(
            {
                let mut path = dir.to_path_buf();
                path.push("last_updated.i");
                path
            },
            option.allocation_lot,
        ));

        Self {
            fields_dir,
            option,
            serial,
            uuid,
            activity,
            term_begin,
            term_end,
            last_updated,
            fields_cache,
        }
    }

    #[inline(always)]
    pub fn serial(&self, row: NonZeroU32) -> u32 {
        self.serial.value(row).copied().unwrap()
    }

    #[inline(always)]
    pub fn uuid(&self, row: NonZeroU32) -> Option<u128> {
        self.uuid.as_ref().and_then(|uuid| uuid.value(row).copied())
    }

    #[inline(always)]
    pub fn uuid_string(&self, row: NonZeroU32) -> Option<String> {
        self.uuid.as_ref().and_then(|uuid| {
            uuid.value(row)
                .map(|v| uuid::Uuid::from_u128(*v).to_string())
        })
    }

    #[inline(always)]
    pub fn activity(&self, row: NonZeroU32) -> Option<Activity> {
        self.activity.as_ref().and_then(|a| {
            a.value(row).map(|v| {
                if *v != 0 {
                    Activity::Active
                } else {
                    Activity::Inactive
                }
            })
        })
    }

    #[inline(always)]
    pub fn term_begin(&self, row: NonZeroU32) -> Option<u64> {
        self.term_begin.as_ref().and_then(|f| f.value(row).copied())
    }

    #[inline(always)]
    pub fn term_end(&self, row: NonZeroU32) -> Option<u64> {
        self.term_end.as_ref().and_then(|f| f.value(row).copied())
    }

    #[inline(always)]
    pub fn last_updated(&self, row: NonZeroU32) -> Option<u64> {
        if let Some(last_update) = &self.last_updated {
            last_update.value(row).copied()
        } else {
            None
        }
    }

    pub async fn update(&mut self, operation: Operation) -> u32 {
        match operation {
            Operation::New(record) => self.create_row(record).await.get(),
            Operation::Update { row, record } => {
                self.update_field(NonZeroU32::new(row).unwrap(), record, false)
                    .await;
                row
            }
            Operation::Delete { row } => {
                self.delete(NonZeroU32::new(row).unwrap()).await;
                0
            }
        }
    }

    pub async fn create_row(&mut self, record: Record) -> NonZeroU32 {
        let row = self.serial.next_row().await;
        self.update_field(row, record, true).await;
        row
    }

    pub async fn update_row(&mut self, row: NonZeroU32, record: Record) {
        self.update_field(row, record, false).await;
    }

    #[inline(always)]
    fn field(&self, name: &str) -> Option<&Field> {
        self.fields_cache.get(name)
    }
    fn load_fields(&mut self) {
        if self.fields_dir.exists() {
            for p in self.fields_dir.read_dir().unwrap() {
                let p = p.unwrap();
                let path = p.path();
                if path.is_dir() {
                    if let Some(str_fname) = p.file_name().to_str() {
                        if !self.fields_cache.contains_key(str_fname) {
                            let field = Field::new(path, self.option.allocation_lot);
                            self.fields_cache
                                .entry(String::from(str_fname))
                                .or_insert(field);
                        }
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
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
                        if let Some(v) = record.fields.get(key) {
                            Some(field.update(row, v))
                        } else {
                            None
                        }
                    },
                ))
                .await;
            }
            .boxed(),
            async {
                if with_uuid {
                    if let Some(ref mut uuid) = self.uuid {
                        uuid.update_with_allocate(row, create_uuid()).await;
                    }
                }
            }
            .boxed(),
            async {
                if let Some(ref mut f) = self.last_updated {
                    f.update_with_allocate(row, Self::now()).await;
                }
            }
            .boxed(),
            async {
                if let Some(ref mut f) = self.activity {
                    f.update_with_allocate(row, record.activity as u8).await;
                }
            }
            .boxed(),
            async {
                if let Some(ref mut f) = self.term_begin {
                    f.update_with_allocate(
                        row,
                        if let Term::Overwrite(term) = record.term_begin {
                            term
                        } else {
                            Self::now()
                        },
                    )
                    .await;
                }
            }
            .boxed(),
            async {
                if let Some(ref mut f) = self.term_end {
                    f.update_with_allocate(
                        row,
                        if let Term::Overwrite(term) = record.term_end {
                            term
                        } else {
                            0
                        },
                    )
                    .await;
                }
            }
            .boxed(),
        ])
        .await;
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
                        .boxed(),
                    );
                }
                if let Some(ref mut f) = self.activity {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed(),
                    );
                }
                if let Some(ref mut f) = self.term_begin {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed(),
                    );
                }
                if let Some(ref mut f) = self.term_end {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed(),
                    );
                }
                if let Some(ref mut f) = self.last_updated {
                    futs.push(
                        async {
                            f.delete(row);
                        }
                        .boxed(),
                    );
                }
                futures::future::join_all(futs).await;
            },
        )
        .await;
    }

    #[inline(always)]
    pub fn all(&self) -> RowSet {
        self.serial.iter().collect()
    }
}
