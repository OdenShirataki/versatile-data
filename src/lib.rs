pub mod search;

mod field;
mod operation;
mod option;
mod row_fragment;
mod serial;
mod sort;

use async_recursion::async_recursion;
pub use field::Field;
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
    sync::{Arc, RwLock},
    thread,
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
    serial: SerialNumber,
    uuid: Option<IdxFile<u128>>,
    activity: Option<Arc<RwLock<IdxFile<u8>>>>,
    term_begin: Option<Arc<RwLock<IdxFile<u64>>>>,
    term_end: Option<Arc<RwLock<IdxFile<u64>>>>,
    last_updated: Option<Arc<RwLock<IdxFile<u64>>>>,
    fields_cache: HashMap<String, Arc<RwLock<Field>>>,
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
                        let field = Field::new(d.path());
                        fields_cache
                            .entry(String::from(fname))
                            .or_insert(Arc::new(RwLock::new(field)));
                    }
                }
            }
        }

        Self {
            fields_dir,
            serial: SerialNumber::new({
                let mut path = dir.to_path_buf();
                path.push("serial");
                path
            }),
            uuid: option.uuid.then_some(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("uuid.i");
                path
            })),
            activity: option
                .activity
                .then_some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("activity.i");
                    path
                })))),
            term_begin: option.term.then_some(Arc::new(RwLock::new(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("term_begin.i");
                path
            })))),
            term_end: option.term.then_some(Arc::new(RwLock::new(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("term_end.i");
                path
            })))),
            last_updated: option
                .last_updated
                .then_some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("last_updated.i");
                    path
                })))),
            fields_cache,
        }
    }

    #[inline(always)]
    pub fn exists(&self, row: NonZeroU32) -> bool {
        self.serial.value(row).is_some()
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
            a.read().unwrap().value(row).map(|v| {
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
        self.term_begin
            .as_ref()
            .and_then(|f| f.read().unwrap().value(row).copied())
    }

    #[inline(always)]
    pub fn term_end(&self, row: NonZeroU32) -> Option<u64> {
        self.term_end
            .as_ref()
            .and_then(|f| f.read().unwrap().value(row).copied())
    }

    #[inline(always)]
    pub fn last_updated(&self, row: NonZeroU32) -> Option<u64> {
        self.last_updated
            .as_ref()
            .and_then(|f| f.read().unwrap().value(row).copied())
    }

    #[async_recursion]
    pub async fn update(&mut self, operation: &Operation) -> u32 {
        match operation {
            Operation::New(record) => self.create_row(record).await.get(),
            Operation::Update { row, record } => {
                self.update_row(NonZeroU32::new(*row).unwrap(), record)
                    .await;
                *row
            }
            Operation::Delete { row } => {
                self.delete(NonZeroU32::new(*row).unwrap());
                0
            }
        }
    }

    pub async fn update_field(&mut self, row: NonZeroU32, field_name: &str, cont: &[u8]) {
        let field = if self.fields_cache.contains_key(field_name) {
            self.fields_cache.get_mut(field_name).unwrap()
        } else {
            self.create_field(field_name)
        };
        field.write().unwrap().update(row, cont).await;
    }

    pub async fn create_row(&mut self, record: &Record) -> NonZeroU32 {
        let row = self.serial.next_row().await;

        if let Some(ref mut uuid) = self.uuid {
            uuid.update(row, create_uuid()).await; //recycled serial_number,uuid recreate.
        }

        self.update_common(row, record).await;

        row
    }

    pub async fn update_row(&mut self, row: NonZeroU32, record: &Record) {
        if self.exists(row) {
            self.update_common(row, record).await;
        }
    }

    #[inline(always)]
    fn field(&self, name: &str) -> Option<&Arc<RwLock<Field>>> {
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
                            let field = Field::new(path);
                            self.fields_cache
                                .entry(String::from(str_fname))
                                .or_insert(Arc::new(RwLock::new(field)));
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

    async fn update_common(&mut self, row: NonZeroU32, record: &Record) {
        if let Some(ref f) = self.last_updated {
            let f = Arc::clone(f);
            thread::spawn(move || {
                futures::executor::block_on(async {
                    f.write().unwrap().update(row, Self::now()).await
                });
            });
        }

        for kv in record.fields.iter() {
            let field = if self.fields_cache.contains_key(&kv.key) {
                self.fields_cache.get_mut(&kv.key).unwrap()
            } else {
                self.create_field(&kv.key)
            };
            let field = Arc::clone(field);
            let kv = kv.clone();
            thread::spawn(move || {
                futures::executor::block_on(async {
                    field.write().unwrap().update(row, &kv.value).await;
                });
            });
        }

        if let Some(ref f) = self.activity {
            let f = Arc::clone(f);
            let activity = record.activity as u8;
            thread::spawn(move || {
                futures::executor::block_on(async {
                    f.write().unwrap().update(row, activity).await;
                })
            });
        }
        if let Some(ref f) = self.term_begin {
            let f = Arc::clone(f);
            let term_begin = record.term_begin.clone();
            thread::spawn(move || {
                futures::executor::block_on(async {
                    f.write()
                        .unwrap()
                        .update(
                            row,
                            if let Term::Overwrite(term) = term_begin {
                                term
                            } else {
                                Self::now()
                            },
                        )
                        .await;
                })
            });
        }
        if let Some(ref f) = self.term_end {
            let f = Arc::clone(f);
            let term_end = record.term_end.clone();
            thread::spawn(move || {
                futures::executor::block_on(async {
                    f.write()
                        .unwrap()
                        .update(
                            row,
                            if let Term::Overwrite(term) = term_end {
                                term
                            } else {
                                0
                            },
                        )
                        .await;
                });
            });
        }
    }

    #[inline(always)]
    fn delete(&mut self, row: NonZeroU32) {
        if self.exists(row) {
            self.serial.delete(row);

            self.load_fields();
            for (_, v) in self.fields_cache.iter() {
                let v = Arc::clone(v);
                thread::spawn(move || {
                    v.write().unwrap().delete(row);
                });
            }

            if let Some(ref mut f) = self.uuid {
                f.delete(row);
            }
            if let Some(ref f) = self.activity {
                let f = Arc::clone(f);
                thread::spawn(move || {
                    f.write().unwrap().delete(row);
                });
            }
            if let Some(ref f) = self.term_begin {
                let f = Arc::clone(f);
                thread::spawn(move || {
                    f.write().unwrap().delete(row);
                });
            }
            if let Some(ref f) = self.term_end {
                let f = Arc::clone(f);
                thread::spawn(move || {
                    f.write().unwrap().delete(row);
                });
            }
            if let Some(ref f) = self.last_updated {
                let f = Arc::clone(f);
                thread::spawn(move || {
                    f.write().unwrap().delete(row);
                });
            }
        }
    }

    #[inline(always)]
    pub fn all(&self) -> RowSet {
        self.serial.iter().collect()
    }
}
