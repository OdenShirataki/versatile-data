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
                        let field = Field::new(d.path());
                        fields_cache.entry(String::from(fname)).or_insert(field);
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
            activity: option.activity.then_some(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("activity.i");
                path
            })),
            term_begin: option.term.then_some(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("term_begin.i");
                path
            })),
            term_end: option.term.then_some(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("term_end.i");
                path
            })),
            last_updated: option.last_updated.then_some(IdxFile::new({
                let mut path = dir.to_path_buf();
                path.push("last_updated.i");
                path
            })),
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
        field.update(row, cont).await;
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
                            let field = Field::new(path);
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

    async fn update_common(&mut self, row: NonZeroU32, record: &Record) {
        for kv in record.fields.iter() {
            if !self.fields_cache.contains_key(&kv.key) {
                self.create_field(&kv.key);
            }
        }

        let mut futs = vec![];
        if let Some(ref mut f) = self.last_updated {
            futs.push(f.update(row, Self::now()).boxed());
        }

        futs.push(
            async {
                for kv in record.fields.iter() {
                    if let Some(field) = self.fields_cache.get_mut(&kv.key) {
                        field.update(row, &kv.value).await;
                    }
                }
            }
            .boxed(),
        );

        if let Some(ref mut f) = self.activity {
            futs.push(f.update(row, record.activity as u8).boxed());
        }
        if let Some(ref mut f) = self.term_begin {
            futs.push(
                f.update(
                    row,
                    if let Term::Overwrite(term) = record.term_begin {
                        term
                    } else {
                        Self::now()
                    },
                )
                .boxed(),
            );
        }
        if let Some(ref mut f) = self.term_end {
            futs.push(
                f.update(
                    row,
                    if let Term::Overwrite(term) = record.term_end {
                        term
                    } else {
                        0
                    },
                )
                .boxed(),
            );
        }
        futures::future::join_all(futs).await;
    }

    #[inline(always)]
    fn delete(&mut self, row: NonZeroU32) {
        if self.exists(row) {
            self.serial.delete(row);

            self.load_fields();
            for (_, v) in self.fields_cache.iter_mut() {
                v.delete(row);
            }

            if let Some(ref mut f) = self.uuid {
                f.delete(row);
            }
            if let Some(ref mut f) = self.activity {
                f.delete(row);
            }
            if let Some(ref mut f) = self.term_begin {
                f.delete(row);
            }
            if let Some(ref mut f) = self.term_end {
                f.delete(row);
            }
            if let Some(ref mut f) = self.last_updated {
                f.delete(row);
            }
        }
    }

    #[inline(always)]
    pub fn all(&self) -> RowSet {
        self.serial.iter().collect()
    }
}
