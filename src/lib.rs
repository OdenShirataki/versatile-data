pub mod search;

mod field;
mod operation;
mod option;
mod row_fragment;
mod serial;
mod sort;

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
    ops::Deref,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use hashbrown::HashMap;
use serial::SerialNumber;

pub type RowSet = BTreeSet<NonZeroU32>;

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
    /// Opens the file and creates the Data.
    pub fn new<P: AsRef<Path>>(dir: P, option: DataOption) -> Self {
        let dir = dir.as_ref();
        if !dir.exists() {
            fs::create_dir_all(dir).unwrap();
        }

        let mut fields_cache = HashMap::new();
        let mut fields_dir = dir.to_path_buf();
        fields_dir.push("fields");
        if fields_dir.exists() {
            for d in fields_dir.read_dir().unwrap().into_iter() {
                let d = d.unwrap();
                if d.file_type().unwrap().is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let field = Field::new(d.path(), option.allocation_lot);
                        fields_cache.entry(fname.into()).or_insert(field);
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

    /// Returns a serial number.The serial number is incremented each time data is added.
    pub fn serial(&self, row: NonZeroU32) -> u32 {
        *unsafe { self.serial.get_unchecked(row) }.deref()
    }

    /// Returns a UUID.UUID is a unique ID that is automatically generated when data is registered..
    pub fn uuid(&self, row: NonZeroU32) -> Option<u128> {
        self.uuid
            .as_ref()
            .and_then(|uuid| uuid.get(row).map(|node| *node.deref()))
    }

    /// Returns the UUID as a string.
    pub fn uuid_string(&self, row: NonZeroU32) -> Option<String> {
        self.uuid.as_ref().and_then(|uuid| {
            uuid.get(row)
                .map(|v| uuid::Uuid::from_u128(*v.deref()).to_string())
        })
    }

    /// Returns the activity value. activity is used to indicate whether data is valid or invalid.
    pub fn activity(&self, row: NonZeroU32) -> Option<Activity> {
        self.activity.as_ref().and_then(|a| {
            a.get(row).map(|v| {
                if *v.deref() != 0 {
                    Activity::Active
                } else {
                    Activity::Inactive
                }
            })
        })
    }

    /// Returns the start date and time of the data's validity period.
    pub fn term_begin(&self, row: NonZeroU32) -> Option<u64> {
        self.term_begin
            .as_ref()
            .and_then(|f| f.get(row).map(|v| *v.deref()))
    }

    /// Returns the end date and time of the data's validity period.
    pub fn term_end(&self, row: NonZeroU32) -> Option<u64> {
        self.term_end
            .as_ref()
            .and_then(|f| f.get(row).map(|v| *v.deref()))
    }

    /// Returns the date and time when the data was last updated.
    pub fn last_updated(&self, row: NonZeroU32) -> Option<u64> {
        self.last_updated
            .as_ref()
            .and_then(|f| f.get(row).map(|v| *v.deref()))
    }

    /// Returns all rows.
    pub fn all(&self) -> RowSet {
        self.serial.iter().collect()
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.fields_cache.get(name)
    }

    fn load_fields(&mut self) {
        if self.fields_dir.exists() {
            for p in self.fields_dir.read_dir().unwrap().into_iter() {
                let p = p.unwrap();
                let path = p.path();
                if path.is_dir() {
                    if let Some(str_fname) = p.file_name().to_str() {
                        if !self.fields_cache.contains_key(str_fname) {
                            let field = Field::new(path, self.option.allocation_lot);
                            self.fields_cache.entry(str_fname.into()).or_insert(field);
                        }
                    }
                }
            }
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}
