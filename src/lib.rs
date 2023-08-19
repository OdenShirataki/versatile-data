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

use serial::SerialNumber;
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

pub type RowSet = BTreeSet<u32>;

pub fn create_uuid() -> u128 {
    Uuid::new_v4().as_u128()
}
pub fn uuid_string(uuid: u128) -> String {
    Uuid::from_u128(uuid).to_string()
}

pub struct Data {
    fields_dir: PathBuf,
    serial: Arc<RwLock<SerialNumber>>,
    uuid: Option<Arc<RwLock<IdxFile<u128>>>>,
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
            serial: Arc::new(RwLock::new(SerialNumber::new({
                let mut path = dir.to_path_buf();
                path.push("serial");
                path
            }))),
            uuid: if option.uuid {
                Some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("uuid.i");
                    path
                }))))
            } else {
                None
            },
            activity: if option.activity {
                Some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("activity.i");
                    path
                }))))
            } else {
                None
            },
            term_begin: if option.term {
                Some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("term_begin.i");
                    path
                }))))
            } else {
                None
            },
            term_end: if option.term {
                Some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("term_end.i");
                    path
                }))))
            } else {
                None
            },
            last_updated: if option.last_updated {
                Some(Arc::new(RwLock::new(IdxFile::new({
                    let mut path = dir.to_path_buf();
                    path.push("last_updated.i");
                    path
                }))))
            } else {
                None
            },
            fields_cache,
        }
    }

    pub fn exists(&self, row: u32) -> bool {
        self.serial.read().unwrap().value(row) != None
    }

    pub fn serial(&self, row: u32) -> u32 {
        if let Some(v) = self.serial.read().unwrap().value(row) {
            *v
        } else {
            0
        }
    }
    pub fn uuid(&self, row: u32) -> Option<u128> {
        if let Some(ref uuid) = self.uuid {
            if let Some(v) = uuid.read().unwrap().value(row) {
                return Some(*v);
            }
        }
        None
    }
    pub fn uuid_string(&self, row: u32) -> Option<String> {
        if let Some(ref uuid) = self.uuid {
            if let Some(v) = uuid.read().unwrap().value(row) {
                return Some(uuid::Uuid::from_u128(*v).to_string());
            }
        }
        None
    }
    pub fn activity(&self, row: u32) -> Option<Activity> {
        if let Some(ref activity) = self.activity {
            if let Some(v) = activity.read().unwrap().value(row) {
                return Some(if *v != 0 {
                    Activity::Active
                } else {
                    Activity::Inactive
                });
            }
        }
        None
    }
    pub fn term_begin(&self, row: u32) -> Option<u64> {
        if let Some(ref f) = self.term_begin {
            if let Some(v) = f.read().unwrap().value(row) {
                return Some(*v);
            }
        }
        None
    }
    pub fn term_end(&self, row: u32) -> Option<u64> {
        if let Some(ref f) = self.term_end {
            if let Some(v) = f.read().unwrap().value(row) {
                return Some(*v);
            }
        }
        None
    }
    pub fn last_updated(&self, row: u32) -> Option<u64> {
        if let Some(ref f) = self.last_updated {
            if let Some(v) = f.read().unwrap().value(row) {
                return Some(*v);
            }
        }
        None
    }

    pub fn update(&mut self, operation: &Operation) -> u32 {
        match operation {
            Operation::New(r) => {
                self.create_row(&r.activity, &r.term_begin, &r.term_end, &r.fields)
            }
            Operation::Update { row, record } => {
                let row = *row;
                self.update_row(
                    row,
                    &record.activity,
                    &record.term_begin,
                    &record.term_end,
                    &record.fields,
                );
                row
            }
            Operation::Delete { row } => {
                self.delete(*row);
                0
            }
        }
    }

    pub fn update_field(&mut self, row: u32, field_name: &str, cont: &[u8]) {
        let field = if self.fields_cache.contains_key(field_name) {
            self.fields_cache.get_mut(field_name).unwrap()
        } else {
            self.create_field(field_name)
        };
        field.write().unwrap().update(row, cont);
    }

    pub fn create_row(
        &mut self,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> u32 {
        let row = self.serial.write().unwrap().next_row();

        if let Some(ref uuid) = self.uuid {
            uuid.write().unwrap().update(row, create_uuid()); //recycled serial_number,uuid recreate.
        }

        self.update_common(row, activity, term_begin, term_end, fields)
    }

    pub fn update_row(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) {
        if self.exists(row) {
            self.update_common(row, activity, term_begin, term_end, fields);
        }
    }

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

    fn update_common(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> u32 {
        if let Some(ref f) = self.last_updated {
            let f = Arc::clone(f);
            thread::spawn(move || {
                f.write().unwrap().update(
                    row,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                );
            });
        }

        for kv in fields.iter() {
            let field = if self.fields_cache.contains_key(&kv.key) {
                self.fields_cache.get_mut(&kv.key).unwrap()
            } else {
                self.create_field(&kv.key)
            };
            let field = Arc::clone(field);
            let kv = kv.clone();
            thread::spawn(move || {
                field.write().unwrap().update(row, &kv.value);
            });
        }

        if let Some(ref f) = self.activity {
            let f = Arc::clone(f);
            let activity = *activity as u8;
            thread::spawn(move || {
                f.write().unwrap().update(row, activity);
            });
        }
        if let Some(ref f) = self.term_begin {
            let f = Arc::clone(f);
            let term_begin = term_begin.clone();
            thread::spawn(move || {
                f.write().unwrap().update(
                    row,
                    if let Term::Overwrite(term) = term_begin {
                        term
                    } else {
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                    },
                );
            });
        }
        if let Some(ref f) = self.term_end {
            let f = Arc::clone(f);
            let term_end = term_end.clone();
            thread::spawn(move || {
                f.write().unwrap().update(
                    row,
                    if let Term::Overwrite(term) = term_end {
                        term
                    } else {
                        0
                    },
                );
            });
        }

        row
    }

    fn delete(&mut self, row: u32) {
        if self.exists(row) {
            let f = Arc::clone(&self.serial);
            thread::spawn(move || {
                f.write().unwrap().delete(row);
            });

            self.load_fields();
            for (_, v) in self.fields_cache.iter() {
                let v = Arc::clone(v);
                thread::spawn(move || {
                    v.write().unwrap().delete(row);
                });
            }

            if let Some(ref f) = self.uuid {
                let f = Arc::clone(f);
                thread::spawn(move || {
                    f.write().unwrap().delete(row);
                });
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

    pub fn all(&self) -> RowSet {
        self.serial
            .read()
            .unwrap()
            .iter()
            .map(|r| r.row())
            .collect()
    }
}
