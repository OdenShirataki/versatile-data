use idx_sized::AvltrieeIter;
use std::{
    cmp::Ordering,
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

pub use idx_sized::{IdxSized, RowSet};

mod serial;
use serial::SerialNumber;

mod field;
pub use field::FieldData;

pub mod search;
pub use search::{Condition, Order, OrderKey, Search};

mod operation;
pub use operation::*;

pub use natord;

pub mod prelude;

pub struct Data {
    fields_dir: PathBuf,
    serial: Arc<RwLock<SerialNumber>>,
    uuid: Arc<RwLock<IdxSized<u128>>>,
    activity: Arc<RwLock<IdxSized<u8>>>,
    term_begin: Arc<RwLock<IdxSized<u64>>>,
    term_end: Arc<RwLock<IdxSized<u64>>>,
    last_updated: Arc<RwLock<IdxSized<u64>>>,
    fields_cache: HashMap<String, Arc<RwLock<FieldData>>>,
}
impl Data {
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref();
        if !dir.exists() {
            fs::create_dir_all(dir).unwrap();
        }

        let mut fields_cache = HashMap::new();

        let mut fields_dir = dir.to_path_buf();
        fields_dir.push("fields");
        if fields_dir.exists() {
            for d in fields_dir.read_dir()? {
                let d = d?;
                if d.file_type()?.is_dir() {
                    if let Some(fname) = d.file_name().to_str() {
                        let field = FieldData::new(d.path())?;
                        fields_cache
                            .entry(String::from(fname))
                            .or_insert(Arc::new(RwLock::new(field)));
                    }
                }
            }
        }

        let mut serial_path = dir.to_path_buf();
        serial_path.push("serial");

        let mut uuid_path = dir.to_path_buf();
        uuid_path.push("uuid.i");

        let mut activity_path = dir.to_path_buf();
        activity_path.push("activity.i");

        let mut term_begin_path = dir.to_path_buf();
        term_begin_path.push("term_begin.i");

        let mut term_end_path = dir.to_path_buf();
        term_end_path.push("term_end.i");

        let mut last_updated_path = dir.to_path_buf();
        last_updated_path.push("last_updated.i");

        Ok(Self {
            fields_dir,
            serial: Arc::new(RwLock::new(SerialNumber::new(serial_path)?)),
            uuid: Arc::new(RwLock::new(IdxSized::new(uuid_path)?)),
            activity: Arc::new(RwLock::new(IdxSized::new(activity_path)?)),
            term_begin: Arc::new(RwLock::new(IdxSized::new(term_begin_path)?)),
            term_end: Arc::new(RwLock::new(IdxSized::new(term_end_path)?)),
            last_updated: Arc::new(RwLock::new(IdxSized::new(last_updated_path)?)),
            fields_cache,
        })
    }

    pub fn update(&mut self, operation: &Operation) -> io::Result<u32> {
        Ok(match operation {
            Operation::New {
                activity,
                term_begin,
                term_end,
                fields,
            } => self.create_row(activity, term_begin, term_end, fields)?,
            Operation::Update {
                row,
                activity,
                term_begin,
                term_end,
                fields,
            } => {
                self.update_row(*row, activity, term_begin, term_end, fields)?;
                *row
            }
            Operation::Delete { row } => {
                self.delete(*row);
                0
            }
        })
    }

    pub fn create_row(
        &mut self,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> io::Result<u32> {
        if self.serial.read().unwrap().exists_blank() {
            let row = self.serial.write().unwrap().pop_blank()?.unwrap();
            self.create_row_recycled(row, activity, term_begin, term_end, fields)
        } else {
            self.create_row_new(activity, term_begin, term_end, fields)
        }
    }
    fn create_row_recycled(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> io::Result<u32> {
        let mut handles = Vec::new();

        let index = self.uuid.clone();
        handles.push(thread::spawn(move || {
            index
                .write()
                .unwrap()
                .update(row, Uuid::new_v4().as_u128())
                .unwrap(); //recycled serial_number,uuid recreate.
        }));

        handles.push(self.update_activity_async(row, *activity));

        handles.push(self.update_term_begin_async(
            row,
            if let Term::Overwrite(term_begin) = term_begin {
                *term_begin
            } else {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
            },
        ));

        handles.push(self.update_term_end_async(
            row,
            if let Term::Overwrite(term_end) = term_end {
                *term_end
            } else {
                0
            },
        ));

        handles.append(&mut self.update_fields(row, fields)?);

        for h in handles {
            h.join().unwrap();
        }

        Ok(row)
    }
    fn create_row_new(
        &mut self,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> io::Result<u32> {
        let row = self.serial.write().unwrap().add()?;

        let mut handles = Vec::new();

        let index = self.uuid.clone();
        handles.push(thread::spawn(move || {
            index
                .write()
                .unwrap()
                .update(row, Uuid::new_v4().as_u128())
                .unwrap();
        }));

        let activity = *activity as u8;
        let index = self.activity.clone();
        handles.push(thread::spawn(move || {
            index.write().unwrap().update(row, activity).unwrap();
        }));

        let term_begin = if let Term::Overwrite(term_begin) = term_begin {
            *term_begin
        } else {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        };
        let index = self.term_begin.clone();
        handles.push(thread::spawn(move || {
            index.write().unwrap().update(row, term_begin).unwrap();
        }));

        let term_end = if let Term::Overwrite(term_end) = term_end {
            *term_end
        } else {
            0
        };
        let index = self.term_end.clone();
        handles.push(thread::spawn(move || {
            index.write().unwrap().update(row, term_end).unwrap();
        }));

        handles.append(&mut self.update_fields(row, fields)?);

        for h in handles {
            h.join().unwrap();
        }

        Ok(row)
    }

    pub fn update_row(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> io::Result<()> {
        let serial = self.serial.read().unwrap().index().value(row);
        if let Some(_) = serial {
            let mut handles = Vec::new();

            handles.push(self.update_activity_async(row, *activity));

            handles.push(self.update_term_begin_async(
                row,
                if let Term::Overwrite(term_begin) = term_begin {
                    *term_begin
                } else {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                },
            ));

            handles.push(self.update_term_end_async(
                row,
                if let Term::Overwrite(term_end) = term_end {
                    *term_end
                } else {
                    0
                },
            ));

            handles.append(&mut self.update_fields(row, fields)?);

            for h in handles {
                h.join().unwrap();
            }
        }

        Ok(())
    }

    pub fn update_row_single_thread(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> io::Result<()> {
        let serial = self.serial.read().unwrap().index().value(row);
        if let Some(_) = serial {
            self.activity
                .clone()
                .write()
                .unwrap()
                .update(row, *activity as u8)?;
            self.term_begin.clone().write().unwrap().update(
                row,
                if let Term::Overwrite(term_begin) = term_begin {
                    *term_begin
                } else {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                },
            )?;
            self.term_end.clone().write().unwrap().update(
                row,
                if let Term::Overwrite(term_end) = term_end {
                    *term_end
                } else {
                    0
                },
            )?;
            for kv in fields.iter() {
                let field = if self.fields_cache.contains_key(&kv.key) {
                    self.fields_cache.get_mut(&kv.key).unwrap()
                } else {
                    self.create_field(&kv.key)?
                };
                field.clone().write().unwrap().update(row, &kv.value)?;
            }
            self.last_update_now(row)
        } else {
            Ok(())
        }
    }
    fn last_update_now(&mut self, row: u32) -> io::Result<()> {
        self.last_updated.write().unwrap().update(
            row,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )?;
        Ok(())
    }
    fn update_activity_async(&mut self, row: u32, activity: Activity) -> thread::JoinHandle<()> {
        let index = self.activity.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, activity as u8).unwrap();
        })
    }
    pub fn update_activity(&mut self, row: u32, activity: Activity) -> io::Result<()> {
        let h = self.update_activity_async(row, activity);
        self.last_update_now(row)?;
        h.join().unwrap();
        Ok(())
    }
    fn update_term_begin_async(&mut self, row: u32, from: u64) -> thread::JoinHandle<()> {
        let index = self.term_begin.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, from).unwrap();
        })
    }
    pub fn update_term_begin(&mut self, row: u32, from: u64) -> io::Result<()> {
        let h = self.update_term_begin_async(row, from);
        self.last_update_now(row)?;
        h.join().unwrap();
        Ok(())
    }
    fn update_term_end_async(&mut self, row: u32, to: u64) -> thread::JoinHandle<()> {
        let index = self.term_end.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, to).unwrap();
        })
    }
    pub fn update_term_end(&mut self, row: u32, to: u64) -> io::Result<()> {
        let h = self.update_term_end_async(row, to);
        self.last_update_now(row)?;
        h.join().unwrap();
        Ok(())
    }
    pub fn update_fields(
        &mut self,
        row: u32,
        fields: &Vec<KeyValue>,
    ) -> io::Result<Vec<thread::JoinHandle<()>>> {
        let mut handles = Vec::new();
        for kv in fields.iter() {
            handles.push(self.update_field_async(row, &kv.key, &kv.value)?);
        }
        self.last_update_now(row)?;
        Ok(handles)
    }
    pub fn update_field_async(
        &mut self,
        row: u32,
        field_name: &str,
        cont: &Vec<u8>,
    ) -> io::Result<thread::JoinHandle<()>> {
        let field = if self.fields_cache.contains_key(field_name) {
            self.fields_cache.get_mut(field_name).unwrap()
        } else {
            self.create_field(field_name)?
        };
        let cont = cont.to_owned();
        let index = field.clone();
        Ok(thread::spawn(move || {
            index.write().unwrap().update(row, &cont).unwrap();
        }))
    }
    pub fn update_field(&mut self, row: u32, field_name: &str, cont: &[u8]) -> io::Result<()> {
        let field = if self.fields_cache.contains_key(field_name) {
            self.fields_cache.get_mut(field_name).unwrap()
        } else {
            self.create_field(field_name)?
        };
        field.clone().write().unwrap().update(row, cont)?;
        self.last_update_now(row)
    }
    fn create_field(&mut self, field_name: &str) -> io::Result<&mut Arc<RwLock<FieldData>>> {
        let mut fields_dir = self.fields_dir.clone();
        fields_dir.push(field_name);
        fs::create_dir_all(&fields_dir)?;
        if fields_dir.exists() {
            let field = FieldData::new(fields_dir)?;
            self.fields_cache
                .entry(String::from(field_name))
                .or_insert(Arc::new(RwLock::new(field)));
        }
        Ok(self.fields_cache.get_mut(field_name).unwrap())
    }
    pub fn delete(&mut self, row: u32) {
        let serial = self.serial.read().unwrap().index().value(row);
        if let Some(_) = serial {
            let mut handles = Vec::new();
            let index = self.serial.clone();
            handles.push(thread::spawn(move || {
                index.write().unwrap().delete(row).unwrap();
            }));

            let index = self.uuid.clone();
            handles.push(thread::spawn(move || {
                index.write().unwrap().delete(row);
            }));

            let index = self.activity.clone();
            handles.push(thread::spawn(move || {
                index.write().unwrap().delete(row);
            }));

            let index = self.term_begin.clone();
            handles.push(thread::spawn(move || {
                index.write().unwrap().delete(row);
            }));

            let index = self.term_end.clone();
            handles.push(thread::spawn(move || {
                index.write().unwrap().delete(row);
            }));

            self.load_fields().unwrap();
            for (_, v) in self.fields_cache.iter() {
                let index = v.clone();
                handles.push(thread::spawn(move || {
                    index.write().unwrap().delete(row);
                }));
            }

            self.last_updated.write().unwrap().delete(row);

            for h in handles {
                h.join().unwrap();
            }
        }
    }

    pub fn serial(&self, row: u32) -> u32 {
        if let Some(v) = self.serial.read().unwrap().index().value(row) {
            v
        } else {
            0
        }
    }
    pub fn uuid(&self, row: u32) -> u128 {
        if let Some(v) = self.uuid.read().unwrap().value(row) {
            v
        } else {
            0
        }
    }
    pub fn uuid_str(&self, row: u32) -> String {
        if let Some(v) = self.uuid.read().unwrap().value(row) {
            uuid::Uuid::from_u128(v).to_string()
        } else {
            "".to_string()
        }
    }
    pub fn activity(&self, row: u32) -> Activity {
        if let Some(v) = self.activity.read().unwrap().value(row) {
            if v != 0 {
                Activity::Active
            } else {
                Activity::Inactive
            }
        } else {
            Activity::Inactive
        }
    }
    pub fn term_begin(&self, row: u32) -> u64 {
        if let Some(v) = self.term_begin.read().unwrap().value(row) {
            v
        } else {
            0
        }
    }
    pub fn term_end(&self, row: u32) -> u64 {
        if let Some(v) = self.term_end.read().unwrap().value(row) {
            v
        } else {
            0
        }
    }
    pub fn last_updated(&self, row: u32) -> u64 {
        if let Some(v) = self.last_updated.read().unwrap().value(row) {
            v
        } else {
            0
        }
    }
    pub fn field_bytes(&self, row: u32, name: &str) -> &[u8] {
        if let Some(f) = self.field(name) {
            if let Some(v) = f.read().unwrap().get(row) {
                v
            } else {
                b""
            }
        } else {
            b""
        }
    }
    pub fn field_num(&self, row: u32, name: &str) -> f64 {
        if let Some(f) = self.field(name) {
            if let Some(f) = f.read().unwrap().num(row) {
                f
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub fn fields(&self) -> Vec<&String> {
        let mut fields = Vec::new();
        for (key, _) in self.fields_cache.iter() {
            fields.push(key);
        }
        fields
    }
    pub fn load_fields(&mut self) -> io::Result<()> {
        if self.fields_dir.exists() {
            for p in self.fields_dir.read_dir()? {
                let p = p?;
                let path = p.path();
                if path.is_dir() {
                    if let Some(str_fname) = p.file_name().to_str() {
                        if !self.fields_cache.contains_key(str_fname) {
                            let field = FieldData::new(&path)?;
                            self.fields_cache
                                .entry(String::from(str_fname))
                                .or_insert(Arc::new(RwLock::new(field)));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn field(&self, name: &str) -> Option<&Arc<RwLock<FieldData>>> {
        self.fields_cache.get(name)
    }

    pub fn all(&self) -> RowSet {
        self.serial
            .read()
            .unwrap()
            .index()
            .triee()
            .iter()
            .map(|r| r.row())
            .collect()
    }
    pub fn begin_search(&self) -> Search {
        Search::new(self)
    }
    pub fn search_field(&self, field_name: impl Into<String>, condition: search::Field) -> Search {
        Search::new(self).search_field(field_name, condition)
    }
    pub fn search_activity(&self, condition: Activity) -> Search {
        Search::new(self).search_activity(condition)
    }
    pub fn search_term(&self, condition: search::Term) -> Search {
        Search::new(self).search_term(condition)
    }
    pub fn search_row(&self, condition: search::Number) -> Search {
        Search::new(self).search_row(condition)
    }
    pub fn search_default(&self) -> Search {
        Search::new(self).search_default()
    }

    pub fn sort(&self, rows: RowSet, orders: &[Order]) -> Vec<u32> {
        let mut sub_orders = vec![];
        for i in 1..orders.len() {
            sub_orders.push(&orders[i]);
        }
        self.sort_with_suborders(rows, &orders[0], sub_orders)
    }
    fn subsort(&self, tmp: Vec<u32>, sub_orders: &[&Order]) -> Vec<u32> {
        let mut tmp = tmp;
        tmp.sort_by(|a, b| {
            for i in 0..sub_orders.len() {
                match sub_orders[i] {
                    Order::Asc(order_key) => match order_key {
                        OrderKey::Serial => {
                            let a = self.serial.read().unwrap().index().value(*a).unwrap();
                            let b = self.serial.read().unwrap().index().value(*b).unwrap();
                            return a.cmp(&b);
                        }
                        OrderKey::Row => return a.cmp(b),
                        OrderKey::TermBegin => {
                            let a = self.term_begin.read().unwrap().value(*a).unwrap();
                            let b = self.term_begin.read().unwrap().value(*b).unwrap();
                            let ord = a.cmp(&b);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        OrderKey::TermEnd => {
                            let a = self.term_end.read().unwrap().value(*a).unwrap();
                            let b = self.term_end.read().unwrap().value(*b).unwrap();
                            let ord = a.cmp(&b);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        OrderKey::LastUpdated => {
                            let a = self.last_updated.read().unwrap().value(*a).unwrap();
                            let b = self.last_updated.read().unwrap().value(*b).unwrap();
                            let ord = a.cmp(&b);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        OrderKey::Field(field_name) => {
                            if let Some(field) = self.field(&field_name) {
                                let a = field.read().unwrap().get(*a).unwrap();
                                let b = field.read().unwrap().get(*b).unwrap();
                                let ord = natord::compare(
                                    std::str::from_utf8(a).unwrap(),
                                    std::str::from_utf8(b).unwrap(),
                                );
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                    },
                    Order::Desc(order_key) => match order_key {
                        OrderKey::Serial => {
                            let a = self.serial.read().unwrap().index().value(*a).unwrap();
                            let b = self.serial.read().unwrap().index().value(*b).unwrap();
                            return b.cmp(&a);
                        }
                        OrderKey::Row => {
                            return b.cmp(a);
                        }
                        OrderKey::TermBegin => {
                            let a = self.term_begin.read().unwrap().value(*a).unwrap();
                            let b = self.term_begin.read().unwrap().value(*b).unwrap();
                            let ord = b.cmp(&a);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        OrderKey::TermEnd => {
                            let a = self.term_end.read().unwrap().value(*a).unwrap();
                            let b = self.term_end.read().unwrap().value(*b).unwrap();
                            let ord = b.cmp(&a);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        OrderKey::LastUpdated => {
                            let a = self.last_updated.read().unwrap().value(*a).unwrap();
                            let b = self.last_updated.read().unwrap().value(*b).unwrap();
                            let ord = b.cmp(&a);
                            if ord != Ordering::Equal {
                                return ord;
                            }
                        }
                        OrderKey::Field(field_name) => {
                            if let Some(field) = self.field(&field_name) {
                                let a = field.read().unwrap().get(*a).unwrap();
                                let b = field.read().unwrap().get(*b).unwrap();
                                let ord = natord::compare(
                                    std::str::from_utf8(b).unwrap(),
                                    std::str::from_utf8(a).unwrap(),
                                );
                                if ord != Ordering::Equal {
                                    return ord;
                                }
                            }
                        }
                    },
                }
            }
            Ordering::Equal
        });
        tmp
    }
    fn sort_with_iter<T>(
        &self,
        rows: RowSet,
        iter: &mut AvltrieeIter<T>,
        sub_orders: Vec<&Order>,
    ) -> Vec<u32>
    where
        T: PartialEq,
    {
        let mut ret = Vec::new();
        if sub_orders.len() == 0 {
            for row in iter {
                let row = row.row();
                if rows.contains(&row) {
                    ret.push(row);
                }
            }
        } else {
            let mut before: Option<&T> = None;
            let mut tmp: Vec<u32> = Vec::new();
            for row in iter {
                let r = row.row();
                if rows.contains(&r) {
                    let value = row.value();
                    if let Some(before) = before {
                        if before.ne(value) {
                            if tmp.len() <= 1 {
                                ret.extend(tmp);
                            } else {
                                let tmp = self.subsort(tmp, &mut sub_orders.clone());
                                ret.extend(tmp);
                            }
                            tmp = vec![];
                        }
                    } else {
                        ret.extend(tmp);
                        tmp = vec![];
                    }
                    tmp.push(r);
                    before = Some(value);
                }
            }
            if tmp.len() <= 1 {
                ret.extend(tmp);
            } else {
                let tmp = self.subsort(tmp, &mut sub_orders.clone());
                ret.extend(tmp);
            }
        }
        ret
    }
    fn sort_with_key(&self, rows: RowSet, key: &OrderKey, sub_orders: Vec<&Order>) -> Vec<u32> {
        let mut ret = Vec::new();
        match key {
            OrderKey::Serial => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.serial.read().unwrap().index().triee().iter(),
                    vec![],
                );
            }
            OrderKey::Row => {
                ret = rows.iter().map(|&x| x).collect::<Vec<u32>>();
            }
            OrderKey::TermBegin => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.term_begin.read().unwrap().triee().iter(),
                    sub_orders,
                );
            }
            OrderKey::TermEnd => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.term_end.read().unwrap().triee().iter(),
                    sub_orders,
                );
            }
            OrderKey::LastUpdated => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.last_updated.read().unwrap().triee().iter(),
                    sub_orders,
                );
            }
            OrderKey::Field(field_name) => {
                if let Some(field) = self.field(&field_name) {
                    ret = self.sort_with_iter(
                        rows,
                        &mut field.read().unwrap().index().triee().iter(),
                        sub_orders,
                    );
                }
            }
        }
        ret
    }
    fn sort_with_key_desc(
        &self,
        rows: RowSet,
        key: &OrderKey,
        sub_orders: Vec<&Order>,
    ) -> Vec<u32> {
        let mut ret = Vec::new();
        match key {
            OrderKey::Serial => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.serial.read().unwrap().index().triee().desc_iter(),
                    vec![],
                );
            }
            OrderKey::Row => {
                ret = rows.iter().rev().map(|&x| x).collect::<Vec<u32>>();
            }
            OrderKey::TermBegin => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.term_begin.read().unwrap().triee().desc_iter(),
                    sub_orders,
                );
            }
            OrderKey::TermEnd => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.term_end.read().unwrap().triee().desc_iter(),
                    sub_orders,
                );
            }
            OrderKey::LastUpdated => {
                ret = self.sort_with_iter(
                    rows,
                    &mut self.last_updated.read().unwrap().triee().desc_iter(),
                    sub_orders,
                );
            }
            OrderKey::Field(field_name) => {
                if let Some(field) = self.field(&field_name) {
                    ret = self.sort_with_iter(
                        rows,
                        &mut field.read().unwrap().index().triee().desc_iter(),
                        sub_orders,
                    );
                }
            }
        }
        ret
    }
    fn sort_with_suborders(
        &self,
        rows: RowSet,
        order: &Order,
        sub_orders: Vec<&Order>,
    ) -> Vec<u32> {
        match order {
            Order::Asc(key) => self.sort_with_key(rows, key, sub_orders),
            Order::Desc(key) => self.sort_with_key_desc(rows, key, sub_orders),
        }
    }
}
