use idx_sized::AvltrieeIter;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
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

pub mod prelude;

pub struct Data {
    data_dir: String,
    serial: Arc<RwLock<SerialNumber>>,
    uuid: Arc<RwLock<IdxSized<u128>>>,
    activity: Arc<RwLock<IdxSized<u8>>>,
    term_begin: Arc<RwLock<IdxSized<i64>>>,
    term_end: Arc<RwLock<IdxSized<i64>>>,
    last_updated: Arc<RwLock<IdxSized<i64>>>,
    fields_cache: HashMap<String, Arc<RwLock<FieldData>>>,
}
impl Data {
    pub fn new(dir: &str) -> Result<Self, std::io::Error> {
        if !std::path::Path::new(dir).exists() {
            std::fs::create_dir_all(dir).unwrap();
        }

        let mut fields_cache = HashMap::new();

        let fields_dir = dir.to_string() + "/fields";
        if std::path::Path::new(&fields_dir).exists() {
            if let Ok(dir) = std::fs::read_dir(&fields_dir) {
                for d in dir.into_iter() {
                    let d = d.unwrap();
                    let dt = d.file_type().unwrap();
                    if dt.is_dir() {
                        if let Some(fname) = d.path().file_name() {
                            if let Some(fname) = fname.to_str() {
                                if let Some(field_dir) = d.path().to_str() {
                                    let field_dir = field_dir.to_owned() + "/";
                                    let field = FieldData::new(&field_dir).unwrap();
                                    fields_cache
                                        .entry(String::from(fname))
                                        .or_insert(Arc::new(RwLock::new(field)));
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Data {
            data_dir: dir.to_string(),
            serial: Arc::new(RwLock::new(SerialNumber::new(
                &(dir.to_string() + "/serial"),
            )?)),
            uuid: Arc::new(RwLock::new(IdxSized::new(&(dir.to_string() + "/uuid.i"))?)),
            activity: Arc::new(RwLock::new(IdxSized::new(
                &(dir.to_string() + "/activity.i"),
            )?)),
            term_begin: Arc::new(RwLock::new(IdxSized::new(
                &(dir.to_string() + "/term_begin.i"),
            )?)),
            term_end: Arc::new(RwLock::new(IdxSized::new(
                &(dir.to_string() + "/term_end.i"),
            )?)),
            last_updated: Arc::new(RwLock::new(IdxSized::new(
                &(dir.to_string() + "/last_updated.i"),
            )?)),
            fields_cache,
        })
    }

    pub fn update(&mut self, operation: &Operation) -> u32 {
        match operation {
            Operation::New {
                activity,
                term_begin,
                term_end,
                fields,
            } => self.create_row(activity, term_begin, term_end, fields),
            Operation::Update {
                row,
                activity,
                term_begin,
                term_end,
                fields,
            } => {
                self.update_row(*row, activity, term_begin, term_end, fields);
                *row
            }
            Operation::Delete { row } => {
                self.delete(*row);
                0
            }
        }
    }

    pub fn create_row(
        &mut self,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) -> u32 {
        if self.serial.read().unwrap().exists_blank() {
            let row = self.serial.write().unwrap().pop_blank().unwrap();
            self.create_row_recycled(row, activity, term_begin, term_end, fields);
            row
        } else {
            let row = self.serial.write().unwrap().add().unwrap();
            self.create_row_new(row, activity, term_begin, term_end, fields);
            row
        }
    }
    fn create_row_recycled(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) {
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
                chrono::Local::now().timestamp()
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

        handles.append(&mut self.update_fields(row, fields));

        for h in handles {
            h.join().unwrap();
        }
    }
    fn create_row_new(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) {
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
            chrono::Local::now().timestamp()
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

        handles.append(&mut self.update_fields(row, fields));

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn update_row(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) {
        let mut handles = Vec::new();

        handles.push(self.update_activity_async(row, *activity));

        handles.push(self.update_term_begin_async(
            row,
            if let Term::Overwrite(term_begin) = term_begin {
                *term_begin
            } else {
                chrono::Local::now().timestamp()
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

        handles.append(&mut self.update_fields(row, fields));

        for h in handles {
            h.join().unwrap();
        }
    }

    pub fn update_row_single_thread(
        &mut self,
        row: u32,
        activity: &Activity,
        term_begin: &Term,
        term_end: &Term,
        fields: &Vec<KeyValue>,
    ) {
        self.activity
            .clone()
            .write()
            .unwrap()
            .update(row, *activity as u8)
            .unwrap();
        self.term_begin
            .clone()
            .write()
            .unwrap()
            .update(
                row,
                if let Term::Overwrite(term_begin) = term_begin {
                    *term_begin
                } else {
                    chrono::Local::now().timestamp()
                },
            )
            .unwrap();
        self.term_end
            .clone()
            .write()
            .unwrap()
            .update(
                row,
                if let Term::Overwrite(term_end) = term_end {
                    *term_end
                } else {
                    0
                },
            )
            .unwrap();
        for kv in fields.iter() {
            let field = if self.fields_cache.contains_key(&kv.key) {
                self.fields_cache.get_mut(&kv.key).unwrap()
            } else {
                self.create_field(&kv.key)
            };
            field
                .clone()
                .write()
                .unwrap()
                .update(row, &kv.value)
                .unwrap();
        }
        self.last_update_now(row).unwrap();
    }
    fn last_update_now(&mut self, row: u32) -> Result<(), std::io::Error> {
        self.last_updated
            .write()
            .unwrap()
            .update(row, chrono::Local::now().timestamp())?;
        Ok(())
    }
    fn update_activity_async(&mut self, row: u32, activity: Activity) -> thread::JoinHandle<()> {
        let index = self.activity.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, activity as u8).unwrap();
        })
    }
    pub fn update_activity(&mut self, row: u32, activity: Activity) -> Result<(), std::io::Error> {
        let h = self.update_activity_async(row, activity);
        self.last_update_now(row)?;
        h.join().unwrap();
        Ok(())
    }
    fn update_term_begin_async(&mut self, row: u32, from: i64) -> thread::JoinHandle<()> {
        let index = self.term_begin.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, from).unwrap();
        })
    }
    pub fn update_term_begin(&mut self, row: u32, from: i64) -> Result<(), std::io::Error> {
        let h = self.update_term_begin_async(row, from);
        self.last_update_now(row)?;
        h.join().unwrap();
        Ok(())
    }
    fn update_term_end_async(&mut self, row: u32, to: i64) -> thread::JoinHandle<()> {
        let index = self.term_end.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, to).unwrap();
        })
    }
    pub fn update_term_end(&mut self, row: u32, to: i64) -> Result<(), std::io::Error> {
        let h = self.update_term_end_async(row, to);
        self.last_update_now(row)?;
        h.join().unwrap();
        Ok(())
    }
    pub fn update_fields(
        &mut self,
        row: u32,
        fields: &Vec<KeyValue>,
    ) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::new();
        for kv in fields.iter() {
            handles.push(self.update_field_async(row, &kv.key, &kv.value));
        }
        self.last_update_now(row).unwrap();
        handles
    }
    pub fn update_field_async(
        &mut self,
        row: u32,
        field_name: &str,
        cont: &Vec<u8>,
    ) -> thread::JoinHandle<()> {
        let field = if self.fields_cache.contains_key(field_name) {
            self.fields_cache.get_mut(field_name).unwrap()
        } else {
            self.create_field(field_name)
        };
        let cont = cont.to_owned();
        let index = field.clone();
        thread::spawn(move || {
            index.write().unwrap().update(row, &cont).unwrap();
        })
    }
    pub fn update_field(
        &mut self,
        row: u32,
        field_name: &str,
        cont: &[u8],
    ) -> Result<(), std::io::Error> {
        let field = if self.fields_cache.contains_key(field_name) {
            self.fields_cache.get_mut(field_name).unwrap()
        } else {
            self.create_field(field_name)
        };
        field.clone().write().unwrap().update(row, cont).unwrap();
        self.last_update_now(row)?;
        Ok(())
    }
    fn create_field(&mut self, field_name: &str) -> &mut Arc<RwLock<FieldData>> {
        let dir_name = self.data_dir.to_string() + "/fields/" + field_name + "/";
        std::fs::create_dir_all(dir_name.to_owned()).unwrap();
        if std::path::Path::new(&dir_name).exists() {
            let field = FieldData::new(&dir_name).unwrap();
            self.fields_cache
                .entry(String::from(field_name))
                .or_insert(Arc::new(RwLock::new(field)));
        }
        self.fields_cache.get_mut(field_name).unwrap()
    }
    pub fn delete(&mut self, row: u32) {
        let mut handles = Vec::new();
        let index = self.serial.clone();
        handles.push(thread::spawn(move || {
            index.write().unwrap().delete(row);
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

        self.load_fields();
        for (_, v) in &mut self.fields_cache {
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
    pub fn term_begin(&self, row: u32) -> i64 {
        if let Some(v) = self.term_begin.read().unwrap().value(row) {
            v
        } else {
            0
        }
    }
    pub fn term_end(&self, row: u32) -> i64 {
        if let Some(v) = self.term_end.read().unwrap().value(row) {
            v
        } else {
            0
        }
    }
    pub fn last_updated(&self, row: u32) -> i64 {
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
        for (key, _) in &self.fields_cache {
            fields.push(key);
        }
        fields
    }
    pub fn load_fields(&mut self) {
        let d = std::fs::read_dir(self.data_dir.to_string() + "/fields/").unwrap();
        for p in d {
            if let Ok(p) = p {
                let path = p.path();
                if path.is_dir() {
                    if let Some(fname) = path.file_name() {
                        if let Some(str_fname) = fname.to_str() {
                            if !self.fields_cache.contains_key(str_fname) {
                                if let Some(p) = path.to_str() {
                                    let field = FieldData::new(&(p.to_string() + "/")).unwrap();
                                    self.fields_cache
                                        .entry(String::from(str_fname))
                                        .or_insert(Arc::new(RwLock::new(field)));
                                }
                            }
                        }
                    }
                }
            }
        }
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

    pub fn sort(&self, rows: RowSet, orders: Vec<Order>) -> Vec<u32> {
        let mut sub_orders = vec![];
        for i in 1..orders.len() {
            sub_orders.push(&orders[i]);
        }
        self.sort_with_suborders(rows, &orders[0], sub_orders)
    }
    fn subsort(&self, tmp: Vec<u32>, sub_orders: &mut Vec<&Order>) -> Vec<u32> {
        let mut tmp = tmp;
        tmp.sort_by(|a, b| {
            for i in 0..sub_orders.len() {
                let order = sub_orders[i];
                match order {
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
