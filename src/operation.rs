use serde::{ser::SerializeMap, Serialize, Serializer};

pub struct KeyValue {
    pub(super) key: String,
    pub(super) value: Vec<u8>,
}
impl Serialize for KeyValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(Some(1))?;
        if let Ok(s) = std::str::from_utf8(&self.value) {
            state.serialize_entry(&self.key, s)?;
        } else {
            state.serialize_entry(&self.key, &self.value)?;
        }
        state.end()
    }
}
impl KeyValue {
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        KeyValue {
            key: key.into(),
            value: value.into(),
        }
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Activity {
    Inactive = 0,
    Active = 1,
}

pub enum Term {
    Defalut,
    Overwrite(i64),
}

pub enum Operation {
    New {
        activity: Activity,
        term_begin: Term,
        term_end: Term,
        fields: Vec<KeyValue>,
    },
    Update {
        row: u32,
        activity: Activity,
        term_begin: Term,
        term_end: Term,
        fields: Vec<KeyValue>,
    },
    Delete {
        row: u32,
    },
}
