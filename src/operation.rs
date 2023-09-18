use serde::{ser::SerializeMap, Serialize, Serializer};

#[derive(Debug, Clone)]
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
    #[inline(always)]
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        KeyValue {
            key: key.into(),
            value: value.into(),
        }
    }

    #[inline(always)]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[inline(always)]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

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
    pub fields: Vec<KeyValue>,
}

pub enum Operation {
    New(Record),
    Update { row: u32, record: Record },
    Delete { row: u32 },
}
