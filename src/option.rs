use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct DataOption {
    pub uuid: bool,
    pub activity: bool,
    pub term: bool,
    pub last_updated: bool,
    pub allocation_lot: u32,
}
impl Default for DataOption {
    fn default() -> Self {
        Self {
            uuid: true,
            activity: true,
            term: true,
            last_updated: true,
            allocation_lot: 1,
        }
    }
}
