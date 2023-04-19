use various_data_file::DataAddress;

#[derive(Clone, Default, PartialEq, Debug)]
pub struct FieldEntity {
    data_address: DataAddress,
    num: f64,
}

impl FieldEntity {
    pub fn new(data_address: &DataAddress, num: f64) -> Self {
        FieldEntity {
            data_address: data_address.clone(),
            num,
        }
    }
    pub fn addr(&self) -> i64 {
        self.data_address.offset()
    }
    pub fn len(&self) -> u64 {
        self.data_address.len()
    }
    pub fn data_address(&self) -> &DataAddress {
        &self.data_address
    }
    pub fn num(&self) -> f64 {
        self.num
    }
}
