use various_data_file::DataAddress;

pub struct FieldEntity{
    data_address:DataAddress
    ,num:f64
}
impl Copy for FieldEntity {}
impl std::clone::Clone for FieldEntity {
    fn clone(&self) -> FieldEntity {
        *self
    }
}
impl std::default::Default for FieldEntity{
    fn default() -> FieldEntity {
        FieldEntity{
            data_address:DataAddress::default()
            ,num:0.0
        }
    }
}

impl FieldEntity {
    pub fn new(data_address:DataAddress,num:f64)->Self{
        FieldEntity{
            data_address:data_address
            ,num
        }
    }
    pub fn addr(&self)->isize{
        self.data_address.offset() as isize
    }
    pub fn len(&self)->usize{
        self.data_address.len() as usize
    }
    pub fn data_address(&self)->&DataAddress{
        &self.data_address
    }
    pub fn num(&self)->f64{
        self.num
    }
}