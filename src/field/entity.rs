use various_data_file::DataAddress;

pub struct FieldEntity{
    word:DataAddress
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
            word:DataAddress::default()
            ,num:0.0
        }
    }
}

impl FieldEntity {
    pub fn new(word:DataAddress,num:f64)->FieldEntity{
        FieldEntity{
            word:word
            ,num
        }
    }
    pub fn addr(&self)->isize{
        self.word.offset() as isize
    }
    pub fn word(&self)->&DataAddress{
        &self.word
    }
    pub fn num(&self)->f64{
        self.num
    }
}