#[cfg(test)]

#[test]
fn test3() {
    use versatile_data::prelude::*;

    let dir="./vd-test3/";
    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
    }
    
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST".to_owned())
            ]
        });
        if let Ok(str)=std::str::from_utf8(data.field_bytes(1,"test")){
            println!("FIELD:{}",str);
        }
    }
    if let Ok(data)=Data::new(dir){
        if let Ok(str)=std::str::from_utf8(data.field_bytes(1,"test")){
            println!("FIELD:{}",str);
        }
    }
}