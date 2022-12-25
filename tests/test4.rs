#[cfg(test)]

#[test]
fn test4() {
    use versatile_data::prelude::*;

    let dir="./vd-test4/";
    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
    }
    
    for i in 1..=4{
        if let Ok(mut data)=Data::new(dir){
            data.update(&Operation::New{
                activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![
                    KeyValue::new("test","TEST".to_owned()+&i.to_string())
                ]
            });
        }
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::Delete { row: 2});
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::Delete { row: 3});
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST11".to_owned())
            ]
        });
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST12".to_owned())
            ]
        });
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST13".to_owned())
            ]
        });
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::Delete { row: 2});
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::Delete { row: 3});
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::Delete { row: 4});
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST14".to_owned())
            ]
        });
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::Delete { row: 2});
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST15".to_owned())
            ]
        });
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("test","TEST16".to_owned())
            ]
        });
    }

    println!("OK");
}