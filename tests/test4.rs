#[cfg(test)]

#[test]
fn test4() {
    use versatile_data::prelude::*;

    let dir="./vd-test4/";
    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
    }

    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("name","test".to_owned())
                ,KeyValue::new("password","test".to_owned())
            ]
        });
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("name","test2".to_owned())
                ,KeyValue::new("password","test".to_owned())
            ]
        });
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("name","test3".to_owned())
                ,KeyValue::new("password","test".to_owned())
            ]
        });
        data.update(&Operation::Delete { row: 2});
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("name","test4".to_owned())
                ,KeyValue::new("password","test".to_owned())
            ]
        });
        let r=data
            .search_default() 
            .result_with_sort(vec![
                Order::Asc(OrderKey::Serial)
            ])
        ;
        println!("{:?}",r);
    }
    if let Ok(mut data)=Data::new(dir){
        data.update(&Operation::New{
            activity:Activity::Active
            ,term_begin:Term::Defalut
            ,term_end:Term::Defalut
            ,fields:vec![
                KeyValue::new("name","test5".to_owned())
                ,KeyValue::new("password","test".to_owned())
            ]
        });
        let r=data
            .search_default() 
            .result_with_sort(vec![
                Order::Asc(OrderKey::Serial)
            ])
        ;
        println!("{:?}",r);
    }

    println!("OK");
}