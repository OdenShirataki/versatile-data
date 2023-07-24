#[cfg(test)]
#[test]
fn test4() {
    use versatile_data::*;

    let dir = "./vd-test4/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    if let Ok(mut data) = Data::new(dir, DataOption::default()) {
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .unwrap();
        data.update(&Operation::New(Record {
            activity: Activity::Active,
            term_begin: Term::Default,
            term_end: Term::Default,
            fields: vec![
                KeyValue::new("name", "test2".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
        }))
        .unwrap();
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test3".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .unwrap();
        data.update(&Operation::Delete { row: 2 }).unwrap();
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test4".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .unwrap();
        let r = data
            .search_default()
            .unwrap()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)]);
        println!("{:?}", r);
    }
    if let Ok(mut data) = Data::new(dir, DataOption::default()) {
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test5".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .unwrap();
        let r = data
            .search_default()
            .unwrap()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)]);
        println!("{:?}", r);
    }

    println!("OK");
}
