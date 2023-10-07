#[cfg(test)]
#[test]
fn test4() {
    use versatile_data::*;

    let dir = "./vd-test4/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());
    futures::executor::block_on(async {
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            activity: Activity::Active,
            term_begin: Term::Default,
            term_end: Term::Default,
            fields: vec![
                KeyValue::new("name", "test2".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test3".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::Delete { row: 2 }).await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test4".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .await;
        let r = data
            .search_default()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await;
        println!("{:?}", r);

        let mut data = Data::new(dir, DataOption::default());
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "test5".to_owned()),
                KeyValue::new("password", "test".to_owned()),
            ],
            ..Default::default()
        }))
        .await;
        let r = data
            .search_default()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await;
        println!("{:?}", r);
    });

    println!("OK");
}
