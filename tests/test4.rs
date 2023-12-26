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
        data.update(Operation::New(Record {
            fields: [
                ("name".into(), "test".into()),
                ("password".into(), "test".into()),
            ]
            .into(),
            ..Default::default()
        }))
        .await;
        data.update(Operation::New(Record {
            activity: Activity::Active,
            term_begin: Term::Default,
            term_end: Term::Default,
            fields: [
                ("name".into(), "test2".into()),
                ("password".into(), "test".into()),
            ]
            .into(),
        }))
        .await;
        data.update(Operation::New(Record {
            fields: [
                ("name".into(), "test3".into()),
                ("password".into(), "test".into()),
            ]
            .into(),
            ..Default::default()
        }))
        .await;
        data.update(Operation::Delete {
            row: 2.try_into().unwrap(),
        })
        .await;
        data.update(Operation::New(Record {
            fields: [
                ("name".into(), "test4".into()),
                ("password".into(), "test".into()),
            ]
            .into(),
            ..Default::default()
        }))
        .await;
        let r = data
            .search_default()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await;
        println!("{:?}", r);

        let mut data = Data::new(dir, DataOption::default());
        data.update(Operation::New(Record {
            fields: [
                ("name".into(), "test5".into()),
                ("password".into(), "test".into()),
            ]
            .into(),
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
