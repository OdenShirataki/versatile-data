#[cfg(test)]
#[test]
fn test5() {
    use versatile_data::*;

    let dir = "./vd-test5/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());
    futures::executor::block_on(async {
        data.update(Operation::New(Record {
            fields: [("num".into(), "2".into())].into(),
            ..Default::default()
        }))
        .await;
        data.update(Operation::New(Record {
            fields: [("num".into(), "2".into())].into(),
            ..Default::default()
        }))
        .await;
        data.update(Operation::New(Record {
            fields: [("num".into(), "3".into())].into(),
            ..Default::default()
        }))
        .await;
        data.update(Operation::New(Record {
            fields: [("num".into(), "5".into())].into(),
            ..Default::default()
        }))
        .await;
        data.update(Operation::New(Record {
            fields: [("num".into(), "8".into())].into(),
            ..Default::default()
        }))
        .await;

        println!("\nmatch");
        for r in data
            .search_default()
            .search_field("num", &search::Field::Match(b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
            );
        }

        println!("\nmin");
        for r in data
            .search_default()
            .search_field("num", &search::Field::Min(b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
            );
        }
        println!("\nmax");
        for r in data
            .search_default()
            .search_field("num", &search::Field::Max(b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
            );
        }

        println!("\nrange");
        for r in data
            .search_default()
            .search_field("num", &search::Field::Range(b"3".to_vec(), b"5".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
            );
        }

        println!("\nrange bad");
        for r in data
            .search_default()
            .search_field("num", &search::Field::Range(b"5".to_vec(), b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
            );
        }

        println!("OK");
    });
}
