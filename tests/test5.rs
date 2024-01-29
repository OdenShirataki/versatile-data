#[cfg(test)]
#[test]
fn test5() {
    use versatile_data::*;

    let dir = "./vd-test5/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());
    let field_num = FieldName::new("num".into());

    futures::executor::block_on(async {
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_num.clone(), "2".into())].into(),
        )
        .await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_num.clone(), "2".into())].into(),
        )
        .await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_num.clone(), "3".into())].into(),
        )
        .await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_num.clone(), "5".into())].into(),
        )
        .await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_num.clone(), "8".into())].into(),
        )
        .await;

        println!("\nmatch");
        for r in data
            .search_default()
            .search_field(field_num.clone(), &search::Field::Match(b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, &field_num)).unwrap()
            );
        }

        println!("\nmin");
        for r in data
            .search_default()
            .search_field(field_num.clone(), &search::Field::Min(b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, &field_num)).unwrap()
            );
        }
        println!("\nmax");
        for r in data
            .search_default()
            .search_field(field_num.clone(), &search::Field::Max(b"3".to_vec()))
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, &field_num)).unwrap()
            );
        }

        println!("\nrange");
        for r in data
            .search_default()
            .search_field(
                field_num.clone(),
                &search::Field::Range(b"3".to_vec(), b"5".to_vec()),
            )
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, &field_num)).unwrap()
            );
        }

        println!("\nrange bad");
        for r in data
            .search_default()
            .search_field(
                field_num.clone(),
                &search::Field::Range(b"5".to_vec(), b"3".to_vec()),
            )
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await
        {
            println!(
                "{} : {}",
                r,
                std::str::from_utf8(data.field_bytes(r, &field_num)).unwrap()
            );
        }

        println!("OK");
    });
}
