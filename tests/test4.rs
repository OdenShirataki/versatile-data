#[cfg(test)]
#[test]
fn test4() {
    use versatile_data::*;

    let dir = "./vd-test4/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());

    let field_name = FieldName::new("name".into());
    let field_password = FieldName::new("password".into());

    futures::executor::block_on(async {
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [
                (field_name.clone(), "test".into()),
                (field_password.clone(), "test".into()),
            ]
            .into(),
        )
        .await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [
                (field_name.clone(), "test2".into()),
                (field_password.clone(), "test".into()),
            ]
            .into(),
        )
        .await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [
                (field_name.clone(), "test3".into()),
                (field_password.clone(), "test".into()),
            ]
            .into(),
        )
        .await;
        data.delete(2.try_into().unwrap()).await;
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [
                (field_name.clone(), "test4".into()),
                (field_password.clone(), "test".into()),
            ]
            .into(),
        )
        .await;
        let r = data
            .search_default()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await;
        println!("{:?}", r);

        let mut data = Data::new(dir, DataOption::default());
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [
                (field_name.clone(), "test5".into()),
                (field_password.clone(), "test".into()),
            ]
            .into(),
        )
        .await;
        let r = data
            .search_default()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await;
        println!("{:?}", r);
    });

    println!("OK");
}
