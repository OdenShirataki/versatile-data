#[cfg(test)]
#[test]
fn test3() {
    use versatile_data::*;

    let dir = "./vd-test3/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());
    let field_test = FieldName::from("test");
    futures::executor::block_on(async {
        data.insert(
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_test.clone(), "TEST".into())].into(),
        )
        .await;
    });

    if let Ok(str) = std::str::from_utf8(data.field_bytes(1.try_into().unwrap(), &field_test)) {
        assert_eq!("TEST", str);
    }

    let data = Data::new(dir, DataOption::default());
    if let Ok(str) = std::str::from_utf8(data.field_bytes(1.try_into().unwrap(), &field_test)) {
        assert_eq!("TEST", str);
    }
}
