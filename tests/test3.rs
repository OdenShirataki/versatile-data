#[cfg(test)]
#[test]
fn test3() {
    use versatile_data::*;

    let dir = "./vd-test3/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());
    futures::executor::block_on(async {
        data.update(&Operation::New(Record {
            fields: [("test".into(), "TEST".into())].into(),
            ..Default::default()
        }))
        .await;
    });

    if let Ok(str) = std::str::from_utf8(data.field_bytes(1.try_into().unwrap(), "test")) {
        println!("FIELD:{}", str);
    }

    let data = Data::new(dir, DataOption::default());
    if let Ok(str) = std::str::from_utf8(data.field_bytes(1.try_into().unwrap(), "test")) {
        println!("FIELD:{}", str);
    }
}
