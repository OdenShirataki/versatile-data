#[cfg(test)]
#[test]
fn test() {
    use versatile_data::*;

    let dir = "./vd-test_100k/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    let mut data = Data::new(dir, DataOption::default());
    
    futures::executor::block_on(async {
        let range = 1..=10000;
        for i in range {
            data.update(&Operation::New(Record {
                fields: vec![
                    KeyValue::new("num", i.to_string()),
                ],
                ..Default::default()
            }))
            .await;
        }
    });
    println!("OK")
}
