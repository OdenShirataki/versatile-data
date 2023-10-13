#[cfg(test)]
#[test]
fn test() {
    use versatile_data::*;

    let dir = "./vd-test_100k/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    let mut data = Data::new(
        dir,
        DataOption {
            allocation_lot: 10000,
            ..Default::default()
        },
    );

    futures::executor::block_on(async {
        let range = 1u32..=100000;
        for i in range {
            data.update(&Operation::Update {
                row: i,
                record: Record {
                    fields: [("num".into(), i.to_string().into())].into(),
                    ..Default::default()
                },
            })
            .await;
        }
    });
    println!("OK")
}
