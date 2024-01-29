#[cfg(test)]
#[test]
fn test() {
    use std::num::NonZeroU32;

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
        let id_num = FieldName::new("num".into());
        for i in range {
            data.update(
                unsafe { NonZeroU32::new_unchecked(i) },
                Activity::Active,
                Term::Default,
                Term::Default,
                [(id_num.clone(), i.to_string().into())].into(),
            )
            .await;
        }
    });
    println!("OK")
}
