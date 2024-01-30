#[cfg(test)]
#[test]
fn test() {
    use std::{num::NonZeroU32, sync::Arc};

    use versatile_data::*;

    let dir = "./vd-test/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    let mut data = Data::new(dir, DataOption::default());
    let range = 1..=10;
    futures::executor::block_on(async {
        let field_num = FieldName::new("num".into());
        let field_num_by3 = FieldName::new("num_by3".into());
        let field_num_mod3 = FieldName::new("num_mod3".into());

        let field_hoge = FieldName::new("hoge".into());

        for i in range.clone() {
            data.insert(
                Activity::Active,
                Term::Default,
                Term::Default,
                [
                    (field_num.clone(), i.to_string().into()),
                    (field_num_by3.clone(), (i * 3).to_string().into()),
                    (field_num_mod3.clone(), (i % 3).to_string().into()),
                ]
                .into(),
            )
            .await;
        }
        let mut sam = 0.0;

        for i in range {
            sam += data.field_num(i.try_into().unwrap(), &field_num);
            println!(
                "{},{},{},{}",
                data.serial(i.try_into().unwrap()),
                std::str::from_utf8(data.field_bytes(i.try_into().unwrap(), &field_num)).unwrap(),
                std::str::from_utf8(data.field_bytes(i.try_into().unwrap(), &field_num_by3))
                    .unwrap(),
                std::str::from_utf8(data.field_bytes(i.try_into().unwrap(), &field_num_mod3))
                    .unwrap()
            );
        }

        assert_eq!(sam, 55.0);

        let r = data
            .search_field(
                field_num.clone(),
                &search::Field::Range(b"3".to_vec(), b"8".to_vec()),
            )
            .search_default() //Automatic execution of the following two lines
            //.search_term(Term::In(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()))
            //.search_activity(Activity::Active)
            .result()
            .await;
        println!("{:?}", r);

        let r = data
            .search_default()
            .search(Condition::Wide(&vec![
                Condition::Field(field_num.clone(), &search::Field::Match(b"4".to_vec())),
                Condition::Field(field_num.clone(), &search::Field::Match(b"6".to_vec())),
            ]))
            .result()
            .await;
        println!("Wide test:{:?}", r);

        let r = data
            .search_default()
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
            .await;
        println!("sorted-serial:{:?}", r);

        let r=data
            .search_default()
            .result_with_sort(vec![Order::Desc(OrderKey::Field(field_num.clone()))]).await   //natural order
        ;
        println!("sorted-num-desc:{:?}", r);

        let r = data
            .search_default()
            .result_with_sort(vec![Order::Desc(OrderKey::Field(field_num_mod3.clone()))])
            .await;
        println!("sorted-mod3-desc:{:?}", r);

        let r = data
            .search_default()
            .result_with_sort(vec![
                Order::Asc(OrderKey::Field(field_num_mod3.clone())),
                Order::Asc(OrderKey::Field(field_num.clone())),
            ])
            .await;
        println!("sorted mod3-asc num-asc:{:?}", r);

        let r = data
            .search_default()
            .result_with_sort(vec![
                Order::Asc(OrderKey::Field(field_num_mod3.clone())),
                Order::Desc(OrderKey::Field(field_num.clone())),
            ])
            .await;
        println!("sorted mod3-asc num-desc:{:?}", r);

        let r = data
            .search_field(
                field_num,
                &search::Field::Range(b"3".to_vec(), b"8".to_vec()),
            )
            .search_row(&search::Number::Range(4..=7))
            .search_default()
            .result()
            .await;
        println!("{:?}", r);

        data.update(
            unsafe { NonZeroU32::new_unchecked(2) },
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_hoge.clone(), "HAHA".into())].into(),
        )
        .await;

        data.update(
            unsafe { NonZeroU32::new_unchecked(4) },
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_hoge.clone(), "agaba".into())].into(),
        )
        .await;
        data.update(
            unsafe { NonZeroU32::new_unchecked(5) },
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_hoge.clone(), "agababi".into())].into(),
        )
        .await;
        data.update(
            unsafe { NonZeroU32::new_unchecked(1) },
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_hoge.clone(), "ageabe".into())].into(),
        )
        .await;
        data.update(
            unsafe { NonZeroU32::new_unchecked(7) },
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_hoge.clone(), "ageee".into())].into(),
        )
        .await;
        data.update(
            unsafe { NonZeroU32::new_unchecked(6) },
            Activity::Active,
            Term::Default,
            Term::Default,
            [(field_hoge.clone(), "bebebe".into())].into(),
        )
        .await;

        let r = data
            .search_field(field_hoge.clone(), &search::Field::Match(b"HAHA".to_vec()))
            .result()
            .await;
        println!("match:{:?}", r);

        let r = data
            .search_field(
                field_hoge.clone(),
                &search::Field::Forward(Arc::new("age".into())),
            )
            .result()
            .await;
        println!("forward:{:?}", r);

        let r = data
            .search_field(
                field_hoge.clone(),
                &search::Field::Partial(Arc::new("eb".into())),
            )
            .result()
            .await;
        println!("partial:{:?}", r);

        let r = data
            .search_field(field_hoge, &search::Field::Backward(Arc::new("be".into())))
            .result()
            .await;
        println!("backward:{:?}", r);

        let r = data.begin_search().result().await;
        println!("all:{:?}", r);
    });
}
