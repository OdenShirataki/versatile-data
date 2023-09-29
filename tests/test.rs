#[cfg(test)]
#[test]
fn test() {
    use std::sync::Arc;

    use versatile_data::*;

    let dir = "./vd-test/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    let mut data = Data::new(dir, DataOption::default());
    let range = 1..=10;
    for i in range.clone() {
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("num", i.to_string()),
                KeyValue::new("num_by3", (i * 3).to_string()),
                KeyValue::new("num_mod3", (i % 3).to_string()),
            ],
            ..Default::default()
        }));
    }
    let mut sam = 0.0;
    for i in range.clone() {
        sam += data.field_num(i.try_into().unwrap(), "num");
        println!(
            "{},{},{},{}",
            data.serial(i.try_into().unwrap()),
            std::str::from_utf8(data.field_bytes(i.try_into().unwrap(), "num")).unwrap(),
            std::str::from_utf8(data.field_bytes(i.try_into().unwrap(), "num_by3")).unwrap(),
            std::str::from_utf8(data.field_bytes(i.try_into().unwrap(), "num_mod3")).unwrap()
        );
    }
    assert_eq!(sam, 55.0);

    let r = data
        .search_field("num", &search::Field::Range(b"3".to_vec(), b"8".to_vec()))
        .search_default() //Automatic execution of the following two lines
        //.search_term(Term::In(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()))
        //.search_activity(Activity::Active)
        .result();
    println!("{:?}", r);

    let r = data
        .search_default()
        .search(Condition::Wide(&vec![
            Condition::Field("num", &search::Field::Match(b"4".to_vec())),
            Condition::Field("num", &search::Field::Match(b"6".to_vec())),
        ]))
        .result();
    println!("Wide test:{:?}", r);

    let r = data
        .search_default()
        .result_with_sort(vec![Order::Asc(OrderKey::Serial)]);
    println!("sorted-serial:{:?}", r);

    let r=data
            .search_default()
            .result_with_sort(vec![Order::Desc(OrderKey::Field("num".to_owned()))])   //natural order
        ;
    println!("sorted-num-desc:{:?}", r);

    let r = data
        .search_default()
        .result_with_sort(vec![Order::Desc(OrderKey::Field("num_mod3".to_owned()))]);
    println!("sorted-mod3-desc:{:?}", r);

    let r = data.search_default().result_with_sort(vec![
        Order::Asc(OrderKey::Field("num_mod3".to_owned())),
        Order::Asc(OrderKey::Field("num".to_owned())),
    ]);
    println!("sorted mod3-asc num-asc:{:?}", r);

    let r = data.search_default().result_with_sort(vec![
        Order::Asc(OrderKey::Field("num_mod3".to_owned())),
        Order::Desc(OrderKey::Field("num".to_owned())),
    ]);
    println!("sorted mod3-asc num-desc:{:?}", r);

    let r = data
        .search_field("num", &search::Field::Range(b"3".to_vec(), b"8".to_vec()))
        .search_row(&search::Number::Range(4..=7))
        .search_default()
        .result();
    println!("{:?}", r);

    data.update_field(2.try_into().unwrap(), "hoge", b"HAHA");
    data.update_field(4.try_into().unwrap(), "hoge", b"agaba");
    data.update_field(5.try_into().unwrap(), "hoge", b"agababi");
    data.update_field(1.try_into().unwrap(), "hoge", b"ageabe");
    data.update_field(7.try_into().unwrap(), "hoge", b"ageee");
    data.update_field(6.try_into().unwrap(), "hoge", b"bebebe");
    let r = data
        .search_field("hoge", &search::Field::Match(b"HAHA".to_vec()))
        .result();
    println!("match:{:?}", r);

    let r = data
        .search_field("hoge", &search::Field::Forward(Arc::new("age".to_string())))
        .result();
    println!("forward:{:?}", r);

    let r = data
        .search_field("hoge", &search::Field::Partial(Arc::new("eb".to_string())))
        .result();
    println!("partial:{:?}", r);

    let r = data
        .search_field("hoge", &search::Field::Backward(Arc::new("be".to_string())))
        .result();
    println!("backward:{:?}", r);

    let r = data.begin_search().result();
    println!("all:{:?}", r);
}
