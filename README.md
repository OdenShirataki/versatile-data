# versatile-data

This is a small database that is easy to use.
It doesn't use SQL.
No table.

It has data formatted like key-value, but with rows like data in Table format. Each row can have data in key-value format.
In addition, as a field of commonly used concepts,
Activity, term_begin, term_end, last_updated
is fixed for each row.

No need to design a table scheme like SQL.
You can add any column(field) at any time.

All fields are indexed for fast search.
You don't have to think about which fields to index. it is done automatically.

## Example

```rust
use versatile_data::*;

let dir = "./vd-test/";
if std::path::Path::new(dir).exists() {
    std::fs::remove_dir_all(dir).unwrap();
}
let mut data = Data::new(dir, DataOption::default());
let range = 1..=10;
futures::executor::block_on(async {
    for i in range.clone() {
        data.update(Operation::New(Record {
            fields: [
                ("num".into(), i.to_string().into()),
                ("num_by3".into(), (i * 3).to_string().into()),
                ("num_mod3".into(), (i % 3).to_string().into()),
            ]
            .into(),
            ..Default::default()
        }))
        .await;
    }
    let mut sam = 0.0;

    for i in range {
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
        .result()
        .await;
    println!("{:?}", r);

    let r = data
        .search_default()
        .search(Condition::Wide(&vec![
            Condition::Field("num", &search::Field::Match(b"4".to_vec())),
            Condition::Field("num", &search::Field::Match(b"6".to_vec())),
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
        .result_with_sort(vec![Order::Desc(OrderKey::Field("num".to_owned()))]).await   //natural order
    ;
    println!("sorted-num-desc:{:?}", r);

    let r = data
        .search_default()
        .result_with_sort(vec![Order::Desc(OrderKey::Field("num_mod3".to_owned()))])
        .await;
    println!("sorted-mod3-desc:{:?}", r);

    let r = data
        .search_default()
        .result_with_sort(vec![
            Order::Asc(OrderKey::Field("num_mod3".to_owned())),
            Order::Asc(OrderKey::Field("num".to_owned())),
        ])
        .await;
    println!("sorted mod3-asc num-asc:{:?}", r);

    let r = data
        .search_default()
        .result_with_sort(vec![
            Order::Asc(OrderKey::Field("num_mod3".to_owned())),
            Order::Desc(OrderKey::Field("num".to_owned())),
        ])
        .await;
    println!("sorted mod3-asc num-desc:{:?}", r);

    let r = data
        .search_field("num", &search::Field::Range(b"3".to_vec(), b"8".to_vec()))
        .search_row(&search::Number::Range(4..=7))
        .search_default()
        .result()
        .await;
    println!("{:?}", r);

    data.update(Operation::Update {
        row: 2.try_into().unwrap(),
        record: Record {
            fields: [("hoge".into(), "HAHA".into())].into(),
            ..Default::default()
        },
    })
    .await;

    data.update(Operation::Update {
        row: 4.try_into().unwrap(),
        record: Record {
            fields: [("hoge".into(), "agaba".into())].into(),
            ..Default::default()
        },
    })
    .await;
    data.update(Operation::Update {
        row: 5.try_into().unwrap(),
        record: Record {
            fields: [("hoge".into(), "agababi".into())].into(),
            ..Default::default()
        },
    })
    .await;
    data.update(Operation::Update {
        row: 1.try_into().unwrap(),
        record: Record {
            fields: [("hoge".into(), "ageabe".into())].into(),
            ..Default::default()
        },
    })
    .await;
    data.update(Operation::Update {
        row: 7.try_into().unwrap(),
        record: Record {
            fields: [("hoge".into(), "ageee".into())].into(),
            ..Default::default()
        },
    })
    .await;
    data.update(Operation::Update {
        row: 6.try_into().unwrap(),
        record: Record {
            fields: [("hoge".into(), "bebebe".into())].into(),
            ..Default::default()
        },
    })
    .await;

    let r = data
        .search_field("hoge", &search::Field::Match(b"HAHA".to_vec()))
        .result()
        .await;
    println!("match:{:?}", r);

    let r = data
        .search_field("hoge", &search::Field::Forward("age".to_string()))
        .result()
        .await;
    println!("forward:{:?}", r);

    let r = data
        .search_field("hoge", &search::Field::Partial("eb".to_string()))
        .result()
        .await;
    println!("partial:{:?}", r);

    let r = data
        .search_field("hoge", &search::Field::Backward("be".to_string()))
        .result()
        .await;
    println!("backward:{:?}", r);

    let r = data.begin_search().result().await;
    println!("all:{:?}", r);
});
```