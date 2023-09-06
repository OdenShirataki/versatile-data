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
    sam += data.field_num(i, "num");
    println!(
        "{},{},{},{}",
        data.serial(i),
        std::str::from_utf8(data.field_bytes(i, "num")).unwrap(),
        std::str::from_utf8(data.field_bytes(i, "num_by3")).unwrap(),
        std::str::from_utf8(data.field_bytes(i, "num_mod3")).unwrap()
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

data.update_field(2, "hoge", b"HAHA");
data.update_field(4, "hoge", b"agaba");
data.update_field(5, "hoge", b"agababi");
data.update_field(1, "hoge", b"ageabe");
data.update_field(7, "hoge", b"ageee");
data.update_field(6, "hoge", b"bebebe");
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
```