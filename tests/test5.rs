#[cfg(test)]
#[test]
fn test5() {
    use std::sync::Arc;

    use versatile_data::*;

    let dir = "./vd-test5/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }

    let mut data = Data::new(dir, DataOption::default());
    data.update(&Operation::New(Record {
        fields: vec![KeyValue::new("num", "2".to_owned())],
        ..Default::default()
    }));
    data.update(&Operation::New(Record {
        fields: vec![KeyValue::new("num", "2".to_owned())],
        ..Default::default()
    }));
    data.update(&Operation::New(Record {
        fields: vec![KeyValue::new("num", "3".to_owned())],
        ..Default::default()
    }));
    data.update(&Operation::New(Record {
        fields: vec![KeyValue::new("num", "5".to_owned())],
        ..Default::default()
    }));
    data.update(&Operation::New(Record {
        fields: vec![KeyValue::new("num", "8".to_owned())],
        ..Default::default()
    }));

    println!("\nmatch");
    for r in data
        .search_default()
        .search_field("num", search::Field::Match(Arc::new(b"3".to_vec())))
        .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
        .unwrap()
    {
        println!(
            "{} : {}",
            r,
            std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
        );
    }

    println!("\nmin");
    for r in data
        .search_default()
        .search_field("num", search::Field::Min(Arc::new(b"3".to_vec())))
        .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
        .unwrap()
    {
        println!(
            "{} : {}",
            r,
            std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
        );
    }
    println!("\nmax");
    for r in data
        .search_default()
        .search_field("num", search::Field::Max(Arc::new(b"3".to_vec())))
        .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
        .unwrap()
    {
        println!(
            "{} : {}",
            r,
            std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
        );
    }

    println!("\nrange");
    for r in data
        .search_default()
        .search_field(
            "num",
            search::Field::Range(Arc::new(b"3".to_vec()), Arc::new(b"5".to_vec())),
        )
        .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
        .unwrap()
    {
        println!(
            "{} : {}",
            r,
            std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
        );
    }

    println!("\nrange bad");
    for r in data
        .search_default()
        .search_field(
            "num",
            search::Field::Range(Arc::new(b"5".to_vec()), Arc::new(b"3".to_vec())),
        )
        .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
        .unwrap()
    {
        println!(
            "{} : {}",
            r,
            std::str::from_utf8(data.field_bytes(r, "num")).unwrap()
        );
    }

    println!("OK");
}
