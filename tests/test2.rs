#[cfg(test)]
#[test]
fn test2() {
    use versatile_data::prelude::*;

    let dir = "./vd-test2/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    if let Ok(mut data) = Data::new(dir) {
        //1
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 1,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 2,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 3,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();

        //2
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 1,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 2,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 3,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 4,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 5,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 6,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();

        //3
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::New {
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 1,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Renamed Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 2,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Renamed Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 3,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Renamed Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 4,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 5,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 6,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Renamed Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 7,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Noah"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 8,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Liam"),
                KeyValue::new("country", "US"),
            ],
        }).unwrap();
        data.update(&Operation::Update {
            row: 9,
            activity: Activity::Active,
            term_begin: Term::Defalut,
            term_end: Term::Defalut,
            fields: vec![
                KeyValue::new("name", "Renamed Olivia"),
                KeyValue::new("country", "UK"),
            ],
        }).unwrap();
    }
}
