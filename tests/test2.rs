#[cfg(test)]
#[test]
fn test2() {
    use versatile_data::*;

    let dir = "./vd-test2/";
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).unwrap();
    }
    let mut data = Data::new(dir, DataOption::default());
    //1
    futures::executor::block_on(async {
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Noah"),
                KeyValue::new("country", "US"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Liam"),
                KeyValue::new("country", "US"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Olivia"),
                KeyValue::new("country", "UK"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::Update {
            row: 1,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Noah"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 2,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Liam"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 3,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Olivia"),
                    KeyValue::new("country", "UK"),
                ],
                ..Default::default()
            },
        })
        .await;

        //2
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Noah"),
                KeyValue::new("country", "US"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Liam"),
                KeyValue::new("country", "US"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Olivia"),
                KeyValue::new("country", "UK"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::Update {
            row: 1,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Noah"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 2,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Liam"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 3,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Olivia"),
                    KeyValue::new("country", "UK"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 4,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Noah"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 5,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Liam"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 6,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Olivia"),
                    KeyValue::new("country", "UK"),
                ],
                ..Default::default()
            },
        })
        .await;

        //3
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Noah"),
                KeyValue::new("country", "US"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Liam"),
                KeyValue::new("country", "US"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::New(Record {
            fields: vec![
                KeyValue::new("name", "Olivia"),
                KeyValue::new("country", "UK"),
            ],
            ..Default::default()
        }))
        .await;
        data.update(&Operation::Update {
            row: 1,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Renamed Noah"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 2,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Renamed Liam"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 3,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Renamed Olivia"),
                    KeyValue::new("country", "UK"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 4,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Noah"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 5,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Liam"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 6,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Renamed Olivia"),
                    KeyValue::new("country", "UK"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 7,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Noah"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 8,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Liam"),
                    KeyValue::new("country", "US"),
                ],
                ..Default::default()
            },
        })
        .await;
        data.update(&Operation::Update {
            row: 9,
            record: Record {
                fields: vec![
                    KeyValue::new("name", "Renamed Olivia"),
                    KeyValue::new("country", "UK"),
                ],
                ..Default::default()
            },
        })
        .await;
    });
}
