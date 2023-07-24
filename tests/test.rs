#[cfg(test)]

#[test]
fn test() {
    use std::sync::Arc;

    use versatile_data::*;

    let dir="./vd-test/";
    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
    }
    if let Ok(mut data)=Data::new(dir,DataOption::default()){
        let range=1..=10;
        for i in range.clone(){
            data.update(&Operation::New(Record{
                fields:vec![
                    KeyValue::new("num",i.to_string())
                    ,KeyValue::new("num_by3",(i*3).to_string())
                    ,KeyValue::new("num_mod3",(i%3).to_string())
                ]
                ,..Default::default()
            })).unwrap();
        }
        let mut sam=0.0;
        for i in range.clone(){
            sam+=data.field_num(i,"num");
            println!(
                "{},{},{},{}"
                ,data.serial(i)
                ,std::str::from_utf8(data.field_bytes(i,"num")).unwrap()
                ,std::str::from_utf8(data.field_bytes(i,"num_by3")).unwrap()
                ,std::str::from_utf8(data.field_bytes(i,"num_mod3")).unwrap()
            );
        }
        assert_eq!(sam,55.0);

        let r=data
            .search_field("num",search::Field::Range(Arc::new(b"3".to_vec()),Arc::new(b"8".to_vec())))
            .search_default().unwrap()   //Automatic execution of the following two lines
            //.search_term(Term::In(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()))
            //.search_activity(Activity::Active)
            .result()
        ;
        println!("{:?}",r);

        let r=data
            .search_default().unwrap()
            .search(Condition::Wide(vec![
                Condition::Field("num".to_string(),search::Field::Match(Arc::new(b"4".to_vec())))
                ,Condition::Field("num".to_string(),search::Field::Match(Arc::new(b"6".to_vec())))
            ]))
            .result()
        ;
        println!("Wide test:{:?}",r);

        let r=data
            .search_default().unwrap() 
            .result_with_sort(vec![Order::Asc(OrderKey::Serial)])
        ;
        println!("sorted-serial:{:?}",r);

        let r=data
            .search_default().unwrap() 
            .result_with_sort(vec![Order::Desc(OrderKey::Field("num".to_owned()))])   //natural order
        ;
        println!("sorted-num-desc:{:?}",r);

        let r=data
            .search_default().unwrap() 
            .result_with_sort(vec![
                Order::Desc(OrderKey::Field("num_mod3".to_owned()))
            ])
        ;
        println!("sorted-mod3-desc:{:?}",r);

        let r=data
            .search_default().unwrap() 
            .result_with_sort(vec![
                Order::Asc(OrderKey::Field("num_mod3".to_owned()))
                ,Order::Asc(OrderKey::Field("num".to_owned()))
            ])
        ;
        println!("sorted mod3-asc num-asc:{:?}",r);

        let r=data
            .search_default().unwrap() 
            .result_with_sort(vec![
                Order::Asc(OrderKey::Field("num_mod3".to_owned()))
                ,Order::Desc(OrderKey::Field("num".to_owned()))
            ])
        ;
        println!("sorted mod3-asc num-desc:{:?}",r);

        let r=data
            .search_field("num",search::Field::Range(Arc::new(b"3".to_vec()),Arc::new(b"8".to_vec())))
            .search_row(search::Number::Range(4..=7))
            .search_default().unwrap()
            .result()
        ;
        println!("{:?}",r);
        
        data.update_field(2,"hoge",b"HAHA").unwrap();
        data.update_field(4,"hoge",b"agaba").unwrap();
        data.update_field(5,"hoge",b"agababi").unwrap();
        data.update_field(1,"hoge",b"ageabe").unwrap();
        data.update_field(7,"hoge",b"ageee").unwrap();
        data.update_field(6,"hoge",b"bebebe").unwrap();
        let r=data
            .search_field("hoge",search::Field::Match(Arc::new(b"HAHA".to_vec())))
            .result()
        ;
        println!("match:{:?}",r);

        let r=data
            .search_field("hoge",search::Field::Forward(Arc::new("age".to_string())))
            .result()
        ;
        println!("forward:{:?}",r);

        let r=data
            .search_field("hoge",search::Field::Partial(Arc::new("eb".to_string())))
            .result()
        ;
        println!("partial:{:?}",r);

        let r=data
            .search_field("hoge",search::Field::Backward(Arc::new("be".to_string())))
            .result()
        ;
        println!("backward:{:?}",r);

        let r=data
            .begin_search()
            .result()
        ;
        println!("all:{:?}",r);
    }
}