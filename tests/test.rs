#[cfg(test)]

#[test]
fn test() {
    use versatile_data::prelude::*;

    let dir="./vd-test/";
    if std::path::Path::new(dir).exists(){
        std::fs::remove_dir_all(dir).unwrap();
    }
    if let Ok(mut data)=Data::new(dir){
        let range=1..=10;
        for i in range.clone(){
            data.update(&Operation::New{
                activity:Activity::Active
                ,term_begin:Term::Defalut
                ,term_end:Term::Defalut
                ,fields:vec![
                    KeyValue::new("num",i.to_string())
                    ,KeyValue::new("num_by3",(i*3).to_string())
                ]
            });
        }
        data.update_activity(3,Activity::Inactive);
        let mut sam=0.0;
        for i in range.clone(){
            sam+=data.field_num(i,"num");
            println!(
                "{},{},{},{},{},{},{},{}"
                ,data.serial(i)
                ,if data.activity(i)==Activity::Active{
                    "Active"
                }else{
                    "Inactive"
                }
                ,data.uuid_str(i)
                ,data.last_updated(i)
                ,data.term_begin(i)
                ,data.term_end(i)
                ,data.field_str(i,"num")
                ,data.field_str(i,"num_by3")
            );
        }
        assert_eq!(sam,55.0);

        let r=data
            .search_field("num",Field::Range(b"3".to_vec(),b"8".to_vec()))
            .search_default()   //Automatic execution of the following two lines
            //.search_term(Term::In(chrono::Local::now().timestamp()))
            //.search_activity(Activity::Active)
            .result()
        ;
        println!("{:?}",r);

        let r=data
            .search_default()
            .search(Condition::Or(vec![
                Condition::Field("num".to_string(),Field::Match(b"4".to_vec()))
                ,Condition::Field("num".to_string(),Field::Match(b"6".to_vec()))
            ]))
            .result()
        ;
        println!("Or test:{:?}",r);

        let r=data
            .search_default() 
            .result_with_sort(&Order::Serial)
        ;
        println!("sorted-serial:{:?}",r);

        let r=data
            .search_default() 
            .result_with_sort(&Order::Field("num"))   //natural order
        ;
        println!("sorted-num:{:?}",r);

        let r=data
            .search_field("num",Field::Range(b"3".to_vec(),b"8".to_vec()))
            .search_row(Number::Range(4..=7))
            .search_default()
            .result()
        ;
        println!("{:?}",r);
        
        data.update_field(2,"hoge","HAHA");
        data.update_field(4,"hoge","agaba");
        data.update_field(5,"hoge","agababi");
        data.update_field(1,"hoge","ageabe");
        data.update_field(7,"hoge","ageee");
        data.update_field(6,"hoge","bebebe");
        let r=data
            .search_field("hoge",Field::Match(b"HAHA".to_vec()))
            .result()
        ;
        println!("match:{:?}",r);

        let r=data
            .search_field("hoge",Field::Forward("age".to_string()))
            .result()
        ;
        println!("forward:{:?}",r);

        let r=data
            .search_field("hoge",Field::Partial("eb".to_string()))
            .result()
        ;
        println!("partial:{:?}",r);

        let r=data
            .search_field("hoge",Field::Backward("be".to_string()))
            .result()
        ;
        println!("backward:{:?}",r);

        let r=data
            .search_field("hoge".to_string(),Field::Backward("be".to_string()))
            .union(data.search_field("hoge",Field::Match(b"HAHA".to_vec())))
            .result()
        ;
        println!("union:{:?}",r);

        let r=data
            .begin_search()
            .result()
        ;
        println!("all:{:?}",r);
    }
}