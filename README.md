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
use versatile_data::prelude::*;

let dir="./vd-test/";
if std::path::Path::new(dir).exists(){
    std::fs::remove_dir_all(dir).unwrap();
}
if let Ok(mut data)=Data::new(dir){
    let range=1..=10;
    for i in range.clone(){
        data.update(Update::New,Activity::Active,0,0,&vec![
            ("num",i.to_string())
            ,("num_by3",(i*3).to_string())
        ]);
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
        .search(Condition::Field("num".to_string(),Field::Range(b"3".to_vec(),b"8".to_vec())))
        .search_default()   //Automatic execution of the following two lines
        //.search(Condition::Term(Term::In(chrono::Local::now().timestamp())))
        //.search(Condition::Activity(Activity::Active))
        .result()
    ;
    println!("{:?}",r);

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
        .search(Condition::Field("num".to_string(),Field::Range(b"3".to_vec(),b"8".to_vec())))
        .search(Condition::Row(Number::Range(4..=7)))
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
        .search(Condition::Field("hoge".to_string(),Field::Match(b"HAHA".to_vec())))
        .result()
    ;
    println!("match:{:?}",r);

    let r=data
        .search(Condition::Field("hoge".to_string(),Field::Forward("age".to_string())))
        .result()
    ;
    println!("forward:{:?}",r);

    let r=data
        .search(Condition::Field("hoge".to_string(),Field::Partial("eb".to_string())))
        .result()
    ;
    println!("partial:{:?}",r);

    let r=data
        .search(Condition::Field("hoge".to_string(),Field::Backward("be".to_string())))
        .result()
    ;
    println!("backward:{:?}",r);

    let r=data
        .search(Condition::Field("hoge".to_string(),Field::Backward("be".to_string())))
        .union(data.search(Condition::Field("hoge".to_string(),Field::Match(b"HAHA".to_vec()))))
        .result()
    ;
    println!("union:{:?}",r);

    let r=data
        .begin_search()
        .result()
    ;
    println!("all:{:?}",r);
}
```