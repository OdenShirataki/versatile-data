# versatile-data

This is a small database that is easy to use.
It doesn't use SQL.
No table.

It has data formatted like key-value, but with rows like data in Table format. Each row can have data in key-value format.
In addition, as a field of commonly used concepts,
Activity, priority, term_begin, term_end, last_updated
is fixed for each row.

No need to design a table scheme like SQL.
You can add any column(field) at any time.

All fields are indexed for fast search.
You don't have to think about which fields to index. it is done automatically.

## Example

```rust
use versatile_data::{
    Data
    ,ConditionActivity
    ,SearchCondition
};

let dir="D:/vd-test/";

if std::path::Path::new(dir).exists(){
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
}else{
    std::fs::create_dir_all(&dir).unwrap();
}
if let Some(mut data)=Data::new(dir){
    let range=1..10;
    for i in range.clone(){
        if let Some(row)=data.insert(true,i.into(),0,0){
            data.update_field(row,"num",i.to_string());
            data.update_field(row,"num_by3",(i*3).to_string());
        }
    }
    data.update(3,false,0.0,0,0);
    data.load_fields();
    let mut sam=0.0;
    for i in range.clone(){
        sam+=data.field_num(i,"num");
        println!(
            "{},{},{},{},{},{},{}"
            ,data.activity(i)
            ,data.uuid_str(i)
            ,data.last_updated(i)
            ,data.term_begin(i)
            ,data.term_end(i)
            ,data.field_str(i,"num")
            ,data.field_str(i,"num_by3")
        );
    }
    assert_eq!(sam,45.0);

    let r=data
        .search(&Search::Field("num".to_string(),ConditionField::Range(b"3".to_vec(),b"8".to_vec())))
        .reduce_default()   //Automatic execution of the following two lines
        //.search(SearchCondition::Term(ConditionTerm::In(chrono::Local::now().timestamp())))
        //.search(SearchCondition::Activity(ConditionActivity::Active))
        .get()
    ;
    println!("{:?}",r);

    let r=data
        .search(&Search::Field("num".to_string(),ConditionField::Range(b"3".to_vec(),b"8".to_vec())))
        .search(&Search::Row(ConditionNumber::Range(4..=7)))
        .reduce_default()
        .get()
    ;
    println!("{:?}",r);
    
    data.update_field(2,"hoge","HAHA");
    data.update_field(4,"hoge","agaba");
    data.update_field(5,"hoge","agababi");
    data.update_field(1,"hoge","ageabe");
    data.update_field(7,"hoge","ageee");
    data.update_field(6,"hoge","bebebe");
    let r=data
        .search(&Search::Field("hoge".to_string(),ConditionField::Match(b"HAHA".to_vec())))
        .get()
    ;
    println!("{:?}",r);

    let r=data
        .search(&Search::Field("hoge".to_string(),ConditionField::Forward("age".to_string())))
        .get()
    ;
    println!("{:?}",r);

    let r=data
        .search(&Search::Field("hoge".to_string(),ConditionField::Partial("eb".to_string())))
        .get()
    ;
    println!("{:?}",r);

    let r=data
        .search(&Search::Field("hoge".to_string(),ConditionField::Backward("be".to_string())))
        .get()
    ;
    println!("{:?}",r);
}
```