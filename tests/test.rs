use versatile_data::{
    Data
    ,ConditionField
    ,SearchCondition
};

#[test]
fn test() {
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
            if let Some(id)=data.insert(true,i.into(),0,0){
                data.update_field(id,"num",i.to_string());
                data.update_field(id,"num_by3",(i*3).to_string());
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
            .search(SearchCondition::Field("num",ConditionField::Range(b"3",b"8")))
            .reduce_default()   //Automatic execution of the following two lines
            //.search(SearchCondition::Term(ConditionTerm::In(chrono::Local::now().timestamp())))
            //.search(SearchCondition::Activity(ConditionActivity::Active))
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
            .search(SearchCondition::Field("hoge",ConditionField::Match(b"HAHA")))
            .get()
        ;
        println!("{:?}",r);

        let r=data
            .search(SearchCondition::Field("hoge",ConditionField::Forward("age")))
            .get()
        ;
        println!("{:?}",r);

        let r=data
            .search(SearchCondition::Field("hoge",ConditionField::Partial("eb")))
            .get()
        ;
        println!("{:?}",r);

        let r=data
            .search(SearchCondition::Field("hoge",ConditionField::Backward("be")))
            .get()
        ;
        println!("{:?}",r);
    }
}