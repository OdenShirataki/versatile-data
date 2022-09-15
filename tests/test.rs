#[test]
fn test() {
    use versatile_data::Data;

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
            if let Some(id)=data.insert(1,i.into(),0,0){
                data.update_field(id,"num",i.to_string());
                data.update_field(id,"num_by3",(i*3).to_string());
            }
        }
        data.load_fields();
        for i in range.clone(){
            println!(
                "{},{},{},{},{},{},{}"
                ,data.activity(i).unwrap()
                ,data.uuid_str(i).unwrap()
                ,data.last_updated(i).unwrap()
                ,data.term_begin(i).unwrap()
                ,data.term_end(i).unwrap()
                ,data.field_value(i,"num").unwrap()
                ,data.field_value(i,"num_by3").unwrap()
            );
        }
    }
}