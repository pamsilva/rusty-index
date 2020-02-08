mod index_db;

fn main() {
    match index_db::create() {
        Ok(_) => println!("Database initialised or verified"),
        Err(e) => println!("Error initialising database: {:?}", e),
    };

    let dummy_record = index_db::IndexRecord {
        checksum: String::from("alkfjopsdfpasdfusdf9908"),
        name: String::from("Do Androids Dream of Electroinc Sheep?"),
        path: String::from("/some/path/to/params/Do Androids Dream of Electroinc Sheep?"),
    };

    let records = [dummy_record];
    // insert(&records)?;

    let res = index_db::select(String::from("Dream"));
    match res {
        Ok(val) => println!("res: '{:?}'", val),
        Err(err) => println!("error parsing header: {:?}", err),
    }
    
}
