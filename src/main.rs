extern crate crypto;

use crypto::digest::Digest;
use crypto::sha3::Sha3;

use std::fs::File;
use std::io::prelude::*;
use std::io::{Result, stdin};

mod index_db;


const BUFFER_SIZE: usize = 1024;


fn hash_file(file_path: &String) -> Result<String> {
    let mut file = File::open(&file_path)?;

    let mut hasher = Sha3::sha3_256();
    let mut buffer = [0u8; BUFFER_SIZE];    
    loop {
        let n = file.read(&mut buffer)?;
        hasher.input(&buffer[..n]);
        
        if n == 0 || n < BUFFER_SIZE {
            break;
        }
    }

    Ok(String::from(hasher.result_str()))
}



fn main() {
    match index_db::create() {
        Ok(_) => println!("Database initialised or verified"),
        Err(e) => println!("Error initialising database: {:?}", e),
    };

    let dummy_record = index_db::IndexRecord {
        id: 0,
        checksum: String::from("alkfjopsdfpasdfusdf9908"),
        name: String::from("Do Androids Dream of Electroinc Sheep?"),
        path: String::from("/some/path/to/params/Do Androids Dream of Electroinc Sheep?"),
    };

    // let records = [dummy_record];
    // match index_db::insert(&records) {
    //     Ok(_) => println!("Record successfully inserted"),
    //     Err(e) => println!("Error inserting record: {:?}", e),
    // };

    loop {
        let mut input = String::new();

        stdin()
            .read_line(&mut input)
            .expect("failed to read from pipe");
        input = input.trim().to_string();
        if input == "" {
            break;
        }
        
        match hash_file(&input) {
            Ok(file_hash) => println!("{}  {}", file_hash, input),
            Err(e) => println!("Error processing file {} {}", input.as_str(), e),
        }
     }

    let res = index_db::select(String::from("Dream"));
    match res {
        Ok(val) => println!("res: '{:?}'", val),
        Err(err) => println!("error parsing header: {:?}", err),
    }
}
