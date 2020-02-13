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

    let mut records = Vec::<index_db::IndexRecord>::new();
    loop {
        let mut input = String::new();

        stdin()
            .read_line(&mut input)
            .expect("failed to read from pipe");
        input = input.trim().to_string();
        if input == "" {
            break;
        }
        
        let file_hash = hash_file(&input).unwrap();
        println!("{:?} file has hash {:?}", input, file_hash);

        let new_record = index_db::IndexRecord {
            id: 0,
            checksum: file_hash,
            name: input.clone(),
            path: input.clone(),
        };

        records.push(new_record);
     }

    match index_db::insert(&records) {
        Ok(_) => println!("Record successfully inserted"),
        Err(e) => println!("Error inserting record: {:?}", e),
    };
    
    let res = index_db::select(String::from("Cargo"));
    match res {
        Ok(val) => println!("res: '{:?}'", val),
        Err(err) => println!("error parsing header: {:?}", err),
    }
}
