use std::fs::{File, metadata};
use std::time::SystemTime;
use std::io::prelude::*;
use std::io::{Result, stdin};
use std::sync::mpsc::channel;

extern crate chrono;
use chrono::{DateTime, Utc};

extern crate num_cpus;

extern crate threadpool;
use threadpool::ThreadPool;

extern crate crypto;
use crypto::digest::Digest;
use crypto::md5::Md5;

use crate::analyser;

use crate::misc;
use misc::get_name_and_split_path;
use misc::process_file_paths;

const BUFFER_SIZE: usize = 1024;


fn hash_file(file_path: &String) -> Result<String> {
    let mut file = File::open(&file_path)?;

    let mut hasher = Md5::new();
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


fn load_files_from_stdin() -> Vec::<String> {
    let mut files = Vec::<String>::new();
    loop {
        let mut input = String::new();

        stdin()
            .read_line(&mut input)
            .expect("failed to read from pipe");
        input = input.trim().to_string();
        if input == "" {
            break;
        }

        files.push(input);
    }

    return files;
}


fn process_into_file_records(file_list: Vec::<String>) -> Vec::<analyser::FileRecord> {
    let n_cpus = num_cpus::get();
    let pool = ThreadPool::new(n_cpus);
    println!("Running with {} threads ...", n_cpus);
    
    let (tx, rx) = channel();

    for file in file_list {
        let tx = tx.clone();
        
        pool.execute(move || {
            let file_hash = hash_file(&file).unwrap();
	    // println!("processing {} ...", file);
	    
            let (path, file_name) = get_name_and_split_path(&file);
            let metadata = match metadata(&file) {
                Ok(m_tada) => m_tada,
                Err(e) => panic!("Can't get metadata for file {:?}; {:?}", &file, e),
            };
            let timestamp = match metadata.modified() {
                Ok(time) => time,
                Err(_e) => SystemTime::now(),
            };

	    let modified: DateTime<Utc> = timestamp.into();
            let new_record = analyser::FileRecord {
                checksum: file_hash,
                name: String::from(file_name),
                path,
                modified,
            };

            tx.send(new_record).expect("Could not send data!");
        })
    }

    println!("Finished spanning. Dropping connection ...");
    drop(tx);
    
    let mut records = Vec::<analyser::FileRecord>::new();
    for r in rx.iter() {
        records.push(r);
    }

    return records;
}


pub fn load_and_process_files() -> Vec::<analyser::FileRecord> {
    let raw_files = load_files_from_stdin();
    let files = process_file_paths(raw_files);
    
    println!("Processing {} files ...", files.len());
    return process_into_file_records(files);
}
