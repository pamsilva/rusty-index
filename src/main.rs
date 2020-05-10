use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::{Result, stdin};
use std::sync::mpsc::channel;

extern crate num_cpus;

extern crate threadpool;
use threadpool::ThreadPool;

extern crate crypto;
use crypto::digest::Digest;
use crypto::md5::Md5;

extern crate petgraph;
use petgraph::dot::Dot;

mod index_db;
use index_db::IndexStorage;

mod analyser;

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


fn get_name_and_path(pwd: &String, file_name: &String) -> (String, String){
    let mut relevant_file_name: String = file_name.clone();

    if file_name.starts_with("./") {
        let (_, new_file_name) = file_name.split_at(2);
        relevant_file_name = String::from(new_file_name);
    }
    
    let mut real_path = format!("{}/{}", pwd, relevant_file_name);
    if file_name.starts_with("/") {
        real_path = relevant_file_name;
    }
    
    let components: Vec<&str> = real_path.rsplitn(2, '/').collect();
    return (String::from(components[1]), String::from(components[0]));
}


fn main() {
    let pool = ThreadPool::new(num_cpus::get());
    let file_name = String::from("index.db");
    let data_source = index_db::initalise_db(&file_name).unwrap();

    match data_source.create() {
        Ok(_) => println!("Database initialised or verified"),
        Err(e) => println!("Error initialising database: {:?}", e),
    };
    
    let current_dir = String::from(
        env::current_dir().unwrap().into_os_string().into_string().unwrap()
    );

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
    println!("Files to process: {:#?}", files);

    let (tx, rx) = channel();

    for file in files {
        let tx = tx.clone();
        let local_current_dir = current_dir.clone();
        pool.execute(move || {
            let file_hash = hash_file(&file).unwrap();
            println!("{:?} file has hash {:?}", file, file_hash);
            let (path, file_name) = get_name_and_path(&local_current_dir, &file);
            
            let new_record = index_db::IndexRecord {
                id: 0,
                checksum: file_hash,
                name: String::from(file_name),
                path: String::from(path),
            };

            tx.send(new_record).expect("Could not send data!");
        })
    }

    println!("Finished spanning. Dropping connection ...");
    drop(tx);
    
    println!("Dropped, now saving.");
    let mut records = Vec::<index_db::IndexRecord>::new();
    for r in rx.iter() {
        records.push(r);
    }

    println!("Inserting Data.");
    match data_source.insert(&records) {
        Ok(_) => println!("Records successfully inserted"),
        Err(e) => println!("Error inserting records: {:?}", e),
    };

    println!("Saving to database. Enjoy.");
    
    let res = data_source.fetch_sorted().unwrap();
    let graph = analyser::process_entries(res);
    let mut f = File::create("example1.dot").unwrap();
    let output = format!("{}", Dot::new(&graph));

    match f.write_all(&output.as_bytes()){
        Ok(_) => println!("All done. Have a nice day in the world."),
        Err(e) => println!("Error writing to file {:?}", e),
    }
}
