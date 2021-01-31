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

extern crate petgraph;
use petgraph::dot::Dot;

extern crate clap;
use clap::{App, SubCommand};

mod index_db;
use index_db::{IndexStorage, IndexRecord};

mod analyser;
use analyser::{GraphStorageInterface, FileRecord};

mod misc;
use misc::to_file_record;
use misc::to_index_record;
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


fn load_and_process_files() -> Vec::<analyser::FileRecord> {
    let raw_files = load_files_from_stdin();
    let files = process_file_paths(raw_files);
    
    println!("Processing {} files ...", files.len());
    return process_into_file_records(files);
}


fn export_graph(graph: &analyser::GraphStorage) {
    let mut f = File::create("example1.dot").unwrap();
    let output = format!("{:?}", Dot::new(&graph.graph));

    println!("Writing dot file with final results.");
    match f.write_all(&output.as_bytes()){
        Ok(_) => println!("All done. Have a nice day in the world."),
        Err(e) => println!("Error writing to file {:?}", e),
    }
}


fn main() {
    let config = App::new("rusty-index")
        .subcommand(SubCommand::with_name("parse"))
        .subcommand(SubCommand::with_name("generate"))
        .subcommand(SubCommand::with_name("virtual"))
        .subcommand(SubCommand::with_name("layered-virtual"))
        .get_matches();

    let file_name = String::from("index.db");
    let data_source = index_db::initalise_db(&file_name).unwrap();

    match data_source.create() {
        Ok(_) => println!("Database initialised or verified"),
        Err(e) => println!("Error initialising database: {:?}", e),
    };
    
    if let Some(_matches) = config.subcommand_matches("parse") {
        let records = load_and_process_files();
        let storage_records: Vec<IndexRecord> = records.into_iter().map(|x| to_index_record(&x)).collect();

        println!("Saving {} into the database.", storage_records.len());
        match data_source.insert(&storage_records) {
            Ok(_) => println!("Records successfully inserted"),
            Err(e) => println!("Error inserting records: {:?}", e),
        };
        
    } else if let Some(_matches) = config.subcommand_matches("generate") {
        let res = data_source.fetch_sorted().unwrap();
        let mut graph = analyser::initialise_graph();
        
        let file_records_res: Vec<FileRecord> = res.into_iter().map(|x| to_file_record(&x)).collect();
	println!("Processing {} from the database.", file_records_res.len());
	
        graph.bulk_insert(file_records_res);
        export_graph(&graph);
        
    } else if let Some(_matches) = config.subcommand_matches("virtual") {
        let records = load_and_process_files();
        
        println!("Dropped, now saving.");
        let mut graph = analyser::initialise_graph();
        graph.insert(records);

        export_graph(&graph);

        let final_res = graph.find_duplicates();
        println!("The final result: {:#?}", final_res);

    } else if let Some(_matches) = config.subcommand_matches("layered-virtual") {
        let records = load_and_process_files();
        println!("Dropped, now saving.");
        
        // let mut graph = analyser::initialise_graph();
        // let mut root = graph.root;
        // graph.bulk_insert(&mut root, records);

        let graph_ref = analyser::create_shared_graph();
        let local_ref = graph_ref.clone();
        let mut root = local_ref.lock().unwrap().root;

        let first_ref = graph_ref.clone();
        analyser::parallel_bulk_insert(first_ref, &mut root, records);
        let graph = local_ref.lock().unwrap();
        
        export_graph(&graph);
        
        let final_res = graph.find_duplicates();
        println!("The final result: {:#?}", final_res);
        
    } else {
        println!("You need to either parse or generate, otherwise there is nothing to do.");
    }
}

