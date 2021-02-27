use std::fs::{metadata, read_dir, File};
use std::io::prelude::*;
use std::io::{stdin, Result};
use std::path::Component::{Normal, RootDir};
use std::path::{Path, PathBuf};
use std::env;
use std::sync::mpsc::channel;
use std::time::SystemTime;

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


pub fn get_current_dir() -> String {
    let path = env::current_dir().expect("Couldn't get current dir ...");
    String::from(path.to_str().expect("Couldn't transform current dir into string ..."))
}


fn hash_file<T: AsRef<Path>>(file_path: &T) -> Result<String> {
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


fn load_files_from_stdin() -> Vec<String> {
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


fn process_into_file_records(file_list: Vec<String>) -> Vec<analyser::FileRecord> {
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


pub fn load_and_process_files() -> Vec<analyser::FileRecord> {
    let raw_files = load_files_from_stdin();
    let files = process_file_paths(raw_files);

    println!("Processing {} files ...", files.len());
    return process_into_file_records(files);
}


fn process_file(path: &Path) -> analyser::FileRecord {
    let file_hash = hash_file(&path).unwrap();
    let file_name = path
        .file_name()
        .expect("Could not get file name from path.")
        .to_str()
        .expect("Cloud not convert file name to string");
    let str_path = path
        .parent()
        .expect("Cloud not extract the lain path from the file path.")
        .components()
        .map(|x| match x {
            Normal(val) => String::from(
                val.to_str()
                    .expect("Could not convert path component into string"),
            ),
            RootDir => String::from(""),
            _ => panic!("Could not process path component."),
        })
        .collect();

    let metadata = match path.metadata() {
        Ok(m_tada) => m_tada,
        Err(e) => panic!("Can't get metadata for file {:?}; {:?}", &file_name, e),
    };
    let timestamp = match metadata.modified() {
        Ok(time) => time,
        Err(_e) => SystemTime::now(),
    };
    let modified: DateTime<Utc> = timestamp.into();
    
    analyser::FileRecord {
        checksum: file_hash,
        name: String::from(file_name),
        path: str_path,
        modified,
    }
}


fn process_directory(path:& Path) -> Vec<analyser::FileRecord> {
    let str_path = match path.to_str() {
	Some(s) => String::from(s),
	None => panic!("Emptt path."),
    };
    let entries = match read_dir(path) {
	Ok(x) => x,
	Err(e) => panic!("Could not read directory {}: {}", str_path, e),
    };

    let mut results = Vec::new();
    for entry in entries {
	let sub_path = match entry {
	    Ok(dir_entry) => dir_entry.path(),
	    Err(e) => panic!("Cannot get path for entries under {}: {}", str_path, e),
	};

	if sub_path.is_dir() {
	    results.push(process_directory(&sub_path));
	    
	} else {
	    results.push(vec![process_file(&sub_path)]);
	}
    }

    results.concat()
}


pub fn scan_directory(base_path: String) -> Vec<analyser::FileRecord> {
    let path = Path::new(&base_path);
    let mut results = Vec::new();
    
    if path.is_dir() {
	results.push(process_directory(&path));
    } else {
        results.push(vec![process_file(&path)]);
    }

    results.concat()
}


fn simple_process_directory(path:& Path) -> Vec<PathBuf> {
    let str_path = match path.to_str() {
	Some(s) => String::from(s),
	None => panic!("Emptt path."),
    };
    let entries = match read_dir(path) {
	Ok(x) => x,
	Err(e) => panic!("Could not read directory {}: {}", str_path, e),
    };

    let mut results = Vec::new();
    for entry in entries {
	let sub_path = match entry {
	    Ok(dir_entry) => dir_entry.path(),
	    Err(e) => panic!("Cannot get path for entries under {}: {}", str_path, e),
	};

	if sub_path.is_dir() {
	    results.push(simple_process_directory(&sub_path));
	    
	} else {
            results.push(vec![PathBuf::from(sub_path)]);
	}
    }

    results.concat()
}


pub fn simple_scan_directory(base_path: String) -> Vec<PathBuf> {
    let path = Path::new(&base_path);
    let mut results = Vec::new();
    
    if path.is_dir() {
	results.push(simple_process_directory(&path));
    } else {
        results.push(vec![PathBuf::from(path)]);
    }

    results.concat()
}
