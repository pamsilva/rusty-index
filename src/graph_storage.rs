
// pub trait PersistentGraph {
//     // fn push_node(&self, node_entry: &FileRecord);
//     // fn push_leaf(&self, node_entry: &FileRecord);
//     fn show(&self);
// }


// fn process_file<T: PersistentGraph>(graph: &T, path: &Path) {
// }


// fn process_dir<T: PersistentGraph>(graph: &T, path: &Path) {
// }


// fn process_base_dir<T: PersistentGraph>(graph: &T, target_path: String) {
//     let shared_queue: Arc::new(Mutex::new(
// 	VecDequeue<(T, Path)>
//     ));

//     let n_cpus = num_cpus::get();
//     let pool = ThreadPool::new(n_cpus);
//     println!("Running with {} threads ...", n_cpus);

//     let (tx, rx) = channel();

    
    
// }

use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::read_dir;
use std::sync::mpsc::channel;

extern crate threadpool;
use threadpool::ThreadPool;

use crate::file_handler;
use file_handler::hash_file;


pub fn mock_process_base_dir(target_path: String) {
    let mut memory = HashMap::<PathBuf, bool>::new();
    let path = PathBuf::from(&target_path);
    memory.insert(path, false);

    let n_cpus = num_cpus::get();
    let pool = ThreadPool::new(n_cpus);
    println!("Running with {} threads ...", n_cpus);

    let (tx, rx) = channel();

    loop {
	let tmp_memory = memory.clone();
	let tasks: Vec<(&PathBuf, &bool)> = tmp_memory.iter().filter(|(_, v)| !*v).collect();
	for (p, v) in tasks {
	    if !v {
		let pp = PathBuf::from(p.clone());
		let tx = tx.clone();
		match memory.get_mut(&pp) {
		    Some(x) => { *x = true },
		    None => panic!("The key must exist, this should be impossible"),
		}

		pool.execute(move || {
		    if pp.is_dir() {
			let local_p = pp.clone();
			let str_path = local_p.to_str().expect("Could not covert path to str");
			println!("Directory path : {}", str_path);

			let entries = match read_dir(&local_p) {
			    Ok(x) => x,
			    Err(e) => panic!("Could not read directory {}: {}", str_path, e),
			};

			let mut count = 0;
			for entry in entries {
			    count = count + 1;
			    let sub_path = match entry {
				Ok(dir_entry) => PathBuf::from(dir_entry.path()),
				Err(e) => panic!("Cannot get path for entries under {}: {}", str_path, e),
			    };
			    // println!("sending {:#?}", sub_path);
			    let new_pp = pp.clone();
			    tx.send((new_pp.clone(), Some(sub_path)));
			}

			if count == 0 {
			    println!("COUNT IS ZERO");
			    let new_pp = pp.clone();
			    let hash = 
			    tx.send((new_pp.clone(), None));
			}

		    } else {
			let str_path = pp.to_str().expect("Could not covert path to str");
			let hash = hash_file(&pp).expect("error calculating hash");
			println!("File path : {} - {}", hash, str_path);
			tx.send((pp, None));
		    }
		});

	    }
	}

	for _ in tmp_memory.iter().filter(|(__, v)| **v) {
	    // println!("memory state: {:#?}", memory);	    
	    match rx.recv().expect("didn't get a value from the threads") {
		(p, Some(val)) => {
		    // println!("Rceived : {:#?}", val);
		    memory.insert(val, false);
		    memory.remove(&p);
		},
		(p, None) => {
		    println!("processed file {:#?}", p);
		    memory.remove(&p);
		}
	    };
	}

	if memory.is_empty(){
	    break;
	}
	
    }
    
}

