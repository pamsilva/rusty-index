use std::fs::File;
use std::io::prelude::*;
use std::collections::HashMap;

extern crate petgraph;
use petgraph::dot::Dot;

extern crate clap;
use clap::{App, Arg, SubCommand};

extern crate colored;
use colored::*;


mod index_db;
use index_db::{IndexRecord, IndexStorage};

mod analyser;
use analyser::{FileRecord, GraphStorageInterface};

mod file_handler;
use file_handler::load_and_process_files;

mod misc;
use misc::to_file_record;
use misc::to_index_record;

fn export_graph(graph: &analyser::GraphStorage) {
    let mut f = File::create("example1.dot").unwrap();
    let output = format!("{:?}", Dot::new(&graph.graph));

    println!("Writing dot file with final results.");
    match f.write_all(&output.as_bytes()) {
        Ok(_) => println!("All done. Have a nice day in the world."),
        Err(e) => println!("Error writing to file {:?}", e),
    }
}


fn display_result(duplicates: &HashMap<String, Vec<String>>) {
    println!("{}", "Here are he results :".green().bold());
    println!("");
    
    for (hash, paths) in duplicates {
	println!("{} {}", "Hash :".red(), hash.magenta());

	for p in paths {
	    if p.ends_with("/") {
		println!("\t {} \t \u{1F4C2}", p.blue().bold());
	    } else {
		println!("\t{}", p.blue().bold());
	    }
	}

	println!("");
    }
    
}


fn main() {
    let config = App::new("rusty-index")
        .subcommand(SubCommand::with_name("parse"))
        .subcommand(SubCommand::with_name("generate"))
        .subcommand(SubCommand::with_name("virtual"))
        .subcommand(SubCommand::with_name("baby-steps").arg(Arg::with_name("path").takes_value(true).index(1)))
        .subcommand(SubCommand::with_name("baby-steps-mem").arg(Arg::with_name("path").takes_value(true).index(1)))
        .get_matches();

    let file_name = String::from("index.db");
    let data_source = index_db::initalise_db(&file_name).unwrap();

    match data_source.create() {
        Ok(_) => println!("Database initialised or verified"),
        Err(e) => println!("Error initialising database: {:?}", e),
    };

    if let Some(_matches) = config.subcommand_matches("parse") {
        let records = load_and_process_files();
        let storage_records: Vec<IndexRecord> =
            records.into_iter().map(|x| to_index_record(&x)).collect();

        println!("Saving {} into the database.", storage_records.len());
        match data_source.insert(&storage_records) {
            Ok(_) => println!("Records successfully inserted"),
            Err(e) => println!("Error inserting records: {:?}", e),
        };

    } else if let Some(_matches) = config.subcommand_matches("generate") {
        let res = data_source.fetch_sorted().unwrap();
        let mut graph = analyser::initialise_graph();

        let file_records_res: Vec<FileRecord> =
            res.into_iter().map(|x| to_file_record(&x)).collect();
        println!("Processing {} from the database.", file_records_res.len());

        graph.bulk_insert(file_records_res);
        export_graph(&graph);

        let final_res = graph.find_duplicates();
	display_result(&final_res);

    } else if let Some(_matches) = config.subcommand_matches("virtual") {
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

    } else if let Some(_matches) = config.subcommand_matches("baby-steps") {
	let path = String::from(
	    _matches.value_of("path").unwrap_or(file_handler::get_current_dir().as_str()));
	let records = file_handler::scan_directory(path);

	let mut graph = analyser::initialise_graph();
        graph.bulk_insert(records);
        let final_res = graph.find_duplicates();
	
	display_result(&final_res);

    } else if let Some(_matches) = config.subcommand_matches("baby-steps-mem") {
    	let path = String::from(
    	    _matches.value_of("path").unwrap_or(file_handler::get_current_dir().as_str()));
    	let records = file_handler::simple_scan_directory(path);
	let strage_records = file_handler::path_to_file_record(records);
        println!("Saving {} into the database.", strage_records.len());
        match data_source.insert(&strage_records) {
            Ok(_) => println!("Records successfully inserted"),
            Err(e) => println!("Error inserting records: {:?}", e),
        };
	
    } else {
        println!("You need to either parse or generate, otherwise there is nothing to do.");
    }
}
