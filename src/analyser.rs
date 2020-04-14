extern crate petgraph;
use petgraph::graph::{Graph, NodeIndex};


use crate::index_db;
use index_db::IndexRecord;


fn is_linked(graph: &Graph::<String, usize>, cursor: &NodeIndex, key: &str) -> Option<NodeIndex> {
    println!("Neighbors for {:#?}", graph.node_weight(*cursor).unwrap());

    for thing in graph.neighbors(*cursor) {
        print!("{:#?}", graph.node_weight(thing).unwrap());
        if graph.node_weight(thing).unwrap().as_str() == key {
            println!();
            return Some(thing);
        }
    }

    println!();
    return None;
}


pub fn process_entries(sorted_entries: Vec<IndexRecord>) -> Graph::<String, usize>{
    let mut graph = Graph::new();

    let root_index = graph.add_node(String::from("root"));
    let mut cursor = root_index;
    for record in sorted_entries {
        let path_elems: Vec<&str> = record.path.split("/").collect();
        let relevant_elements = &path_elems[1..]; 
        println!("Here {:#?}", relevant_elements);

        for elem in relevant_elements.iter().as_slice() {
            cursor = match is_linked(&graph, &cursor, *elem) {
                Some(res) => res,
                None      => {
                    let new_node = graph.add_node(String::from(*elem));
                    graph.add_edge(cursor, new_node, 1);

                    new_node
                },
            }
            
        }

        cursor = root_index;
    }

    return graph;
}
