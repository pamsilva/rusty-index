extern crate petgraph;
use petgraph::graph::{Graph, NodeIndex};
use std::fmt;

use crate::index_db;
use index_db::IndexRecord;


#[derive(Debug)]
pub enum GNode{
    DirNode {
        name: String,
        checksum: String,
    },
    FileLeaf {
        name: String,
        checksum: String,
        id: u32,
    },
}


impl fmt::Display for GNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &*self {
            GNode::FileLeaf {name, checksum: _, id: __} => write!(f, "File-{}", String::from(name)),
            GNode::DirNode {name, checksum: _}  => write!(f, "Directory-{}", String::from(name)),
        }
    }
}


fn is_linked(graph: &Graph::<GNode, String>, cursor: &NodeIndex, key: &str) -> Option<NodeIndex> {
    for thing in graph.neighbors(*cursor) {
        let i = match graph.node_weight(thing).unwrap() {
            GNode::FileLeaf {name: _1, checksum: _2, id: _3} => None,
            GNode::DirNode {name: dir_name, checksum: _2} => Some(dir_name),
        };

        match i {
            Some(dir_name) => {
                if dir_name == key {
                    return Some(thing)
                } else {
                    continue
                }
            },
            None => continue
        };
    }

    println!();
    return None;
}


pub fn process_entries(sorted_entries: Vec<IndexRecord>) -> Graph::<GNode, String>{
    let mut graph = Graph::new();

    let root_index = graph.add_node(GNode::DirNode{
        name: String::from("root"),
        checksum: String::from("potato"),
    });

    let mut cursor = root_index;
    for record in sorted_entries {
        let path_elems: Vec<&str> = record.path.split("/").collect();
        let relevant_elements = &path_elems[1..]; 

        println!("Here {:#?}", relevant_elements);

        for elem in relevant_elements.iter().as_slice() {
            cursor = match is_linked(&graph, &cursor, *elem) {
                Some(res) => res,
                None      => {
                    let new_node = graph.add_node(GNode::DirNode{
                        name: String::from(*elem),
                        checksum: String::from("potato"),
                    });
                    graph.add_edge(cursor, new_node, String::from("dir"));
                    new_node
                },
            }
        }

        let leaf = graph.add_node(GNode::FileLeaf {
            name: String::from(record.name),
            checksum: String::from(record.checksum),
            id: record.id,
        });
            
        graph.add_edge(cursor, leaf, String::from("file"));
        cursor = root_index;
    }

    return graph;
}



// #cfg[test()]
// mod test {
//     use supper::*;

//     #[test]
//     fn test_graph_generation() {
        
//     }
// }

