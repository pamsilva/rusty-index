extern crate petgraph;
use petgraph::graph::{Graph, NodeIndex};
use std::fmt;
use std::collections::VecDeque;

extern crate crypto;
use crypto::md5::Md5;
use crypto::digest::Digest;

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
        match self {
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


fn calculate_hash(graph: &Graph::<GNode, String>, cursor: &NodeIndex) -> String {
    let mut buff = Vec::<String>::new();
    for thing in graph.neighbors(*cursor) {
        let elem_checksum = match graph.node_weight(thing).unwrap() {
            GNode::FileLeaf {name: _1, checksum, id: _3} => checksum,
            GNode::DirNode {name: _2, checksum} => checksum,
        };

        buff.push(elem_checksum.clone());
    }

    let mut hasher = Md5::new();
    buff.sort();
    for checksum in buff {
        hasher.input_str(checksum.as_str());
    }
    
    String::from(hasher.result_str())
}


pub fn process_entries(sorted_entries: Vec<IndexRecord>) -> Graph::<GNode, String>{
    let mut graph = Graph::new();

    let root_index = graph.add_node(GNode::DirNode{
        name: String::from("root"),
        checksum: String::from("potato"),
    });

    let mut cursor = root_index;
    let mut trace = VecDeque::<NodeIndex>::new();
    trace.push_front(cursor);
    for record in sorted_entries {
        let path_elems: Vec<&str> = record.path.split("/").collect();
        let relevant_elements = &path_elems[1..]; 

        for elem in relevant_elements.iter().as_slice() {
            cursor = match is_linked(&graph, &cursor, *elem) {
                Some(res) => res,
                None      => {
                    let new_node = graph.add_node(GNode::DirNode {
                        name: String::from(*elem),
                        checksum: String::from("potato"),
                    });
                    graph.add_edge(cursor, new_node, String::from("dir"));
                    new_node
                },
            };

            trace.push_front(cursor);
        }

        let leaf = graph.add_node(GNode::FileLeaf {
            name: String::from(record.name),
            checksum: String::from(record.checksum),
            id: record.id,
        });
        graph.add_edge(cursor, leaf, String::from("file"));
        
        for elem in trace.iter() {
            let checksum = calculate_hash(&graph, &elem); 

            let node = graph.node_weight_mut(*elem).unwrap();
            let node_name = match node {
                GNode::DirNode {name, checksum: _2} => name,
                GNode::FileLeaf {name: _1, checksum: _2, id: _3} => panic!(
                    "LeafNode cannot be part of the trace. It should be impossible"
                ),
            };

            *node = GNode::DirNode {
                name: node_name.to_string(),
                checksum: checksum,
            }
        }
        
        cursor = root_index;
    }

    return graph;
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_process_entries() {
        let mut records = Vec::<IndexRecord>::new();
        records.push(IndexRecord {
            id: 1,
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: String::from("/some/location/"),
        });

        let graph = process_entries(records);
        
        assert_eq!(true, true)
    }
}

