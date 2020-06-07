extern crate petgraph;
use petgraph::graph::{Graph, NodeIndex};
use std::fmt;
use std::collections::VecDeque;
use std::collections::HashMap;

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


#[derive(Debug)]
struct EdgeInfo {
    node: NodeIndex,
    tag: String,
}


#[derive(Debug)]
pub struct GraphStorage {
    pub graph: Graph::<GNode, String>,
    pub root: NodeIndex,
}


pub fn initialise_graph() -> GraphStorage {
    let mut new_graph = Graph::<GNode, String>::new();
    let root_index = new_graph.add_node(GNode::DirNode{
        name: String::from("root"),
        checksum: String::from(""),
    });

    GraphStorage {
        graph: new_graph,
        root: root_index,
    }
}


pub trait GraphIndexStorage {
    fn insert(& mut self, sorted_entries: Vec<IndexRecord>);
    fn find_duplicates(&self) -> HashMap<String, Vec<String>>;
}


impl GraphIndexStorage for GraphStorage {

    fn insert(& mut self, sorted_entries: Vec<IndexRecord>) {
        let mut cursor = self.root;
        let mut trace = VecDeque::<NodeIndex>::new();
        trace.push_front(cursor);
        for record in sorted_entries {
            let path_elems: Vec<&str> = record.path.split("/").collect();
            let relevant_elements = &path_elems[1..]; 

            for elem in relevant_elements.iter().as_slice() {
                if *elem == "" {
                    continue;
                }
                
                cursor = match is_linked(&self.graph, &cursor, *elem) {
                    Some(res) => res,
                    None      => {
                        let new_node = self.graph.add_node(GNode::DirNode {
                            name: String::from(*elem),
                            checksum: String::from("potato"),
                        });
                        self.graph.add_edge(cursor, new_node, String::from("dir"));
                        new_node
                    },
                };

                trace.push_front(cursor);
            }

            
            let leaf = self.graph.add_node(GNode::FileLeaf {
                name: String::from(record.name),
                checksum: String::from(record.checksum),
                id: record.id,
            });
            self.graph.add_edge(cursor, leaf, String::from("file"));
            
            for elem in trace.iter() {
                let checksum = calculate_hash(&self.graph, &elem); 

                let node = self.graph.node_weight_mut(*elem).unwrap();
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
            
            cursor = self.root;
        }

    }

    fn find_duplicates(&self) -> HashMap<String, Vec<String>> {
        let mut duplicates = HashMap::<String, Vec<String>>::new();
        let mut edges = VecDeque::<EdgeInfo>::new();
        edges.push_back(EdgeInfo {
            node: self.root,
            tag: String::from(""),
        });

        while !edges.is_empty() {
            let pivot = edges.pop_front().unwrap();
            
            for elem in self.graph.neighbors(pivot.node) {
                match self.graph.node_weight(elem).unwrap() {
                    
                    GNode::FileLeaf {name, checksum, id: _} => {
                        let path = String::from(format!("{}/{}", pivot.tag.as_str(), name.as_str()));

                        match duplicates.get_mut(checksum) {
                            Some(vec) => {vec.push(path)},
                            None => {
                                let mut new_vec = Vec::<String>::new();
                                new_vec.push(path);
                                duplicates.insert(String::from(checksum.as_str()), new_vec);
                            },
                        };
                    },
                    
                    GNode::DirNode {name, checksum} => {
                        let path = String::from(format!("{}/{}", pivot.tag.as_str(), name.as_str()));

                        match duplicates.get_mut(checksum) {
                            Some(vec) => {
                                vec.push(path);
                            },
                            None => {
                                let mut new_vec = Vec::<String>::new();
                                new_vec.push(path.clone());
                                duplicates.insert(String::from(checksum.as_str()), new_vec);

                                edges.push_back(EdgeInfo {
                                    node: elem,
                                    tag: path,
                                });
                            },
                        };
                    },
                };
            }
        }

        return duplicates.into_iter().filter(|(_, v)| v.len() > 1).collect();
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
            path: String::from("/some/"),
        });
        records.push(IndexRecord {
            id: 1,
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: String::from("/some/location/"),
        });
        records.push(IndexRecord {
            id: 1,
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: String::from("/some/other/"),
        });
        records.push(IndexRecord {
            id: 1,
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: String::from("/some/yet-another/"),
        });
        records.push(IndexRecord {
            id: 1,
            checksum: String::from("aabbb"),
            name: String::from("aabbb.txt"),
            path: String::from("/some/location/"),
        });
        
        let mut graph = initialise_graph();
        graph.insert(records);
        let res = graph.find_duplicates();

        println!("{:#?}", res);

        assert_eq!(res.len(), 2);
        assert_eq!(res.get("aaaaa").unwrap().len(), 3);
    }
}

