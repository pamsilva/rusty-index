use std::fmt;
use std::collections::VecDeque;
use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, Mutex};

extern crate petgraph;
use petgraph::graph::{Graph, NodeIndex};

extern crate crypto;
use crypto::md5::Md5;
use crypto::digest::Digest;

use crate::index_db;
use index_db::IndexRecord;


#[derive(Debug)]
struct EdgeInfo {
    node: NodeIndex,
    tag: String,
}


#[derive(Debug)]
pub struct FileRecord {
    pub checksum: String,
    pub name: String,
    pub path: Vec<String>,
}


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
pub struct GraphStorage {
    pub graph: Graph::<GNode, ()>,
    pub root: NodeIndex,
}


pub fn initialise_graph() -> GraphStorage {
    let mut new_graph = Graph::<GNode, ()>::new();
    let root_index = new_graph.add_node(GNode::DirNode{
        name: String::from("root"),
        checksum: String::from(""),
    });

    GraphStorage {
        graph: new_graph,
        root: root_index,
    }
}


pub trait GraphStorageInterface {
    fn bulk_insert(&mut self, node: &mut NodeIndex, sorted_entries: Vec<FileRecord>);
    fn insert(& mut self, sorted_entries: Vec<IndexRecord>);
    fn find_duplicates(&self) -> HashMap<String, Vec<String>>;
}


impl GraphStorageInterface for GraphStorage {
    
    fn bulk_insert(&mut self, node: &mut NodeIndex, sorted_entries: Vec<FileRecord>) {
        let mut local_contents = HashMap::<String, Vec<FileRecord>>::new();

        for record in sorted_entries {
            if record.path.len() == 0 {
                let leaf = self.graph.add_node(GNode::FileLeaf {
                    name: String::from(record.name),
                    checksum: String::from(record.checksum),
                    id: 0,
                });
                self.graph.add_edge(*node, leaf, ());

                continue;
            }
            
            let s = &record.path[0];
            match local_contents.get_mut(s) {
                Some(vec) => {
                    let mut new_path = record.path.clone();
                    let t = new_path.remove(0);
                    if t.as_str() == "" && new_path.len() >= 1{
                        new_path.remove(0);
                    }
                    
                    vec.push(FileRecord {
                        checksum: record.checksum,
                        name: record.name,
                        path: new_path
                    });
                }
                None => {
                    let mut new_vec = Vec::<FileRecord>::new();
                    let mut new_path = record.path.clone();
                    new_path.remove(0);
                    
                    new_vec.push(FileRecord {
                        checksum: record.checksum,
                        name: record.name,
                        path: new_path
                    });
                    local_contents.insert(String::from(s), new_vec);
                }
            }            
        }

        // for each of the keys, check if the node already exists on the graph
        // - if it does, get the node index and recursivelly call parallel_execution
        // - if it doesn't, create the node and the edge between input node and new one
        //   and then recusively call parallel_execution with new node and corresponding
        //   file records
        for (key, value) in local_contents {
            let mut cursor = match is_linked(&self.graph, node, &key) {
                Some(res) => res,
                None => {
                    let new_node = self.graph.add_node(GNode::DirNode {
                        name: String::from(key),
                        checksum: String::from("NA"),
                    });
                    self.graph.add_edge(*node, new_node, ());
                    
                    new_node
                },
            };

            self.bulk_insert(&mut cursor, value);
        }

        // update current node's hash for all of its contents.
        let checksum = calculate_hash(&self.graph, node); 
        let node_data = self.graph.node_weight_mut(*node).unwrap();
        let node_name = match node_data {
            GNode::DirNode {name, checksum: _2} => name,
            GNode::FileLeaf {name: _1, checksum: _2, id: _3} => panic!(
                "LeafNode cannot be part of the trace. It should be impossible"
            ),
        };

        *node_data = GNode::DirNode {
            name: node_name.to_string(),
            checksum: checksum,
        }

    }
    
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
                    None => {
                        let new_node = self.graph.add_node(GNode::DirNode {
                            name: String::from(*elem),
                            checksum: String::from("potato"),
                        });
                        self.graph.add_edge(cursor, new_node, ());
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
            self.graph.add_edge(cursor, leaf, ());
            
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


pub fn parallel_bulk_insert(shared_graph: Arc<Mutex<GraphStorage>>, node: &mut NodeIndex, sorted_entries: Vec<FileRecord>){
    let mut local_contents = HashMap::<String, Vec<FileRecord>>::new();
    
    for record in sorted_entries {
        if record.path.len() == 0 {
            let mut tmp_graph = shared_graph.lock().unwrap();

            let leaf = tmp_graph.graph.add_node(GNode::FileLeaf {
                name: String::from(record.name),
                checksum: String::from(record.checksum),
                id: 0,
            });
            tmp_graph.graph.add_edge(*node, leaf, ());

            continue;
        }
    
        let s = &record.path[0];
        match local_contents.get_mut(s) {
            Some(vec) => {
                let mut new_path = record.path.clone();
                let t = new_path.remove(0);
                if t.as_str() == "" && new_path.len() >= 1{
                    new_path.remove(0);
                }
    
                vec.push(FileRecord {
                    checksum: record.checksum,
                    name: record.name,
                    path: new_path
                });
            }
            None => {
                let mut new_vec = Vec::<FileRecord>::new();
                let mut new_path = record.path.clone();
                new_path.remove(0);
    
                new_vec.push(FileRecord {
                    checksum: record.checksum,
                    name: record.name,
                    path: new_path
                });
                local_contents.insert(String::from(s), new_vec);
            }
        }            
    }

    // for each of the keys, check if the node already exists on the graph
    // - if it does, get the node index and recursivelly call parallel_execution
    // - if it doesn't, create the node and the edge between input node and new one
    //   and then recusively call parallel_execution with new node and corresponding
    //   file records
    let mut handles = Vec::new();
    for (key, value) in local_contents {
        let mut tmp_graph = shared_graph.lock().unwrap();

        let mut cursor = match is_linked(&tmp_graph.graph, node, &key) {
            Some(res) => res,
            None => {
                let new_node = tmp_graph.graph.add_node(GNode::DirNode {
                    name: String::from(key),
                    checksum: String::from("NA"),
                });
                tmp_graph.graph.add_edge(*node, new_node, ());
    
                new_node
            },
        };

        let new_ref = shared_graph.clone();
        handles.push(thread::spawn(move || {
            parallel_bulk_insert(new_ref, &mut cursor, value);
        }));
    }

    for item in handles {
        item.join().unwrap();
    }

    let mut tmp_graph = shared_graph.lock().unwrap();

    // update current node's hash for all of its contents.
    let checksum = calculate_hash(&tmp_graph.graph, node); 
    let node_data = tmp_graph.graph.node_weight_mut(*node).unwrap();
    let node_name = match node_data {
        GNode::DirNode {name, checksum: _2} => name,
        GNode::FileLeaf {name: _1, checksum: _2, id: _3} => panic!(
            "LeafNode cannot be part of the trace. It should be impossible"
        ),
    };

    *node_data = GNode::DirNode {
        name: node_name.to_string(),
        checksum: checksum,
    }
    
}


pub fn create_shared_graph() -> Arc<Mutex<GraphStorage>> {
    let mut new_graph = Graph::<GNode, ()>::new();
    let root_index = new_graph.add_node(GNode::DirNode{
        name: String::from("root"),
        checksum: String::from(""),
    });

    Arc::new(Mutex::new(
        GraphStorage {
            graph: new_graph,
            root: root_index,
        }
    ))
}


fn is_linked(graph: &Graph::<GNode, ()>, cursor: &NodeIndex, key: &str) -> Option<NodeIndex> {
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

    return None;
}


fn calculate_hash(graph: &Graph::<GNode, ()>, cursor: &NodeIndex) -> String {
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

    fn elem_from_path(path: String) -> Vec<String> {
        path.split('/')
            .filter(|x| *x != "")
            .map(|x| String::from(x))
            .collect()
    }

    #[test]
    fn test_bulk_insert() {
        let mut records = Vec::<FileRecord>::new();
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/")),
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/location/")),
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/other/")),
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/yet-another/")),
        });
        records.push(FileRecord {
            checksum: String::from("aabbb"),
            name: String::from("aabbb.txt"),
            path: elem_from_path(String::from("/some/location/")),
        });
        
        let mut graph = initialise_graph();
        let mut root = graph.root;
        graph.bulk_insert(&mut root, records);
        
        let res = graph.find_duplicates();
        
        println!("dupes : {:#?}", res);

        assert_eq!(res.len(), 2);
        assert_eq!(res.get("aaaaa").unwrap().len(), 3);
    }

    #[test]
    fn test_bulk_parallel_insert() {
        let mut records = Vec::<FileRecord>::new();
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/")),
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/location/")),
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/other/")),
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/yet-another/")),
        });
        records.push(FileRecord {
            checksum: String::from("aabbb"),
            name: String::from("aabbb.txt"),
            path: elem_from_path(String::from("/some/location/")),
        });
        
        let graph_ref = create_shared_graph();
        let local_ref = graph_ref.clone();
        let mut root = local_ref.lock().unwrap().root;

        let first_ref = graph_ref.clone();
        parallel_bulk_insert(first_ref, &mut root, records);
        
        let res = local_ref.lock().unwrap().find_duplicates();
        
        println!("dupes : {:#?}", res);

        assert_eq!(res.len(), 2);
        assert_eq!(res.get("aaaaa").unwrap().len(), 3);
    }
}

