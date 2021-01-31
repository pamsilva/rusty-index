use std::fmt;
use std::collections::VecDeque;
use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, Mutex};

extern crate chrono;
use chrono::{DateTime, Utc};

extern crate petgraph;
use petgraph::graph::{Graph, NodeIndex};

extern crate crypto;
use crypto::md5::Md5;
use crypto::digest::Digest;


#[derive(Debug)]
pub struct FileRecord {
    pub checksum: String,
    pub name: String,
    pub path: Vec<String>,
    pub modified: DateTime<Utc>,
}


#[derive(Debug)]
struct EdgeInfo {
    node: NodeIndex,
    tag: String,
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
    fn _bulk_insert(&mut self, node: &mut NodeIndex, sorted_entries: Vec<FileRecord>);
    fn bulk_insert(&mut self, sorted_entries: Vec<FileRecord>);
    fn find_duplicates(&self) -> HashMap<String, Vec<String>>;
}


impl GraphStorageInterface for GraphStorage {
    fn _bulk_insert(&mut self, node: &mut NodeIndex, sorted_entries: Vec<FileRecord>) {
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
                    new_path.remove(0);
                    
                    vec.push(FileRecord {
                        checksum: record.checksum,
                        name: record.name,
                        path: new_path,
			modified: record.modified,
                    });
                }
                None => {
                    let mut new_vec = Vec::<FileRecord>::new();
                    let mut new_path = record.path.clone();
                    new_path.remove(0);
                    
                    new_vec.push(FileRecord {
                        checksum: record.checksum,
                        name: record.name,
                        path: new_path,
			modified: record.modified,
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

            self._bulk_insert(&mut cursor, value);
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
            checksum
        }

    }

    fn bulk_insert(&mut self, sorted_entries: Vec<FileRecord>) {
	self._bulk_insert(&mut self.root.clone(), sorted_entries)
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
                new_path.remove(0);
                
                vec.push(FileRecord {
                    checksum: record.checksum,
                    name: record.name,
                    path: new_path,
		    modified: record.modified,
                });
            }
            None => {
                let mut new_vec = Vec::<FileRecord>::new();
                let mut new_path = record.path.clone();
                new_path.remove(0);
    
                new_vec.push(FileRecord {
                    checksum: record.checksum,
                    name: record.name,
                    path: new_path,
		    modified: record.modified,
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
        checksum,
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

    use chrono::{DateTime, NaiveDate, NaiveTime, NaiveDateTime, Utc};

    use crate::misc;
    use misc::path_to_components;

    
    fn elem_from_path(path: String) -> Vec<String> {
        path_to_components(&path)
    }

    fn mock_date_time() -> DateTime<Utc> {
	let d = NaiveDate::from_ymd(2015, 6, 3);
	let t = NaiveTime::from_hms_milli(12, 34, 56, 789);

	return DateTime::<Utc>::from_utc(NaiveDateTime::new(d, t), Utc);
    }
    
    #[test]
    fn test_bulk_insert() {
        let mut records = Vec::<FileRecord>::new();
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/location/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/other/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/yet-another/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aabbb"),
            name: String::from("aabbb.txt"),
            path: elem_from_path(String::from("/some/location/")),
	    modified: mock_date_time()
        });
        
        let mut graph = initialise_graph();
        let mut root = graph.root;
        graph._bulk_insert(&mut root, records);
        
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
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/location/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/other/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: elem_from_path(String::from("/some/yet-another/")),
	    modified: mock_date_time()
        });
        records.push(FileRecord {
            checksum: String::from("aabbb"),
            name: String::from("aabbb.txt"),
            path: elem_from_path(String::from("/some/location/")),
	    modified: mock_date_time()
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
