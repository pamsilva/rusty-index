use crate::index_db;
use index_db::IndexRecord;

use crate::analyser;
use analyser::FileRecord;


pub fn path_to_components(path: &String) -> Vec<String> {
    path.split('/')
        .filter(|x| *x != "")
        .map(|x| String::from(x))
        .collect()
}


pub fn components_to_path(components: &Vec<String>) -> String {
    format!("/{}/", components.join("/"))
}

pub fn to_index_record(file_record: &FileRecord) -> IndexRecord {
    IndexRecord {
        id: 0,
        checksum: file_record.checksum.clone(),
        name: file_record.name.clone(),
        path: components_to_path(&file_record.path),
    }
}


pub fn to_file_record(index_record: &IndexRecord) -> FileRecord {
    FileRecord {
        checksum: index_record.checksum.clone(),
        name: index_record.name.clone(),
        path: path_to_components(&index_record.path),
    }
}


#[cfg(test)]
mod test {
    use super::*;
    
    #[test]
    fn test_to_file_record() {
        let example = IndexRecord {
            id: 1,
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: String::from("/some/"),
        };

        let res = to_file_record(&example);
        println!("{:#?}", res);

        assert_eq!(res.checksum, String::from("aaaaa"));
        assert_eq!(res.name, String::from("aaaaa.txt"));
        assert_eq!(res.path, vec!["some"]);
    }

    #[test]
    fn test_to_index_record() {
        let example = FileRecord {
            checksum: String::from("aaaaa"),
            name: String::from("aaaaa.txt"),
            path: vec![String::from("some")],
        };

        let res = to_index_record(&example);
        println!("{:#?}", res);

        assert_eq!(res.checksum, String::from("aaaaa"));
        assert_eq!(res.name, String::from("aaaaa.txt"));
        assert_eq!(res.path, String::from("/some/"));
        assert_eq!(res.id, 0);
    }
}
