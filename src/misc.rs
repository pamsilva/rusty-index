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


pub fn get_name_and_split_path(pwd: &String, file_name: &String) -> (Vec<String>, String) {
    let mut relevant_file_name: String = file_name.clone();

    if file_name.starts_with("./") {
        let (_, new_file_name) = file_name.split_at(2);
        relevant_file_name = String::from(new_file_name);
    }

    let mut real_path = format!("{}/{}", pwd, relevant_file_name);
    if file_name.starts_with("/") {
        real_path = relevant_file_name;
    }

    let parts: Vec<String> = real_path.rsplitn(2, '/').map(|x| String::from(x)).collect();
    let path_components = path_to_components(&parts[1]);
    return (path_components, parts[0].clone());
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
